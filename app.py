from fastapi import FastAPI, Request, HTTPException, BackgroundTasks, Form, Depends, status, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, desc
import os
from datetime import datetime, timedelta
from models import TokenMapping, ProcessedFile, User
from backend.database import db_session, init_db, Base, engine
from file_watcher import FileWatcher
from quantum_tokenizer import QuantumTokenizer
from tokenization_rules import TokenizationRules
import pandas as pd
import logging
import json
import bcrypt
from jose import JWTError, jwt
from typing import Optional

# JWT settings
SECRET_KEY = "your-secret-key-here"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

app = FastAPI(title="Quantum Tokenization")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure templates and static files
templates = Jinja2Templates(directory="templates")

# Custom StaticFiles class to prevent caching
class NoCache(StaticFiles):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        if response.status_code == 200:
            response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
            response.headers['Pragma'] = 'no-cache'
            response.headers['Expires'] = '0'
        return response

# Mount static files with no-cache configuration
app.mount("/static", NoCache(directory="static"), name="static")

# Initialize database
init_db()

# Ensure directories exist
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
UPLOAD_FOLDER = os.path.join(DATA_DIR, 'input')
PROCESSED_FOLDER = os.path.join(DATA_DIR, 'processed')
ARCHIVE_FOLDER = os.path.join(DATA_DIR, 'archive')

for directory in [DATA_DIR, UPLOAD_FOLDER, PROCESSED_FOLDER, ARCHIVE_FOLDER]:
    os.makedirs(directory, exist_ok=True)

# Initialize file watcher
tokenizer = QuantumTokenizer()
file_watcher = FileWatcher(UPLOAD_FOLDER, PROCESSED_FOLDER, tokenizer, TokenizationRules)

@app.on_event("startup")
async def startup_event():
    # Start file watcher
    file_watcher.start()
    logger.info("File watcher started")
    await init_db()

@app.on_event("shutdown")
async def shutdown_event():
    # Stop file watcher
    file_watcher.stop()
    logger.info("File watcher stopped")
    # Close database session
    db_session.remove()

def get_current_user(request: Request):
    token = request.cookies.get("access_token")
    if not token or not token.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        token = token.split(" ")[1]
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    except (JWTError, IndexError):
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    
    user = db_session.query(User).filter(User.username == username).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return user

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(request: Request, username: str = Form(...), password: str = Form(...)):
    user = db_session.query(User).filter(User.username == username).first()
    if not user or not bcrypt.checkpw(password.encode(), user.hashed_password.encode()):
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Invalid username or password"
        })
    
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    
    response = RedirectResponse(url="/dashboard", status_code=status.HTTP_302_FOUND)
    response.set_cookie(key="access_token", value=f"Bearer {access_token}", httponly=True)
    return response

@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})

@app.post("/register")
async def register(request: Request, username: str = Form(...), email: str = Form(...), password: str = Form(...)):
    # Check if username or email already exists
    if db_session.query(User).filter(User.username == username).first():
        return templates.TemplateResponse("register.html", {
            "request": request,
            "error": "Username already exists"
        })
    
    if db_session.query(User).filter(User.email == email).first():
        return templates.TemplateResponse("register.html", {
            "request": request,
            "error": "Email already exists"
        })
    
    # Hash password and create user
    hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    user = User(username=username, email=email, hashed_password=hashed_password)
    db_session.add(user)
    db_session.commit()
    
    return RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)

@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    current_user = get_current_user(request)
    return templates.TemplateResponse("index.html", {"request": request, "user": current_user})

@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=status.HTTP_302_FOUND)
    response.delete_cookie("access_token")
    return response

@app.get("/stats")
async def get_stats(request: Request):
    current_user = get_current_user(request)
    try:
        # Get list of files in input and processed directories
        input_files = [f for f in os.listdir(UPLOAD_FOLDER) if f.endswith('.csv')]
        processed_files = [f for f in os.listdir(PROCESSED_FOLDER) if f.endswith('.csv')]
        
        total_records = 0
        field_stats = {}
        
        # Process statistics from processed files
        for filename in processed_files:
            file_path = os.path.join(PROCESSED_FOLDER, filename)
            try:
                df = pd.read_csv(file_path)
                total_records += len(df)
                
                # Update field statistics
                for field in df.columns:
                    if field not in field_stats:
                        field_stats[field] = {'count': 0, 'unique_values': set()}
                    field_stats[field]['count'] += len(df[df[field].notna()])
                    field_stats[field]['unique_values'].update(df[field].unique())
            except Exception as e:
                logging.error(f"Error processing file {filename}: {str(e)}")
        
        # Convert sets to counts for JSON serialization
        for field in field_stats:
            field_stats[field]['unique_values'] = len(field_stats[field]['unique_values'])
        
        # Get recent files (last 10)
        recent_files = []
        all_files = []
        
        # Add processed files
        for filename in processed_files:
            file_path = os.path.join(PROCESSED_FOLDER, filename)
            stat = os.stat(file_path)
            file_info = {
                'filename': filename,
                'status': 'Completed',
                'processed_at': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'records': len(pd.read_csv(file_path)) if os.path.exists(file_path) else 0
            }
            all_files.append(file_info)
        
        # Add files currently in input folder
        for filename in input_files:
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            stat = os.stat(file_path)
            file_info = {
                'filename': filename,
                'status': 'Pending',
                'processed_at': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                'records': len(pd.read_csv(file_path)) if os.path.exists(file_path) else 0
            }
            all_files.append(file_info)
        
        # Sort by processed_at timestamp and get last 10
        recent_files = sorted(all_files, key=lambda x: x['processed_at'], reverse=True)[:10]
        
        # Get token mappings from processed and archive files
        token_mappings = {}
        all_fields = ['Name', 'Phone', 'Email', 'Salary', 'Department', 'Position']
        
        # Initialize mappings for all fields
        for field in all_fields:
            token_mappings[field] = {}
        
        # Process both archive and processed files to build complete mappings
        for directory in [ARCHIVE_FOLDER, PROCESSED_FOLDER]:
            for filename in os.listdir(directory):
                if not filename.endswith('.csv'):
                    continue
                    
                file_path = os.path.join(directory, filename)
                try:
                    df = pd.read_csv(file_path)
                    
                    # Extract source and destination departments
                    source_dept, dest_dept = TokenizationRules.parse_filename(filename)
                    if not source_dept or not dest_dept:
                        continue
                        
                    # Get tokenization rules for this file
                    rules = TokenizationRules.get_rules(source_dept, dest_dept)
                    if not rules:
                        continue
                    
                    # For each field in the file
                    for field in df.columns:
                        if field not in all_fields:
                            continue
                            
                        # For tokenized fields, map values to tokens
                        if field in rules['tokenize']:
                            # If processing archive file, map original values
                            if directory == ARCHIVE_FOLDER:
                                archive_df = df
                                processed_file = os.path.join(PROCESSED_FOLDER, f'processed_{filename}')
                                if os.path.exists(processed_file):
                                    processed_df = pd.read_csv(processed_file)
                                    # Map original values to their tokens
                                    for orig_val, token_val in zip(archive_df[field].dropna(), processed_df[field].dropna()):
                                        if str(orig_val) not in token_mappings[field]:
                                            token_mappings[field][str(orig_val)] = str(token_val)
                        
                        # For pass-through fields, map values to themselves
                        elif field in rules['pass_through']:
                            for value in df[field].dropna().unique():
                                if str(value) not in token_mappings[field]:
                                    token_mappings[field][str(value)] = str(value)
                                    
                except Exception as e:
                    logging.error(f"Error processing file {filename}: {str(e)}")
        
        # Calculate active tokens
        active_tokens = sum(len(mappings) for mappings in token_mappings.values())
        
        return {
            'total_records': total_records,
            'active_tokens': active_tokens,
            'field_stats': field_stats,
            'recent_files': recent_files,
            'token_mappings': token_mappings
        }
    except Exception as e:
        logging.error(f"Error fetching stats: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch stats: {str(e)}")

@app.get("/token-mappings")
async def get_all_mappings(request: Request):
    current_user = get_current_user(request)
    try:
        # Get all mappings ordered by field and creation time
        mappings = db_session.query(TokenMapping).order_by(
            TokenMapping.field_name,
            desc(TokenMapping.created_at)
        ).all()
        
        # Convert to list of dictionaries
        mappings_data = [{
            'id': mapping.id,
            'field': mapping.field_name,
            'original': mapping.original_value,
            'token': mapping.token_value,
            'source_dept': mapping.source_dept,
            'dest_dept': mapping.dest_dept,
            'usage_count': mapping.usage_count,
            'created_at': mapping.created_at.isoformat()
        } for mapping in mappings]
        
        return mappings_data
    except Exception as e:
        logger.error(f"Error fetching token mappings: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch token mappings: {str(e)}")

@app.get("/api/mappings/{field}")
async def get_mappings(field: str, request: Request):
    current_user = get_current_user(request)
    try:
        mappings = db_session.query(TokenMapping).filter(
            TokenMapping.field_name == field
        ).order_by(desc(TokenMapping.created_at)).all()
        
        return [{
            "original": m.original_value,
            "token": m.token_value,
            "created_at": m.created_at.isoformat() if m.created_at else None,
            "last_used": m.last_used_at.isoformat() if m.last_used_at else None,
            "usage_count": m.usage_count or 0
        } for m in mappings]
    except Exception as e:
        logger.error(f"Error getting mappings for field {field}: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching mappings")

@app.get("/api/fields")
async def get_fields(request: Request):
    current_user = get_current_user(request)
    try:
        fields = db_session.query(TokenMapping.field_name).distinct().all()
        return [field[0] for field in fields]
    except Exception as e:
        logger.error(f"Error getting fields: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching fields")

@app.get("/processed-files")
async def get_processed_files():
    try:
        files = db_session.query(ProcessedFile).order_by(
            desc(ProcessedFile.processed_at)
        ).all()
        
        return [{
            "filename": f.filename,
            "status": f.status,
            "records_processed": f.records_processed,
            "processed_at": f.processed_at.isoformat() if f.processed_at else None,
            "error_message": f.error_message,
            "source_dept": f.source_dept,
            "dest_dept": f.dest_dept
        } for f in files]
    except Exception as e:
        logger.error(f"Error getting processed files: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching processed files")

@app.get("/api/files")
async def get_files(request: Request):
    current_user = get_current_user(request)
    try:
        # Get files from input folder
        input_files = [f for f in os.listdir(UPLOAD_FOLDER) if f.endswith('.csv')]
        
        # Get files from processed folder
        processed_files = [f for f in os.listdir(PROCESSED_FOLDER) if f.endswith('.csv')]
        
        # Get files from archive folder
        archive_files = [f for f in os.listdir(ARCHIVE_FOLDER) if f.endswith('.csv')]
        
        # Get processing status from database
        processing_files = db_session.query(ProcessedFile.filename, ProcessedFile.status).all()
        file_status = {f.filename: f.status for f in processing_files}
        
        return {
            "input": [{
                "filename": f,
                "status": file_status.get(f, "pending")
            } for f in input_files],
            "processed": [{
                "filename": f,
                "status": file_status.get(f, "success")
            } for f in processed_files],
            "archive": archive_files
        }
    except Exception as e:
        logger.error(f"Error getting files: {str(e)}")
        raise HTTPException(status_code=500, detail="Error fetching files")

async def init_db():
    try:
        # Create tables
        Base.metadata.create_all(bind=engine)
        
        # Check if we have any data
        if db_session.query(TokenMapping).count() == 0:
            # Add sample token mappings
            sample_mappings = [
                TokenMapping(
                    field_name="SSN",
                    original_value="123-45-6789",
                    token_value="TOK_SSN_001",
                    source_dept="HR",
                    dest_dept="Finance",
                    usage_count=5
                ),
                TokenMapping(
                    field_name="Email",
                    original_value="john.doe@example.com",
                    token_value="TOK_EMAIL_001",
                    source_dept="Sales",
                    dest_dept="Marketing",
                    usage_count=3
                ),
                TokenMapping(
                    field_name="Credit Card",
                    original_value="4111-1111-1111-1111",
                    token_value="TOK_CC_001",
                    source_dept="Sales",
                    dest_dept="Finance",
                    usage_count=2
                )
            ]
            db_session.add_all(sample_mappings)
            
            # Add sample processed files
            sample_files = [
                ProcessedFile(
                    filename="employees.csv",
                    records_processed=100,
                    status="Completed",
                    processed_at=datetime.now() - timedelta(hours=1)
                ),
                ProcessedFile(
                    filename="customers.csv",
                    records_processed=250,
                    status="Completed",
                    processed_at=datetime.now() - timedelta(hours=2)
                ),
                ProcessedFile(
                    filename="transactions.csv",
                    records_processed=0,
                    status="Failed",
                    error_message="Invalid file format",
                    processed_at=datetime.now() - timedelta(hours=3)
                )
            ]
            db_session.add_all(sample_files)
            
            # Commit the changes
            db_session.commit()
            
            logger.info("Database initialized with sample data")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")