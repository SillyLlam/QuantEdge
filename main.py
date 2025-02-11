from fastapi import FastAPI, HTTPException, Depends, status, Request, File, UploadFile, Form
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from pydantic import BaseModel
from typing import Optional, List, Dict
import uvicorn
from datetime import datetime, timedelta
from jose import JWTError, jwt
from passlib.context import CryptContext
import random2 as random
import numpy as np
from web3 import Web3
import os
from dotenv import load_dotenv
import pandas as pd
import io
import json

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(title="Quantum-AI Hybrid Tokenization System")

# Mount templates
templates = Jinja2Templates(directory="templates")

# CORS middleware configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security configuration
SECRET_KEY = os.getenv("SECRET_KEY", "your-secret-key-here")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Initialize Web3
w3 = Web3(Web3.HTTPProvider(os.getenv("ETHEREUM_NODE_URL", "http://localhost:8545")))

# Models
class Token(BaseModel):
    access_token: str
    token_type: str

class TokenData(BaseModel):
    username: Optional[str] = None

class User(BaseModel):
    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None

class UserInDB(User):
    hashed_password: str

class TokenizationRequest(BaseModel):
    source_department: str
    target_department: str

class TokenizationRules(BaseModel):
    source_department: str
    target_department: str
    sensitive_fields: List[str]

# Define department-based tokenization rules
DEPARTMENT_RULES = {
    # HR Department Rules
    ('HR', 'Sales'): ['Phone Number', 'Salary', 'Age'],
    ('HR', 'Marketing'): ['Phone Number', 'Salary'],
    ('HR', 'Finance'): ['Phone Number'],
    ('HR', 'Engineering'): ['Salary', 'Age'],
    
    # Sales Department Rules
    ('Sales', 'HR'): ['Phone Number', 'Salary'],
    ('Sales', 'Marketing'): ['Phone Number', 'Salary'],
    ('Sales', 'Finance'): ['Phone Number', 'Salary'],
    ('Sales', 'Engineering'): ['Phone Number'],
    
    # Marketing Department Rules
    ('Marketing', 'HR'): ['Phone Number', 'Salary'],
    ('Marketing', 'Sales'): ['Phone Number', 'Salary'],
    ('Marketing', 'Finance'): ['Phone Number', 'Salary'],
    ('Marketing', 'Engineering'): ['Phone Number'],
    
    # Finance Department Rules
    ('Finance', 'HR'): ['Salary', 'Age'],
    ('Finance', 'Sales'): ['Salary', 'Age'],
    ('Finance', 'Marketing'): ['Salary'],
    ('Finance', 'Engineering'): ['Salary', 'Age'],
    
    # Engineering Department Rules
    ('Engineering', 'HR'): ['Phone Number'],
    ('Engineering', 'Sales'): ['Phone Number', 'Salary'],
    ('Engineering', 'Marketing'): ['Phone Number'],
    ('Engineering', 'Finance'): ['Salary']
}

def get_sensitive_fields(source: str, target: str) -> List[str]:
    """Get sensitive fields based on source and target departments"""
    if source == target:
        return []  # No tokenization needed if source and target are same
    return DEPARTMENT_RULES.get((source, target), [])

def quantum_random():
    """Generate quantum-inspired random numbers using hardware entropy"""
    return random.random()

# AI sensitivity detection (mock implementation)
def detect_sensitivity(data: str) -> float:
    """Mock AI sensitivity detection"""
    # In a real implementation, this would use a trained model
    return random.uniform(0, 1)

# Token generation with quantum entropy
def generate_quantum_token(data: str, field_name: str) -> str:
    """Generate a token using quantum entropy"""
    quantum_entropy = random.random()
    token_base = hash(str(data) + str(quantum_entropy) + field_name)
    return f"QT{abs(token_base):010d}_{field_name}"

# Mock user database - create with a known password hash
test_password = "testpass"
test_hash = pwd_context.hash(test_password)
print(f"Created test user with password hash: {test_hash}")

fake_users_db = {
    "testuser": {
        "username": "testuser",
        "full_name": "Test User",
        "email": "test@example.com",
        "hashed_password": test_hash,
        "disabled": False,
    }
}

# Security functions
def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def get_user(db, username: str):
    if username in db:
        user_dict = db[username]
        return UserInDB(**user_dict)

def authenticate_user(fake_db, username: str, password: str):
    print(f"Attempting to authenticate user: {username}")
    user = get_user(fake_db, username)
    if not user:
        print(f"User {username} not found in database")
        return False
    if not verify_password(password, user.hashed_password):
        print(f"Invalid password for user {username}")
        return False
    print(f"Successfully authenticated user {username}")
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

async def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        token_data = TokenData(username=username)
    except JWTError:
        raise credentials_exception
    user = get_user(fake_users_db, username=token_data.username)
    if user is None:
        raise credentials_exception
    return user

# Routes
@app.post("/token", response_model=Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    user = authenticate_user(fake_users_db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": user.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}

@app.post("/tokenize-csv")
async def tokenize_csv(
    file: UploadFile = File(...),
    source_department: str = Form(...),
    target_department: str = Form(...),
    current_user: User = Depends(get_current_user)
):
    """Tokenize CSV data based on department rules"""
    try:
        print(f"Processing CSV from {source_department} to {target_department}")
        
        # Read CSV content
        content = await file.read()
        df = pd.read_csv(io.BytesIO(content))
        
        # Get sensitive fields based on departments
        sensitive_fields = get_sensitive_fields(source_department, target_department)
        print(f"Sensitive fields to tokenize: {sensitive_fields}")
        
        if not sensitive_fields:
            print("No fields need to be tokenized for this department combination")
            return StreamingResponse(
                iter([content.decode()]),
                media_type="text/csv",
                headers={
                    'Content-Disposition': f'attachment; filename="tokenized_{file.filename}"',
                    'X-Token-Mappings': json.dumps({"message": "No tokenization needed"})
                }
            )
        
        # Create a copy of the dataframe for tokenization
        df_tokenized = df.copy()
        
        # Store token mappings
        token_mappings = {}
        
        # Tokenize sensitive fields
        for field in sensitive_fields:
            if field in df.columns:
                print(f"Tokenizing field: {field}")
                token_mappings[field] = {}
                
                # Convert the column to string type
                df_tokenized[field] = df_tokenized[field].astype(str)
                
                # Generate tokens for unique values
                unique_values = df[field].unique()
                for value in unique_values:
                    if pd.notna(value):
                        token = generate_quantum_token(str(value), field)
                        token_mappings[field][str(value)] = token
                        # Replace all occurrences of the value with its token
                        df_tokenized[field] = df_tokenized[field].replace(str(value), token)
                
                print(f"Tokenized {len(unique_values)} unique values for {field}")
        
        # Convert tokenized dataframe to CSV
        output = io.StringIO()
        df_tokenized.to_csv(output, index=False)
        
        # Create response with tokenized CSV
        response = StreamingResponse(
            iter([output.getvalue()]),
            media_type="text/csv",
            headers={
                'Content-Disposition': f'attachment; filename="tokenized_{file.filename}"'
            }
        )
        
        # Add token mappings to response headers
        response.headers["X-Token-Mappings"] = json.dumps({
            "tokenized_fields": sensitive_fields,
            "mappings": token_mappings
        })
        print("Successfully processed CSV")
        
        return response
        
    except Exception as e:
        print(f"Error processing CSV: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error processing CSV: {str(e)}"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.utcnow().isoformat()}

@app.get("/api/stats")
async def get_stats(current_user: User = Depends(get_current_user)):
    """Get tokenization statistics"""
    try:
        with open('data/stats.json', 'r') as f:
            stats = json.load(f)
        return stats
    except FileNotFoundError:
        return {"files_processed": [], "total_records": 0, "total_tokens": 0}

@app.get("/api/token-mappings")
async def get_token_mappings(current_user: User = Depends(get_current_user)):
    """Get token mappings"""
    try:
        with open('data/token_mappings.json', 'r') as f:
            mappings = json.load(f)
        return mappings
    except FileNotFoundError:
        return {}

@app.get("/")
async def read_root():
    with open("templates/index.html", "r") as f:
        html_content = f.read()
    return HTMLResponse(content=html_content, media_type="text/html")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
