from fastapi import FastAPI, HTTPException, Depends, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from typing import List, Optional
import jwt
from models import *
from database import SessionLocal, engine
from quantum_tokenizer import QuantumTokenizer
from ai_detector import SensitivityDetector
from blockchain_manager import BlockchainManager

app = FastAPI(title="Quantum-AI Hybrid Tokenization System")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize components
quantum_tokenizer = QuantumTokenizer()
sensitivity_detector = SensitivityDetector()
blockchain_manager = BlockchainManager()

# JWT settings
SECRET_KEY = "your-secret-key"  # In production, use environment variable
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(db, form_data.username, form_data.password)
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

@app.post("/tokenize")
async def tokenize_data(data: dict, current_user: User = Depends(get_current_user)):
    # Detect sensitive fields using AI
    sensitive_fields = sensitivity_detector.detect(data)
    
    # Generate quantum tokens for sensitive fields
    tokenized_data = {}
    for field in sensitive_fields:
        token_a, token_b = quantum_tokenizer.generate_token_pair()
        
        # Store token B on blockchain
        blockchain_manager.store_token(token_b)
        
        # Replace sensitive data with token A
        tokenized_data[field] = token_a
    
    return tokenized_data

@app.get("/data")
async def get_tokenized_data(current_user: User = Depends(get_current_user)):
    # Retrieve tokenized data for the current user
    return {"data": "Tokenized data here"}

@app.get("/stats")
async def get_stats(current_user: User = Depends(get_current_user)):
    # Get volume statistics of tokenized data
    return {
        "total_tokens": blockchain_manager.get_total_tokens(),
        "active_tokens": blockchain_manager.get_active_tokens(),
        "token_operations_24h": blockchain_manager.get_recent_operations()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
