from sqlalchemy.orm import Session
from models import User
from database import SessionLocal
from passlib.context import CryptContext
import os

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def create_sample_user():
    db: Session = SessionLocal()
    try:
        username = os.getenv('SAMPLE_USERNAME', 'testuser')
        email = os.getenv('SAMPLE_EMAIL', 'testuser@example.com')
        password = os.getenv('SAMPLE_PASSWORD', 'password123')
        hashed_password = pwd_context.hash(password) 
        sample_user = User(username=username, email=email, hashed_password=hashed_password)
        db.add(sample_user)
        db.commit()
        db.refresh(sample_user)
        print("Sample user created:", sample_user.username)
    except Exception as e:
        print("An error occurred while creating the sample user:", e)
    finally:
        db.close()

if __name__ == "__main__":
    create_sample_user()