from sqlalchemy.orm import Session
from database import SessionLocal
from models import User
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def create_initial_user():
    db = SessionLocal()
    
    # Check if user already exists
    existing_user = db.query(User).filter(User.email == "admin@example.com").first()
    if existing_user:
        print("Initial user already exists!")
        return
    
    # Create new user
    hashed_password = pwd_context.hash("admin123")
    new_user = User(
        username="admin",
        email="admin@example.com",
        hashed_password=hashed_password,
        is_active=True
    )
    
    db.add(new_user)
    db.commit()
    print("Initial user created successfully!")
    db.close()

if __name__ == "__main__":
    create_initial_user()
