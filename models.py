from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, UniqueConstraint
from datetime import datetime
from backend.database import Base

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    username = Column(String(100), unique=True, nullable=False)
    email = Column(String(255), unique=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f"<User(username='{self.username}', email='{self.email}')>"

class TokenMapping(Base):
    __tablename__ = 'token_mappings'
    
    id = Column(Integer, primary_key=True)
    field_name = Column(String(100), nullable=False)
    original_value = Column(Text, nullable=False)
    token_value = Column(String(255), nullable=False, unique=True)
    source_dept = Column(String(100), nullable=False)
    dest_dept = Column(String(100), nullable=False)
    usage_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_used_at = Column(DateTime, nullable=True)

    # Ensure unique combination of field_name and original_value per department pair
    __table_args__ = (
        UniqueConstraint('field_name', 'original_value', 'source_dept', 'dest_dept',
                        name='uix_token_mapping'),
    )

    def __repr__(self):
        return f"<TokenMapping(field='{self.field_name}', original='{self.original_value}', token='{self.token_value}')>"

class ProcessedFile(Base):
    __tablename__ = 'processed_files'
    
    id = Column(Integer, primary_key=True)
    filename = Column(String(255), nullable=False)
    records_processed = Column(Integer, default=0)
    status = Column(String(50), nullable=False)  # Pending, Processing, Completed, Failed
    error_message = Column(Text, nullable=True)
    processed_at = Column(DateTime, default=datetime.utcnow)
    source_dept = Column(String(100), nullable=True)
    dest_dept = Column(String(100), nullable=True)

    def __repr__(self):
        return f"<ProcessedFile(filename='{self.filename}', status='{self.status}')>"


