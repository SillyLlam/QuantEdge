from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Create SQLite database in the project directory
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'quantum.db')
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DB_PATH}"

# Create engine with SQLite connection args
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    echo=True  # Enable SQL logging
)

# Enable foreign key support for SQLite
@event.listens_for(engine, 'connect')
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute('PRAGMA foreign_keys=ON')
    cursor.close()

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Create thread-safe session
db_session = scoped_session(SessionLocal)

# Initialize base class for models
Base = declarative_base()
Base.query = db_session.query_property()

def init_db(reset=False):
    """Initialize the database, creating all tables."""
    try:
        # Import models here to avoid circular imports
        from models import TokenMapping, ProcessedFile, User
        
        if reset:
            # Drop all tables if reset is True
            Base.metadata.drop_all(bind=engine)
            logger.info("Dropped all existing tables")
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
        
        # Configure SQLite optimizations
        with engine.connect() as conn:
            conn.execute(text('PRAGMA journal_mode=WAL'))
            conn.execute(text('PRAGMA synchronous=NORMAL'))
            conn.execute(text('PRAGMA temp_store=MEMORY'))
            conn.execute(text('PRAGMA cache_size=-2000'))
            conn.commit()
            
            # Verify tables exist
            tables = conn.execute(text('SELECT name FROM sqlite_master WHERE type="table"')).fetchall()
            logger.info(f"Created tables: {[table[0] for table in tables]}")
        
        logger.info("Database initialized successfully")
        
    except ImportError as e:
        logger.error(f"Failed to import models: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

def get_db():
    """Get a database session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
