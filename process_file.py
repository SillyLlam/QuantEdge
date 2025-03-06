import os
import pandas as pd
import shutil
from datetime import datetime
import logging
import time
from tokenization_rules import TokenizationRules
from quantum_tokenizer import QuantumTokenizer
from models import TokenMapping, ProcessedFile
from backend.database import db_session
from sqlalchemy import or_

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class FileProcessor:
    def __init__(self):
        # Get absolute paths
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(base_dir, 'data')
        
        self.input_dir = os.path.join(data_dir, 'input')
        self.processed_dir = os.path.join(data_dir, 'processed')
        self.archive_dir = os.path.join(data_dir, 'archive')
        
        self.tokenizer = QuantumTokenizer()
        
        # Create directories
        for dir_path in [self.input_dir, self.processed_dir, self.archive_dir]:
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"Created/verified directory: {dir_path}")
    
    def get_or_create_token(self, field, value, source_dept, dest_dept):
        """Get existing token or create a new one"""
        try:
            # Try to find existing token
            token_mapping = db_session.query(TokenMapping).filter(
                TokenMapping.field_name == field,
                TokenMapping.original_value == str(value),
                or_(
                    TokenMapping.source_dept == source_dept,
                    TokenMapping.source_dept.is_(None)
                ),
                or_(
                    TokenMapping.dest_dept == dest_dept,
                    TokenMapping.dest_dept.is_(None)
                )
            ).first()
            
            if token_mapping:
                # Update usage statistics
                token_mapping.last_used_at = datetime.utcnow()
                token_mapping.usage_count += 1
                db_session.commit()
                return token_mapping.token_value
            
            # Create new token if not found
            token_a, _ = self.tokenizer.generate_token_pair()
            token_value = f'QT_{token_a[:12]}'
            
            new_mapping = TokenMapping(
                field_name=field,
                original_value=str(value),
                token_value=token_value,
                source_dept=source_dept,
                dest_dept=dest_dept,
                usage_count=1,
                last_used_at=datetime.utcnow()
            )
            
            db_session.add(new_mapping)
            db_session.commit()
            
            return token_value
            
        except Exception as e:
            db_session.rollback()
            logging.error(f"Error in get_or_create_token: {str(e)}")
            raise
    
    def process_file(self, filename):
        start_time = time.time()
        
        try:
            file_path = os.path.join(self.input_dir, filename)
            logging.info(f"Processing file: {file_path}")
            
            # Create ProcessedFile record
            processed_file = ProcessedFile(
                filename=filename,
                status='processing',
                created_at=datetime.utcnow()
            )
            db_session.add(processed_file)
            db_session.commit()
            
            # Get departments from filename
            source_dept, dest_dept = TokenizationRules.parse_filename(filename)
            if not source_dept or not dest_dept:
                raise ValueError(f"Invalid filename format: {filename}")
            
            processed_file.source_dept = source_dept
            processed_file.dest_dept = dest_dept
            
            # Get rules
            rules = TokenizationRules.get_rules(source_dept, dest_dept)
            logging.info(f"Rules: tokenize={rules['tokenize']}, pass_through={rules['pass_through']}")
            
            # Read CSV
            df = pd.read_csv(file_path)
            logging.info(f"Read {len(df)} rows")
            
            # Process fields
            processed_df = df.copy()
            fields_tokenized = {}
            
            for field in df.columns:
                if field in rules['tokenize']:
                    logging.info(f"Tokenizing field: {field}")
                    fields_tokenized[field] = 0
                    
                    # Process each unique value
                    for value in df[field].unique():
                        if pd.notna(value):
                            try:
                                token = self.get_or_create_token(field, value, source_dept, dest_dept)
                                mask = df[field] == value
                                processed_df.loc[mask, field] = token
                                fields_tokenized[field] += mask.sum()
                            except Exception as e:
                                logging.error(f"Error tokenizing value in {field}: {str(e)}")
                else:
                    logging.info(f"Passing through field: {field}")
            
            # Save processed file
            output_path = os.path.join(self.processed_dir, f"processed_{filename}")
            processed_df.to_csv(output_path, index=False)
            logging.info(f"Saved to: {output_path}")
            
            # Archive original
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_path = os.path.join(self.archive_dir, f"{timestamp}_{filename}")
            shutil.move(file_path, archive_path)
            logging.info(f"Archived to: {archive_path}")
            
            # Update ProcessedFile record
            processed_file.records_processed = len(df)
            processed_file.fields_tokenized = fields_tokenized
            processed_file.status = 'success'
            processed_file.processed_at = datetime.utcnow()
            processed_file.processing_time = int((time.time() - start_time) * 1000)
            db_session.commit()
            
            # Log sample
            logging.info("Sample of processed data:")
            for field in processed_df.columns:
                sample = processed_df[field].head(2).tolist()
                logging.info(f"{field}: {sample}")
            
            return True
            
        except Exception as e:
            logging.error(f"Error processing file: {str(e)}")
            if 'processed_file' in locals():
                processed_file.status = 'error'
                processed_file.error_message = str(e)
                processed_file.processed_at = datetime.utcnow()
                processed_file.processing_time = int((time.time() - start_time) * 1000)
                db_session.commit()
            return False
            
        except Exception as e:
            logging.error(f"Error processing file: {str(e)}")
            return False

if __name__ == "__main__":
    try:
        processor = FileProcessor()
        logging.info(f"Looking for CSV files in {processor.input_dir}")
        
        # Process any CSV files in input directory
        files = [f for f in os.listdir(processor.input_dir) if f.endswith('.csv')]
        if files:
            logging.info(f"Found CSV files: {files}")
            for filename in files:
                logging.info(f"Processing CSV file: {filename}")
                processor.process_file(filename)
        else:
            logging.info("No CSV files found in input directory")
            
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        import traceback
        logging.error(traceback.format_exc())
