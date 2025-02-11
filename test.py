import os
import time
import pandas as pd
from quantum_tokenizer import QuantumTokenizer
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import shutil
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

class CSVHandler(FileSystemEventHandler):
    def __init__(self):
        self.tokenizer = QuantumTokenizer()
        self.input_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'input')
        self.processed_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'processed')
        self.archive_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'archive')
        
        # Create directories
        for dir_path in [self.input_dir, self.processed_dir, self.archive_dir]:
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f'Created/verified directory: {dir_path}')
    
    def process_file(self, file_path):
        try:
            logging.info(f'Processing file: {file_path}')
            
            # Read the CSV file
            df = pd.read_csv(file_path)
            logging.info(f'Read {len(df)} rows')
            
            # Parse departments from filename
            filename = os.path.basename(file_path)
            source_dept = None
            dest_dept = None
            
            if '_to_' in filename:
                try:
                    name_parts = filename.replace('.csv', '').split('_to_')
                    source_dept = name_parts[0].upper()
                    dest_dept = name_parts[1].upper()
                    logging.info(f'Detected departments - Source: {source_dept}, Destination: {dest_dept}')
                except Exception as e:
                    logging.error(f'Error parsing departments from filename: {str(e)}')
                    return
            else:
                logging.error('Filename must be in format SourceDept_to_DestDept.csv')
                return
                
            # Validate departments
            valid_depts = {'HR', 'SALES', 'FINANCE', 'IT'}
            if source_dept not in valid_depts or dest_dept not in valid_depts:
                logging.error(f'Invalid department(s). Must be one of: {valid_depts}')
                return
            
            # Get tokenization rules
            from tokenization_rules import TokenizationRules
            rules = TokenizationRules.get_rules(source_dept, dest_dept)
            
            if not rules:
                logging.error(f'No rules found for {source_dept} to {dest_dept}')
                return
                
            logging.info(f'Using rules - Tokenize: {rules["tokenize"]}, Pass through: {rules["pass_through"]}')
            
            # Validate that all required fields are present
            missing_fields = [field for field in rules['tokenize'] + rules['pass_through'] 
                            if field not in df.columns]
            if missing_fields:
                logging.error(f'Missing required fields in CSV: {missing_fields}')
                return
                
            fields_to_tokenize = rules['tokenize']
            
            # Process each field based on rules
            processed_df = df.copy()
            all_fields = set(df.columns)
            
            # Log which fields will be tokenized vs passed through
            logging.info('Field processing plan:')
            for field in all_fields:
                if field in fields_to_tokenize:
                    logging.info(f'  {field}: Will be tokenized')
                else:
                    logging.info(f'  {field}: Will be passed through')
            
            # First clear any existing tokens
            for field in df.columns:
                if field.startswith('QT_'):
                    del processed_df[field]
            
            # Process fields according to rules
            for field in all_fields:
                if field in fields_to_tokenize:
                    logging.info(f'\nTokenizing {field}:')
                    for value in df[field].unique():
                        if pd.notna(value):
                            # Generate quantum token
                            token_a, _ = self.tokenizer.generate_token_pair()
                            quantum_token = f'QT_{token_a[:12]}'
                            logging.info(f'{value} -> {quantum_token}')
                            # Replace value with token
                            processed_df.loc[df[field] == value, field] = quantum_token
                elif field in rules['pass_through']:
                    logging.info(f'Passing through field: {field}')
                    # Keep original values
                else:
                    logging.warning(f'Field {field} specified in rules but not found in CSV')
            
            # Save processed file
            filename = os.path.basename(file_path)
            output_file = os.path.join(self.processed_dir, f'processed_{filename}')
            processed_df.to_csv(output_file, index=False)
            logging.info(f'Saved processed data to: {output_file}')
            
            # Archive original file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_file = os.path.join(self.archive_dir, f'{timestamp}_{filename}')
            shutil.move(file_path, archive_file)
            logging.info(f'Archived original file to: {archive_file}')
            
            # Log sample of processed data
            logging.info('Sample of processed data:')
            print(processed_df.head().to_string())
            
        except Exception as e:
            logging.error(f'Error processing file: {str(e)}')
    
    def on_created(self, event):
        if event.is_directory:
            return
        if not event.src_path.endswith('.csv'):
            return
        
        # Wait a bit to ensure file is completely written
        time.sleep(1)
        self.process_file(event.src_path)

def start_watching():
    handler = CSVHandler()
    observer = Observer()
    observer.schedule(handler, path=handler.input_dir, recursive=False)
    observer.start()
    
    logging.info(f'Started watching directory: {handler.input_dir}')
    logging.info('Ready to process CSV files...')
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        logging.info('Stopping file watcher...')
    
    observer.join()

if __name__ == '__main__':
    start_watching()
