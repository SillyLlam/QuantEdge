import os
import pandas as pd
import json
import shutil
from datetime import datetime
import logging
from tokenization_rules import TokenizationRules
from quantum_tokenizer import QuantumTokenizer

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
        self.stats_file = os.path.join(data_dir, 'stats.json')
        self.mappings_file = os.path.join(data_dir, 'token_mappings.json')
        
        self.tokenizer = QuantumTokenizer()
        
        # Create directories
        for dir_path in [self.input_dir, self.processed_dir, self.archive_dir]:
            os.makedirs(dir_path, exist_ok=True)
            logging.info(f"Created/verified directory: {dir_path}")
        
        # Initialize files
        self.load_or_create_files()
    
    def load_or_create_files(self):
        if not os.path.exists(self.stats_file):
            self.save_stats({'total_files': 0, 'total_records': 0, 'sensitive_fields_found': {}})
        if not os.path.exists(self.mappings_file):
            self.save_mappings({})
    
    def load_stats(self):
        try:
            with open(self.stats_file, 'r') as f:
                return json.load(f)
        except:
            return {'total_files': 0, 'total_records': 0, 'sensitive_fields_found': {}}
    
    def save_stats(self, stats):
        with open(self.stats_file, 'w') as f:
            json.dump(stats, f, indent=4)
    
    def load_mappings(self):
        try:
            with open(self.mappings_file, 'r') as f:
                return json.load(f)
        except:
            return {}
    
    def save_mappings(self, mappings):
        with open(self.mappings_file, 'w') as f:
            json.dump(mappings, f, indent=4)
    
    def generate_quantum_token(self, field, value):
        token_a, _ = self.tokenizer.generate_token_pair()
        return f'QT_{token_a[:12]}'
    
    def process_file(self, filename):
        try:
            file_path = os.path.join(self.input_dir, filename)
            logging.info(f"Processing file: {file_path}")
            
            # Get departments from filename
            source_dept, dest_dept = TokenizationRules.parse_filename(filename)
            if not source_dept or not dest_dept:
                raise ValueError(f"Invalid filename format: {filename}")
            
            # Get rules
            rules = TokenizationRules.get_rules(source_dept, dest_dept)
            logging.info(f"Rules: tokenize={rules['tokenize']}, pass_through={rules['pass_through']}")
            
            # Read CSV
            df = pd.read_csv(file_path)
            logging.info(f"Read {len(df)} rows")
            
            # Load mappings and stats
            mappings = self.load_mappings()
            stats = self.load_stats()
            
            # Update stats
            stats['total_files'] += 1
            stats['total_records'] += len(df)
            
            # Process fields
            processed_df = df.copy()
            for field in df.columns:
                # Update field stats
                if field not in stats['sensitive_fields_found']:
                    stats['sensitive_fields_found'][field] = 0
                stats['sensitive_fields_found'][field] += len(df[df[field].notna()])
                
                # Process based on rules
                if field in rules['tokenize']:
                    logging.info(f"Tokenizing field: {field}")
                    if field not in mappings:
                        mappings[field] = {}
                    
                    # Process each unique value
                    for value in df[field].unique():
                        if pd.notna(value):
                            value_str = str(value)
                            if value_str not in mappings[field]:
                                token = self.generate_quantum_token(field, value_str)
                                mappings[field][value_str] = token
                                logging.info(f"New token: {value_str} -> {token}")
                            
                            # Replace value with token
                            processed_df.loc[df[field] == value, field] = mappings[field][value_str]
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
            
            # Save mappings and stats
            self.save_mappings(mappings)
            self.save_stats(stats)
            
            # Log sample
            logging.info("Sample of processed data:")
            for field in processed_df.columns:
                sample = processed_df[field].head(2).tolist()
                logging.info(f"{field}: {sample}")
            
            return True
            
        except Exception as e:
            logging.error(f"Error processing file: {str(e)}")
            return False

if __name__ == "__main__":
    try:
        processor = FileProcessor()
        logging.info(f"Looking for CSV files in {processor.input_dir}")
        
        # Process any CSV files in input directory
        files = os.listdir(processor.input_dir)
        logging.info(f"Found files: {files}")
        
        for filename in files:
            if filename.endswith('.csv'):
                logging.info(f"Processing CSV file: {filename}")
                processor.process_file(filename)
            else:
                logging.info(f"Skipping non-CSV file: {filename}")
    except Exception as e:
        logging.error(f"Error in main: {str(e)}")
        logging.error(traceback.format_exc())
