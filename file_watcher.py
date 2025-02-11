import time
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import pandas as pd
import json
import shutil
from datetime import datetime
import logging
import traceback
from tokenization_rules import TokenizationRules
from quantum_tokenizer import QuantumTokenizer
from threading import Thread

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class CSVHandler(FileSystemEventHandler):
    def __init__(self, input_dir, processed_dir, tokenizer, rules):
        self.input_dir = input_dir
        self.processed_dir = processed_dir
        self.tokenizer = tokenizer
        self.rules = rules
        self.archive_dir = os.path.join(os.path.dirname(input_dir), "archive")
        self.stats_file = os.path.join(os.path.dirname(input_dir), "stats.json")
        self.mappings_file = os.path.join(os.path.dirname(input_dir), "token_mappings.json")
        
        # Create directories if they don't exist
        for dir_path in [self.input_dir, self.processed_dir, self.archive_dir]:
            os.makedirs(dir_path, exist_ok=True)
        
        # Initialize or load stats and mappings
        self.load_or_create_files()
    
    def generate_quantum_token(self, field, value):
        """Generate a quantum token for a value"""
        token_a, _ = self.tokenizer.generate_token_pair()
        # Use first 12 chars of the quantum token
        return f'QT_{token_a[:12]}'

    def load_or_create_files(self):
        """Initialize or load stats and mappings files"""
        # Initialize stats
        if not os.path.exists(self.stats_file) or os.path.getsize(self.stats_file) == 0:
            initial_stats = {
                'total_files': 0,
                'total_records': 0,
                'sensitive_fields_found': {}
            }
            self.save_stats(initial_stats)
            logging.info("Created new stats file")
        
        # Initialize mappings
        if not os.path.exists(self.mappings_file) or os.path.getsize(self.mappings_file) == 0:
            self.save_mappings({})
            logging.info("Created new mappings file")

    def load_stats(self):
        """Load statistics from file"""
        try:
            with open(self.stats_file, 'r') as f:
                stats = json.load(f)
                logging.info(f"Loaded stats: {stats}")
                return stats
        except Exception as e:
            logging.error(f"Error loading stats: {str(e)}")
            return {'total_files': 0, 'total_records': 0, 'sensitive_fields_found': {}}
    
    def save_stats(self, stats):
        """Save statistics to file"""
        try:
            # Convert numpy values to Python native types
            stats = {
                'total_files': int(stats['total_files']),
                'total_records': int(stats['total_records']),
                'sensitive_fields_found': {k: int(v) for k, v in stats['sensitive_fields_found'].items()}
            }
            with open(self.stats_file, 'w') as f:
                json.dump(stats, f, indent=4)
            logging.info("Saved stats successfully")
        except Exception as e:
            logging.error(f"Error saving stats: {str(e)}")
    
    def load_mappings(self):
        """Load token mappings from file"""
        try:
            with open(self.mappings_file, 'r') as f:
                mappings = json.load(f)
                logging.info(f"Loaded mappings for fields: {list(mappings.keys())}")
                return mappings
        except Exception as e:
            logging.error(f"Error loading mappings: {str(e)}")
            return {}
    
    def save_mappings(self, mappings):
        """Save token mappings to file"""
        try:
            # Ensure the mappings is a dictionary
            if not isinstance(mappings, dict):
                mappings = {}
            
            # Load existing mappings
            existing_mappings = self.load_mappings()
            
            # Update with new mappings
            for field, values in mappings.items():
                if field not in existing_mappings:
                    existing_mappings[field] = {}
                existing_mappings[field].update(values)
            
            # Save updated mappings
            with open(self.mappings_file, 'w') as f:
                json.dump(existing_mappings, f, indent=4)
            logging.info("Saved mappings successfully")
        except Exception as e:
            logging.error(f"Error saving mappings: {str(e)}")

    def on_created(self, event):
        logging.info(f"File system event detected: {event.event_type} - {event.src_path}")
        if event.is_directory:
            logging.info("Skipping directory event")
            return
        if not event.src_path.endswith('.csv'):
            logging.info("Skipping non-CSV file")
            return
        
        # Wait a bit to ensure file is completely written
        time.sleep(1)
        
        try:
            logging.info(f"Starting to process file: {event.src_path}")
            self.process_file(event.src_path)
        except Exception as e:
            logging.error(f"Error processing {event.src_path}: {str(e)}")
            import traceback
            logging.error(traceback.format_exc())

    def process_file(self, file_path):
        try:
            logging.info(f"Processing file: {file_path}")
            
            # Extract departments from filename
            filename = os.path.basename(file_path)
            source_dept, dest_dept = TokenizationRules.parse_filename(filename)
            if not source_dept or not dest_dept:
                logging.error(f"Invalid filename format: {filename}")
                return
            
            # Get tokenization rules
            rules = TokenizationRules.get_rules(source_dept, dest_dept)
            logging.info(f"Using rules: tokenize={rules['tokenize']}, pass_through={rules['pass_through']}")
            
            # Read CSV
            df = pd.read_csv(file_path)
            logging.info(f"Read {len(df)} rows from {filename}")
            
            # Load mappings and stats
            mappings = self.load_mappings()
            stats = self.load_stats()
            
            # Update stats
            stats['total_files'] += 1
            stats['total_records'] += len(df)
            
            # Process each field
            processed_df = df.copy()
            for field in df.columns:
                # Update stats
                if field not in stats['sensitive_fields_found']:
                    stats['sensitive_fields_found'][field] = 0
                stats['sensitive_fields_found'][field] += len(df[df[field].notna()])
                
                # Process field based on rules
                if field in rules['tokenize']:
                    logging.info(f"Tokenizing field: {field}")
                    if field not in mappings:
                        mappings[field] = {}
                    
                    # Tokenize each unique value
                    for value in df[field].unique():
                        if pd.notna(value):
                            value_str = str(value)
                            if value_str not in mappings[field]:
                                token = self.generate_quantum_token(field, value_str)
                                mappings[field][value_str] = token
                                logging.info(f"New token for {field}: {value_str} -> {token}")
                            
                            # Replace value with token
                            processed_df.loc[df[field] == value, field] = mappings[field][value_str]
                else:
                    logging.info(f"Passing through field: {field}")
            
            # Save processed file
            output_path = os.path.join(self.processed_dir, f"processed_{filename}")
            processed_df.to_csv(output_path, index=False)
            logging.info(f"Saved processed file to: {output_path}")
            
            # Archive original file
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            archive_path = os.path.join(self.archive_dir, f"{timestamp}_{filename}")
            shutil.move(file_path, archive_path)
            logging.info(f"Archived original file to: {archive_path}")
            
            # Save mappings and stats
            self.save_mappings(mappings)
            self.save_stats(stats)
            
            # Log sample of processed data
            logging.info("Sample of processed data:")
            for field in processed_df.columns:
                sample = processed_df[field].head(2).tolist()
                logging.info(f"{field}: {sample}")
            
            logging.info(f"Successfully processed {filename}")
        except Exception as e:
            logging.error(f"Error in process_file: {str(e)}")
            logging.error(traceback.format_exc())

class FileWatcher:
    def __init__(self, input_dir, processed_dir, tokenizer, rules):
        self.input_dir = input_dir
        self.processed_dir = processed_dir
        self.tokenizer = tokenizer
        self.rules = rules
        self.observer = None
        self.handler = None
        self.watcher_thread = None

    def start(self):
        try:
            # Create handler and observer
            self.handler = CSVHandler(self.input_dir, self.processed_dir, self.tokenizer, self.rules)
            self.observer = Observer()
            
            # Schedule monitoring of input directory
            input_path = os.path.abspath(self.input_dir)
            os.makedirs(input_path, exist_ok=True)
            self.observer.schedule(self.handler, path=input_path, recursive=False)
            
            # Start observer in a separate thread
            def run_observer():
                logging.info(f"Starting file watcher on directory: {input_path}")
                self.observer.start()
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    self.stop()
            
            self.watcher_thread = Thread(target=run_observer, daemon=True)
            self.watcher_thread.start()
            
        except Exception as e:
            logging.error(f"Error starting file watcher: {str(e)}")
            logging.error(traceback.format_exc())
    
    def stop(self):
        if self.observer:
            self.observer.stop()
            logging.info("Stopping file watcher...")
            self.observer.join()

if __name__ == "__main__":
    # Test the file watcher
    input_dir = "data/input"
    processed_dir = "data/processed"
    tokenizer = QuantumTokenizer()
    rules = TokenizationRules()
    
    watcher = FileWatcher(input_dir, processed_dir, tokenizer, rules)
    watcher.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        watcher.stop()
