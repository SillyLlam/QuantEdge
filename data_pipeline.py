import os
import time
import json
import shutil
import pandas as pd
from datetime import datetime
import random
import hashlib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Directory paths
INPUT_DIR = "data/input"
PROCESSED_DIR = "data/processed"
ARCHIVE_DIR = "data/archive"
STATS_FILE = "data/stats.json"
TOKEN_MAPPINGS_FILE = "data/token_mappings.json"

# Department rules
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

def generate_quantum_token(data: str, field_name: str) -> str:
    """Generate a token using quantum-inspired entropy"""
    entropy = random.random()
    token_base = hashlib.sha256(f"{data}{entropy}{field_name}".encode()).hexdigest()
    return f"QT{token_base[:10]}_{field_name}"

def get_sensitive_fields(source: str, target: str) -> list:
    """Get sensitive fields based on source and target departments"""
    if source == target:
        return []
    return DEPARTMENT_RULES.get((source, target), [])

def update_stats(file_name: str, source_dept: str, target_dept: str, fields_tokenized: list, records_processed: int):
    """Update processing statistics"""
    try:
        with open(STATS_FILE, 'r') as f:
            stats = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        stats = {"files_processed": [], "total_records": 0, "total_tokens": 0}
    
    file_stats = {
        "timestamp": datetime.now().isoformat(),
        "file_name": file_name,
        "source_department": source_dept,
        "target_department": target_dept,
        "fields_tokenized": fields_tokenized,
        "records_processed": records_processed,
        "tokens_generated": records_processed * len(fields_tokenized) if fields_tokenized else 0
    }
    
    stats["files_processed"].append(file_stats)
    stats["total_records"] += records_processed
    stats["total_tokens"] += file_stats["tokens_generated"]
    
    with open(STATS_FILE, 'w') as f:
        json.dump(stats, f, indent=2)

def update_token_mappings(new_mappings: dict):
    """Update token mappings with new entries"""
    try:
        with open(TOKEN_MAPPINGS_FILE, 'r') as f:
            mappings = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        mappings = {}
    
    # Update mappings for each field
    for field, values in new_mappings.items():
        if field not in mappings:
            mappings[field] = {}
        mappings[field].update(values)
    
    with open(TOKEN_MAPPINGS_FILE, 'w') as f:
        json.dump(mappings, f, indent=2)

def process_csv_file(file_path: str):
    """Process a CSV file and tokenize sensitive data"""
    try:
        # Extract source and target departments from filename
        # Expected format: source_to_target_*.csv (e.g., HR_to_Sales_data.csv)
        file_name = os.path.basename(file_path)
        name_parts = file_name.split('_')
        if len(name_parts) < 4 or name_parts[1] != 'to':
            raise ValueError(f"Invalid filename format: {file_name}")
        
        source_dept = name_parts[0]
        target_dept = name_parts[2]
        
        # Read CSV
        df = pd.read_csv(file_path)
        records_count = len(df)
        
        # Get fields to tokenize
        sensitive_fields = get_sensitive_fields(source_dept, target_dept)
        if not sensitive_fields:
            print(f"No tokenization needed for {source_dept} to {target_dept}")
            return
        
        # Create a copy for tokenization
        df_tokenized = df.copy()
        token_mappings = {}
        
        # Tokenize sensitive fields
        for field in sensitive_fields:
            if field in df.columns:
                token_mappings[field] = {}
                df_tokenized[field] = df_tokenized[field].astype(str)
                
                unique_values = df[field].unique()
                for value in unique_values:
                    if pd.notna(value):
                        token = generate_quantum_token(str(value), field)
                        token_mappings[field][str(value)] = token
                        df_tokenized[field] = df_tokenized[field].replace(str(value), token)
        
        # Save tokenized file
        output_path = os.path.join(PROCESSED_DIR, f"tokenized_{file_name}")
        df_tokenized.to_csv(output_path, index=False)
        
        # Archive original file
        archive_path = os.path.join(ARCHIVE_DIR, f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{file_name}")
        shutil.move(file_path, archive_path)
        
        # Update statistics and mappings
        update_stats(file_name, source_dept, target_dept, sensitive_fields, records_count)
        update_token_mappings(token_mappings)
        
        print(f"Successfully processed {file_name}")
        
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")
        # Move failed file to archive with error prefix
        archive_path = os.path.join(ARCHIVE_DIR, f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(file_path)}")
        shutil.move(file_path, archive_path)

class CSVHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.csv'):
            print(f"New CSV detected: {event.src_path}")
            process_csv_file(event.src_path)

def main():
    """Main function to start the file watcher"""
    # Create directories if they don't exist
    for directory in [INPUT_DIR, PROCESSED_DIR, ARCHIVE_DIR]:
        os.makedirs(directory, exist_ok=True)
    
    # Initialize stats and mappings files
    if not os.path.exists(STATS_FILE):
        with open(STATS_FILE, 'w') as f:
            json.dump({"files_processed": [], "total_records": 0, "total_tokens": 0}, f)
    
    if not os.path.exists(TOKEN_MAPPINGS_FILE):
        with open(TOKEN_MAPPINGS_FILE, 'w') as f:
            json.dump({}, f)
    
    # Set up file watcher
    event_handler = CSVHandler()
    observer = Observer()
    observer.schedule(event_handler, INPUT_DIR, recursive=False)
    observer.start()
    
    print(f"Watching for CSV files in {INPUT_DIR}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
