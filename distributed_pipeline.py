import os
import time
import json
import shutil
import dask.dataframe as dd
import pandas as pd
import numpy as np
import vaex
from datetime import datetime
import random
import hashlib
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from distributed import Client, LocalCluster
import redis
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
import logging
from typing import Dict, List, Set, Tuple
import msgpack

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Directory paths
INPUT_DIR = "data/input"
PROCESSED_DIR = "data/processed"
ARCHIVE_DIR = "data/archive"
TEMP_DIR = "data/temp"
STATS_FILE = "data/stats.json"
TOKEN_MAPPINGS_FILE = "data/token_mappings.json"
CHUNK_SIZE = 100_000  # Number of rows per chunk

# Redis configuration for caching
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

# Department rules (same as before)
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

class TokenCache:
    def __init__(self):
        self.prefix = "token:"
        self.ttl = 86400  # 24 hours

    def get_token(self, value: str, field: str) -> str:
        """Get token from cache or generate new one"""
        key = f"{self.prefix}{field}:{value}"
        token = redis_client.get(key)
        if token is None:
            token = self._generate_token(value, field)
            redis_client.setex(key, self.ttl, token)
        return token

    def _generate_token(self, value: str, field: str) -> str:
        """Generate a new token using quantum-inspired entropy"""
        entropy = random.random()
        token_base = hashlib.sha256(f"{value}{entropy}{field}".encode()).hexdigest()
        return f"QT{token_base[:10]}_{field}"

class ChunkProcessor:
    def __init__(self, sensitive_fields: List[str]):
        self.sensitive_fields = sensitive_fields
        self.token_cache = TokenCache()
        self.unique_values: Dict[str, Set[str]] = {field: set() for field in sensitive_fields}

    def process_chunk(self, chunk: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Dict[str, str]]]:
        """Process a chunk of data and return tokenized chunk and mappings"""
        chunk_tokenized = chunk.copy()
        token_mappings = {}

        for field in self.sensitive_fields:
            if field in chunk.columns:
                token_mappings[field] = {}
                chunk_tokenized[field] = chunk_tokenized[field].astype(str)
                
                # Get unique values in this chunk
                unique_values = chunk[field].unique()
                
                # Generate tokens for new unique values
                for value in unique_values:
                    if pd.notna(value):
                        str_value = str(value)
                        token = self.token_cache.get_token(str_value, field)
                        token_mappings[field][str_value] = token
                        chunk_tokenized[field] = chunk_tokenized[field].replace(str_value, token)

        return chunk_tokenized, token_mappings

class DistributedProcessor:
    def __init__(self):
        # Initialize Dask cluster
        self.cluster = LocalCluster(
            n_workers=os.cpu_count(),
            threads_per_worker=2,
            memory_limit='4GB'
        )
        self.client = Client(self.cluster)
        self.token_cache = TokenCache()

    def process_file(self, file_path: str, source_dept: str, target_dept: str) -> None:
        """Process a large CSV file using distributed computing"""
        try:
            logger.info(f"Starting distributed processing of {file_path}")
            
            # Get sensitive fields
            sensitive_fields = DEPARTMENT_RULES.get((source_dept, target_dept), [])
            if not sensitive_fields:
                logger.info(f"No tokenization needed for {source_dept} to {target_dept}")
                return

            # Create temporary directory for chunks
            temp_dir = os.path.join(TEMP_DIR, datetime.now().strftime('%Y%m%d_%H%M%S'))
            os.makedirs(temp_dir, exist_ok=True)

            # Read and process data in chunks using Dask
            ddf = dd.read_csv(file_path, blocksize="256MB")
            total_rows = len(ddf)
            
            # Convert to Parquet for better performance
            temp_parquet = os.path.join(temp_dir, "temp.parquet")
            ddf.to_parquet(temp_parquet)
            
            # Process using Vaex for memory efficiency
            df = vaex.open(temp_parquet)
            
            # Initialize chunk processor
            chunk_processor = ChunkProcessor(sensitive_fields)
            
            # Process in parallel using Dask
            futures = []
            token_mappings = {}
            
            for i, chunk in enumerate(df.chunk_iterator(chunk_size=CHUNK_SIZE)):
                chunk_pd = chunk.to_pandas_df()
                future = self.client.submit(
                    chunk_processor.process_chunk,
                    chunk_pd
                )
                futures.append(future)
            
            # Collect results
            processed_chunks = []
            for future in futures:
                chunk_result, chunk_mappings = future.result()
                processed_chunks.append(chunk_result)
                
                # Update token mappings
                for field, mappings in chunk_mappings.items():
                    if field not in token_mappings:
                        token_mappings[field] = {}
                    token_mappings[field].update(mappings)
            
            # Combine processed chunks
            df_tokenized = pd.concat(processed_chunks, ignore_index=True)
            
            # Save tokenized file
            output_path = os.path.join(PROCESSED_DIR, f"tokenized_{os.path.basename(file_path)}")
            df_tokenized.to_parquet(
                output_path,
                engine='fastparquet',
                compression='snappy'
            )
            
            # Archive original file
            archive_path = os.path.join(
                ARCHIVE_DIR,
                f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(file_path)}"
            )
            shutil.move(file_path, archive_path)
            
            # Update statistics
            self._update_stats(
                file_name=os.path.basename(file_path),
                source_dept=source_dept,
                target_dept=target_dept,
                fields_tokenized=sensitive_fields,
                records_processed=total_rows
            )
            
            # Update token mappings
            self._update_token_mappings(token_mappings)
            
            # Cleanup
            shutil.rmtree(temp_dir)
            
            logger.info(f"Successfully processed {file_path}")
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {str(e)}")
            # Move failed file to archive with error prefix
            archive_path = os.path.join(
                ARCHIVE_DIR,
                f"error_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{os.path.basename(file_path)}"
            )
            shutil.move(file_path, archive_path)

    def _update_stats(self, file_name: str, source_dept: str, target_dept: str,
                     fields_tokenized: List[str], records_processed: int) -> None:
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

    def _update_token_mappings(self, new_mappings: Dict[str, Dict[str, str]]) -> None:
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

class CSVHandler(FileSystemEventHandler):
    def __init__(self):
        self.processor = DistributedProcessor()

    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.csv'):
            try:
                file_name = os.path.basename(event.src_path)
                name_parts = file_name.split('_')
                if len(name_parts) < 4 or name_parts[1] != 'to':
                    logger.error(f"Invalid filename format: {file_name}")
                    return
                
                source_dept = name_parts[0]
                target_dept = name_parts[2]
                
                logger.info(f"Processing {file_name} from {source_dept} to {target_dept}")
                self.processor.process_file(event.src_path, source_dept, target_dept)
                
            except Exception as e:
                logger.error(f"Error processing {event.src_path}: {str(e)}")

def main():
    """Main function to start the distributed pipeline"""
    # Create directories if they don't exist
    for directory in [INPUT_DIR, PROCESSED_DIR, ARCHIVE_DIR, TEMP_DIR]:
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
    
    logger.info(f"Watching for CSV files in {INPUT_DIR}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    main()
