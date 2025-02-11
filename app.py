from flask import Flask, render_template, request, jsonify
import os
from datetime import datetime
from file_watcher import FileWatcher
import pandas as pd
from quantum_tokenizer import QuantumTokenizer
from tokenization_rules import TokenizationRules
import json

app = Flask(__name__)

# Initialize components
tokenizer = QuantumTokenizer()
rules = TokenizationRules()

# Ensure directories exist
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
UPLOAD_FOLDER = os.path.join(DATA_DIR, 'input')
PROCESSED_FOLDER = os.path.join(DATA_DIR, 'processed')
MAPPINGS_FILE = os.path.join(DATA_DIR, 'token_mappings.json')

for directory in [DATA_DIR, UPLOAD_FOLDER, PROCESSED_FOLDER]:
    os.makedirs(directory, exist_ok=True)

# Initialize or load token mappings
if os.path.exists(MAPPINGS_FILE):
    with open(MAPPINGS_FILE, 'r') as f:
        token_mappings = json.load(f)
else:
    token_mappings = {}

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/stats')
def get_stats():
    total_records = 0
    field_mappings = {}
    
    # Load mappings from the data directory
    mappings_file = os.path.join(DATA_DIR, 'token_mappings.json')
    if os.path.exists(mappings_file):
        try:
            with open(mappings_file, 'r') as f:
                field_mappings = json.load(f)
        except Exception as e:
            print(f"Error loading mappings: {str(e)}")
    
    # Calculate total records from processed files
    for file in os.listdir(PROCESSED_FOLDER):
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join(PROCESSED_FOLDER, file))
                total_records += len(df)
            except Exception as e:
                print(f"Error processing {file}: {str(e)}")
    
    # Load mappings directly from the file watcher's mappings file
    mappings_file = os.path.join(DATA_DIR, 'token_mappings.json')
    field_mappings = {}
    
    if os.path.exists(mappings_file):
        try:
            with open(mappings_file, 'r') as f:
                field_mappings = json.load(f)
        except Exception as e:
            print(f"Error loading mappings: {str(e)}")
    
    # Create organized mappings with actual field names
    organized_mappings = {}
    
    # Add each field with its mappings
    for field, values in field_mappings.items():
        # Skip empty mappings
        if not values:
            continue
        # Use the actual field name from the CSV
        organized_mappings[field] = values
    
    return jsonify({
        'total_records': total_records,
        'field_mappings': organized_mappings
    })

@app.route('/status')
def get_status():
    # Get processing files
    current_processing = [
        {
            'filename': filename,
            'status': info['status'],
            'source_dept': info['source_dept'],
            'target_dept': info['target_dept'],
            'timestamp': info['timestamp'],
            'progress': info.get('progress', 0)
        }
        for filename, info in processing_files.items()
    ]
    
    # Get processed files
    processed_files = []
    for filename in os.listdir(PROCESSED_FOLDER):
        if filename.endswith('.csv'):
            file_stat = os.stat(os.path.join(PROCESSED_FOLDER, filename))
            processed_time = datetime.fromtimestamp(file_stat.st_mtime)
            
            # Parse source and target departments from filename
            parts = filename.replace('.csv', '').split('_to_')
            if len(parts) >= 2:
                source_dept = parts[0]
                target_dept = parts[1].split('_')[0]
                
                processed_files.append({
                    'filename': filename,
                    'status': 'Completed',
                    'source_dept': source_dept,
                    'target_dept': target_dept,
                    'timestamp': processed_time.isoformat()
                })
    
    return jsonify({
        'processing': current_processing,
        'processed': sorted(processed_files, key=lambda x: x['timestamp'], reverse=True)
    })

@app.route('/recent-activity')
def get_recent_activity():
    all_files = []
    
    # Add processing files
    for filename, info in processing_files.items():
        all_files.append({
            'filename': filename,
            'status': info['status'],
            'timestamp': info['timestamp']
        })
    
    # Add processed files
    for filename in os.listdir(PROCESSED_FOLDER):
        if filename.endswith('.csv'):
            file_stat = os.stat(os.path.join(PROCESSED_FOLDER, filename))
            all_files.append({
                'filename': filename,
                'status': 'Completed',
                'timestamp': datetime.fromtimestamp(file_stat.st_mtime).isoformat()
            })
    
    # Sort by timestamp and return latest 10
    sorted_files = sorted(all_files, key=lambda x: x['timestamp'], reverse=True)
    return jsonify(sorted_files[:10])

if __name__ == '__main__':
    # Start the file watcher
    watcher = FileWatcher(UPLOAD_FOLDER, PROCESSED_FOLDER, tokenizer, rules)
    watcher.start()
    
    # Run the app with host='0.0.0.0' to allow external access
    app.run(debug=True, host='0.0.0.0', port=3005)
