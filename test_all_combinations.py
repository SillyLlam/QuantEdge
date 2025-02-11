import os
import shutil
import logging
import subprocess
import time
from tokenization_rules import TokenizationRules

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(message)s')

# Sample data
SAMPLE_DATA = """Name,Phone,Salary,Email,Department,Position
John Doe,+1-555-0123,85000,john.doe@company.com,Engineering,Senior Developer
Jane Smith,+1-555-0124,92000,jane.smith@company.com,Marketing,Marketing Manager
Mike Johnson,+1-555-0125,78000,mike.j@company.com,Sales,Sales Representative
Sarah Wilson,+1-555-0126,115000,sarah.w@company.com,HR,HR Director"""

def test_combination(source, dest):
    """Test a specific department combination"""
    filename = f"{source}_to_{dest}.csv"
    input_path = os.path.join("data", "input", filename)
    processed_path = os.path.join("data", "processed", f"processed_{filename}")
    
    # Write test file
    os.makedirs(os.path.dirname(input_path), exist_ok=True)
    with open(input_path, 'w') as f:
        f.write(SAMPLE_DATA)
    
    # Wait for processing
    import time
    time.sleep(2)
    
    # Check results
    if os.path.exists(processed_path):
        logging.info(f"\nProcessed file for {source} -> {dest}:")
        with open(processed_path, 'r') as f:
            content = f.read()
            logging.info(content)
            
        # Verify rules were applied correctly
        rules = TokenizationRules.get_rules(source, dest)
        if not rules:
            logging.error(f"No rules found for {source} -> {dest}")
            return
            
        tokenize_fields = rules['tokenize']
        pass_through_fields = rules['pass_through']
        
        # Read first line to get headers
        headers = content.split('\n')[0].split(',')
        first_data = content.split('\n')[1].split(',')
        
        logging.info("\nRule Verification:")
        for field, value in zip(headers, first_data):
            if field in tokenize_fields:
                if not value.startswith('QT_'):
                    logging.error(f"Field {field} should be tokenized but value is: {value}")
                else:
                    logging.info(f"✓ Field {field} correctly tokenized")
            elif field in pass_through_fields:
                if value.startswith('QT_'):
                    logging.error(f"Field {field} should not be tokenized but value is: {value}")
                else:
                    logging.info(f"✓ Field {field} correctly passed through")
    else:
        logging.error(f"No processed file found for {source} -> {dest}")

def main():
    # Clean up previous test files
    for dir_name in ['input', 'processed', 'archive']:
        dir_path = os.path.join('data', dir_name)
        if os.path.exists(dir_path):
            shutil.rmtree(dir_path)
        os.makedirs(dir_path)
        
    # Start the file watcher in the background
    watcher_process = subprocess.Popen(['python3', 'test.py'])
    time.sleep(2)  # Wait for watcher to start
    
    # Get all departments from rules
    departments = set()
    for source, dest in TokenizationRules.DEPARTMENT_RULES.keys():
        departments.add(source)
        departments.add(dest)
    
    # Test all combinations with rules
    for source, dest in TokenizationRules.DEPARTMENT_RULES.keys():
        logging.info(f"\n{'='*50}")
        logging.info(f"Testing {source} -> {dest}")
        logging.info('='*50)
        test_combination(source, dest)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nTest completed.")
