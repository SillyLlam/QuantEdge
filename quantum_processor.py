import os
import pandas as pd
import logging
from quantum_tokenizer import QuantumTokenizer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def tokenize_file():
    try:
        # Initialize quantum tokenizer
        tokenizer = QuantumTokenizer()
        
        # Get absolute paths
        base_dir = os.path.dirname(os.path.abspath(__file__))
        input_file = os.path.join(base_dir, "data", "input", "HR_to_Sales.csv")
        output_dir = os.path.join(base_dir, "data", "processed")
        output_file = os.path.join(output_dir, "processed_HR_to_Sales.csv")
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        logging.info(f"Input file: {input_file}")
        logging.info(f"Output file: {output_file}")
        
        # Read input file
        df = pd.read_csv(input_file)
        logging.info(f"Read input file with {len(df)} rows")
        
        # Define fields to tokenize (based on HR to Sales rules)
        fields_to_tokenize = ['Name', 'Phone', 'Salary', 'Email']
        
        # Process each field
        processed_df = df.copy()
        for field in fields_to_tokenize:
            if field in df.columns:
                logging.info(f"\nTokenizing {field}:")
                # Tokenize each unique value
                for value in df[field].unique():
                    if pd.notna(value):
                        # Generate quantum token
                        token_a, _ = tokenizer.generate_token_pair()
                        quantum_token = f'QT_{token_a[:12]}'
                        logging.info(f"{value} -> {quantum_token}")
                        # Replace value with token
                        processed_df.loc[df[field] == value, field] = quantum_token
        
        # Save processed file
        processed_df.to_csv(output_file, index=False)
        logging.info(f"\nProcessed file saved to {output_file}")
        
        # Display sample of processed data
        logging.info("\nSample of processed data:")
        print(processed_df.head().to_string())
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")

if __name__ == "__main__":
    tokenize_file()