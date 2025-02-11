import logging

class TokenizationRules:
    # Define which fields should be tokenized when data is shared between departments
    DEPARTMENT_RULES = {
        ('HR', 'SALES'): {
            'tokenize': ['Name', 'Phone', 'Salary', 'Email'],  # Tokenize sensitive info
            'pass_through': ['Department', 'Position']  # These fields won't be tokenized
        },
        ('HR', 'FINANCE'): {
            'tokenize': ['Name', 'Phone', 'Email'],  # Finance needs real salary data
            'pass_through': ['Salary', 'Department', 'Position']
        },
        ('HR', 'IT'): {
            'tokenize': ['Name', 'Salary', 'Phone'],  # IT needs email for system access
            'pass_through': ['Email', 'Department', 'Position']
        },
        ('SALES', 'FINANCE'): {
            'tokenize': ['Name', 'Email', 'Phone'],
            'pass_through': ['Department', 'Position', 'Salary']
        },
        ('SALES', 'IT'): {
            'tokenize': ['Name', 'Salary', 'Phone'],
            'pass_through': ['Email', 'Department', 'Position']
        },
        ('FINANCE', 'IT'): {
            'tokenize': ['Name', 'Phone', 'Email'],
            'pass_through': ['Salary', 'Department', 'Position']
        }
    }

    @classmethod
    def get_rules(cls, source_dept, dest_dept):
        """Get tokenization rules for a specific department pair."""
        # Normalize department names
        source_dept = source_dept.upper()
        dest_dept = dest_dept.upper()
        
        logging.info(f"Looking up rules for {source_dept} -> {dest_dept}")
        
        # Check if rules exist for this department pair
        key = (source_dept, dest_dept)
        if key not in cls.DEPARTMENT_RULES:
            logging.error(f"No rules found for {key}. Available rules: {list(cls.DEPARTMENT_RULES.keys())}")
            return None
        
        rules = cls.DEPARTMENT_RULES[key]
        logging.info(f"Found rules: tokenize={rules['tokenize']}, pass_through={rules['pass_through']}")
        return rules
        
    @classmethod
    def parse_filename(cls, filename):
        """Parse source and destination departments from filename."""
        try:
            # Remove .csv extension and split by '_to_'
            parts = filename.replace('.csv', '').split('_to_')
            if len(parts) != 2:
                logging.error(f"Invalid filename format: {filename}")
                return None, None
            
            source_dept, dest_dept = parts
            return source_dept.upper(), dest_dept.upper()
        except Exception as e:
            logging.error(f"Error parsing filename {filename}: {str(e)}")
            return None, None
