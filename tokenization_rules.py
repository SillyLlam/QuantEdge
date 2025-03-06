import logging

class TokenizationRules:
    DEPARTMENT_RULES = {
        ('HR', 'SALES'): {
            'tokenize': ['Name', 'Phone', 'Salary', 'Email', 'Department'],  
            'pass_through': ['Position']  
        },
        ('SALES', 'HR'): {
            'tokenize': ['Name', 'Phone', 'Salary', 'Email', 'Department'],  
            'pass_through': ['Position']  
        },
        ('HR', 'FINANCE'): {
            'tokenize': ['Name', 'Phone', 'Email', 'Salary', 'Department'],  
            'pass_through': ['Position']
        },
        ('FINANCE', 'HR'): {
            'tokenize': ['Name', 'Phone', 'Email', 'Salary', 'Department'],  
            'pass_through': ['Position']
        },
        ('HR', 'IT'): {
            'tokenize': ['Name', 'Salary', 'Phone', 'Department'],  
            'pass_through': ['Email', 'Position']
        },
        ('IT', 'HR'): {
            'tokenize': ['Name', 'Salary', 'Phone', 'Department'],  
            'pass_through': ['Email', 'Position']
        },
        ('SALES', 'FINANCE'): {
            'tokenize': ['Name', 'Email', 'Phone', 'Salary', 'Department'],
            'pass_through': ['Position']
        },
        ('FINANCE', 'SALES'): {
            'tokenize': ['Name', 'Email', 'Phone', 'Salary', 'Department'],
            'pass_through': ['Position']
        },
        ('SALES', 'IT'): {
            'tokenize': ['Name', 'Salary', 'Phone', 'Department'],
            'pass_through': ['Email', 'Position']
        },
        ('IT', 'SALES'): {
            'tokenize': ['Name', 'Salary', 'Phone', 'Department'],
            'pass_through': ['Email', 'Position']
        },
        ('FINANCE', 'IT'): {
            'tokenize': ['Name', 'Phone', 'Email', 'Salary', 'Department'],
            'pass_through': ['Position']
        },
        ('IT', 'FINANCE'): {
            'tokenize': ['Name', 'Phone', 'Email', 'Salary', 'Department'],
            'pass_through': ['Position']
        }
    }

    def __init__(self):
        self.current_source = None
        self.current_dest = None
        self.current_rules = None

    def should_tokenize(self, field):
        """Check if a field should be tokenized based on current rules."""
        if not self.current_rules:
            logging.warning("No rules currently set")
            return False
        
        return field in self.current_rules.get('tokenize', [])

    def set_departments(self, source_dept, dest_dept):
        """Set the current source and destination departments."""
        self.current_source = source_dept.upper()
        self.current_dest = dest_dept.upper()
        self.current_rules = self.get_rules(self.current_source, self.current_dest)
        return bool(self.current_rules)

    @classmethod
    def get_rules(cls, source_dept, dest_dept):
        """Get tokenization rules for a specific department pair."""
        source_dept = source_dept.upper()
        dest_dept = dest_dept.upper()
        
        logging.info(f"Looking up rules for {source_dept} -> {dest_dept}")
        
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
            parts = filename.replace('.csv', '').split('_to_')
            if len(parts) != 2:
                logging.error(f"Invalid filename format: {filename}")
                return None, None
            
            source_dept, dest_dept = parts[0], parts[1].split('_')[0]
            return source_dept.upper(), dest_dept.upper()
        except Exception as e:
            logging.error(f"Error parsing filename {filename}: {str(e)}")
            return None, None
