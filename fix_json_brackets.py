#!/usr/bin/env python3
"""
Fix JSON brackets in transformed file
Adds missing ] brackets between tables
"""
import gzip
import re
import sys

def fix_json_brackets(input_file, output_file):
    print(f"Reading {input_file}...")
    
    with gzip.open(input_file, 'rt') as f:
        content = f.read()
    
    # Find patterns where a table starts without closing the previous data array
    # Pattern: "},     "table_name": {       "records": 
    # Should be: "}]},   "table_name": {       "records":
    
    # Fix missing ] before new table starts
    pattern = r'(\}\s*,)\s*"(\w+)":\s*\{\s*"records":'
    
    def add_bracket(match):
        # Check if there's already a ] before the comma
        before_comma = match.group(1)
        if ']' not in before_comma:
            return '}]\n    },\n    "' + match.group(2) + '": {\n      "records":'
        return match.group(0)
    
    print("Fixing missing brackets...")
    fixed_content = re.sub(pattern, add_bracket, content)
    
    # Also ensure the last table is properly closed
    # Find the last table data section and ensure it ends with ]}
    last_table_pattern = r'"data":\s*\[[^]]*\}\s*(\}\s*\}\s*\}\s*$)'
    
    def fix_last_table(match):
        closing = match.group(1)
        # Add ] before the closing braces if missing
        return '"data": [' + match.group(0)[match.group(0).find('[') + 1:-len(closing)] + ']\n    }\n  }\n}\n'
    
    # Apply the fix for the last table
    if not re.search(r'\]\s*\}\s*\}\s*\}\s*$', fixed_content):
        print("Fixing last table closure...")
        # Find content from last "data": [ to end
        last_data_pos = fixed_content.rfind('"data": [')
        if last_data_pos != -1:
            before_last_data = fixed_content[:last_data_pos]
            after_last_data = fixed_content[last_data_pos:]
            
            # Check if the data array is properly closed
            if not re.search(r'\]\s*\}\s*\}\s*\}', after_last_data):
                # Find the last record and add closing
                last_record = after_last_data.rfind('}')
                if last_record != -1:
                    # Find position after the last record
                    for i in range(last_record + 1, len(after_last_data)):
                        if after_last_data[i] not in ' \n\t,':
                            # Insert ] here
                            after_last_data = after_last_data[:i] + ']\n    ' + after_last_data[i:]
                            break
                
                fixed_content = before_last_data + after_last_data
    
    print(f"Writing fixed file to {output_file}...")
    with gzip.open(output_file, 'wt') as f:
        f.write(fixed_content)
    
    print("Done! File fixed.")
    
    # Validate the fix
    print("\nValidating fixed JSON...")
    import json
    try:
        with gzip.open(output_file, 'rt') as f:
            data = json.load(f)
        print(f"✓ JSON is valid! Found {len(data.get('tables', {}))} tables")
        for table, info in data.get('tables', {}).items():
            print(f"  - {table}: {info.get('records', 0)} records")
    except Exception as e:
        print(f"✗ JSON validation failed: {e}")
        print("You may need to manually inspect the file")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python fix_json_brackets.py input.json.gz output.json.gz")
        sys.exit(1)
    
    fix_json_brackets(sys.argv[1], sys.argv[2])
