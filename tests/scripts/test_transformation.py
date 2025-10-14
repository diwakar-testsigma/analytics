#!/usr/bin/env python3

import sys
import json
import os
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent / "src"))

from transformers.transformer import DataTransformer

def test_transformation(input_file, workers=4, enable_concurrent=True):
    """Test transformation using DataTransformer with command line arguments"""
    print(f"Testing transformation with: {input_file}")
    print(f"Workers: {workers}, Concurrent: {enable_concurrent}")
    
    # Initialize transformer
    transformer = DataTransformer()
    
    # Check if it's a demons format file or standard format
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Transform the file (handles both formats)
    print("Transforming data...")
    output_file = transformer.transform_file(input_file)
    
    # Show results
    with open(output_file, 'r') as f:
        result_data = json.load(f)
    
    tables = result_data.get('tables', {})
    total_records = sum(len(records) for records in tables.values())
    
    print(f"\nTransformed {len(tables)} tables with {total_records} total records")
    print("\nTables transformed:")
    for table, records in tables.items():
        if records:
            print(f"  - {table}: {len(records)} records")
    
    print(f"\nOutput file created: {output_file}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python test_transformation.py <input_file> [workers] [concurrent]")
        print("\nExamples:")
        print("  python test_transformation.py tests/transform_files/demons_extracted_data_20250918_142750.json")
        print("  python test_transformation.py tests/transform_files/demons_extracted_data_20250918_142750.json 8")
        print("  python test_transformation.py tests/transform_files/demons_extracted_data_20250918_142750.json 4 false")
        sys.exit(1)
    
    input_file = sys.argv[1]
    workers = int(sys.argv[2]) if len(sys.argv) > 2 else 4
    enable_concurrent = sys.argv[3].lower() == 'true' if len(sys.argv) > 3 else True
    
    if not os.path.exists(input_file):
        print(f"Error: File not found: {input_file}")
        sys.exit(1)
    
    # Test transformation
    test_transformation(input_file, workers, enable_concurrent)