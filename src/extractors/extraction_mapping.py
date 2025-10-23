"""
Extraction Mapping Configuration
=================================

Defines which source tables to extract from MySQL databases.
Based on transformation_mapping.py - only extract tables that are actually used.

This ensures we:
- Extract only what's needed for transformations
- Skip unnecessary tables (faster extraction)
- Keep extraction aligned with transformation logic
"""

# Import required source tables from transformation_mapping.py
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from transformers.transformation_mapping import ALL_MAPPINGS


def get_required_source_tables():
    """
    Extract all source tables from transformation mappings.
    
    Returns:
        Set of table names that need to be extracted
    """
    required_tables = set()
    
    for target_table, config in ALL_MAPPINGS.items():
        source_tables = config.get('source_tables', [])
        required_tables.update(source_tables)
    
    return required_tables


# Required source tables (extracted from transformation_mapping.py)
REQUIRED_TABLES = get_required_source_tables()


# Semi-static tables: Required but rarely updated (only created_at_epoch, no updated_at_epoch)
# Extract based on mode: if mode='skip', these can be skipped
SEMI_STATIC_TABLES = set()
# NOTE: All required tables have updated_at_epoch, so this list is empty


# Tables to always skip (not used in transformations)
SKIP_TABLES = {
    'flyway_schema_history',          # Migration tracking (not needed)
    'schema_migration',                # Schema version (not needed)
    'schema_migrations',               # Schema version (not needed)
    'SPRING_SESSION',                  # Spring sessions (not needed)
    'SPRING_SESSION_ATTRIBUTES',       # Spring session data (not needed)
    'application_types',               # App type reference (not needed)
    'object_method_details',           # NLP method details (not needed)
    'object_method_params',            # NLP method params (not needed)
    'object_type_template_mappings',   # Template mappings (not needed)
}


def should_extract_table(table_name: str) -> bool:
    """
    Determine if a table should be extracted.
    
    Args:
        table_name: Name of the table
        
    Returns:
        True if table should be extracted, False otherwise
    """
    # Always skip tables not required by transformation mappings
    if table_name in SKIP_TABLES:
        return False
    
    # Always extract required tables
    if table_name in REQUIRED_TABLES:
        return True
    
    # Unknown table: extract by default (dynamic detection will handle filtering)
    # This maintains backward compatibility for new/unknown tables
    return True


def get_extraction_info(table_name: str) -> dict:
    """
    Get extraction information for a table.
    
    Returns:
        dict with: required, category, reason
    """
    if table_name in SKIP_TABLES:
        return {
            'required': False,
            'category': 'skip',
            'reason': 'Not used in transformations'
        }
    elif table_name in REQUIRED_TABLES:
        return {
            'required': True,
            'category': 'dynamic',
            'reason': 'Required by transformation mappings'
        }
    elif table_name in SEMI_STATIC_TABLES:
        return {
            'required': True,
            'category': 'semi_static',
            'reason': 'Required but rarely updated'
        }
    else:
        return {
            'required': None,
            'category': 'unknown',
            'reason': 'Not in mapping - use dynamic detection'
        }


if __name__ == "__main__":
    print("=" * 70)
    print("Extraction Mapping Configuration")
    print("=" * 70)
    
    print(f"\nRequired tables: {len(REQUIRED_TABLES)}")
    print(f"Semi-static tables: {len(SEMI_STATIC_TABLES)}")
    print(f"Skip tables: {len(SKIP_TABLES)}")
    
    print("\nRequired tables:")
    for table in sorted(REQUIRED_TABLES):
        print(f"  • {table}")
    
    print("\nSkip tables:")
    for table in sorted(SKIP_TABLES):
        print(f"  • {table}")

