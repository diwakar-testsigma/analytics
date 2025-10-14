#!/usr/bin/env python3
"""
Clear SQLite Database Data Script
================================

This script clears all data from the SQLite analytics database while preserving
the table structure. This is useful when you need to re-run the ETL pipeline
with updated schema or data.

Usage:
    python clear_sqlite_data.py

Features:
- Clears all data from all tables
- Preserves table structure and schema
- Provides detailed logging of operations
- Safe to run multiple times
"""

import sqlite3
import os
import sys
from pathlib import Path

def get_database_path():
    """Get the path to the SQLite database."""
    # Get the project root directory (two levels up from this script)
    script_dir = Path(__file__).parent
    project_root = script_dir.parent.parent
    db_path = project_root / "data" / "analytics.db"
    return db_path

def get_all_tables(cursor):
    """Get all table names from the database."""
    cursor.execute("""
        SELECT name FROM sqlite_master 
        WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
    """)
    return [row[0] for row in cursor.fetchall()]

def clear_table_data(cursor, table_name):
    """Clear all data from a specific table."""
    try:
        cursor.execute(f"DELETE FROM {table_name}")
        return cursor.rowcount
    except sqlite3.Error as e:
        print(f"Error clearing table {table_name}: {e}")
        return 0

def reset_auto_increment(cursor, table_name):
    """Reset auto-increment counter for a table."""
    try:
        cursor.execute(f"DELETE FROM sqlite_sequence WHERE name='{table_name}'")
    except sqlite3.Error:
        # Table might not have auto-increment, that's fine
        pass

def clear_database():
    """Clear all data from the SQLite database."""
    db_path = get_database_path()
    
    if not db_path.exists():
        print(f"❌ Database not found at: {db_path}")
        print("Please ensure the database exists before running this script.")
        return False
    
    print(f"🗄️  Database path: {db_path}")
    print(f"📊 Database size before: {db_path.stat().st_size / 1024:.1f} KB")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        # Get all tables
        tables = get_all_tables(cursor)
        print(f"📋 Found {len(tables)} tables to clear:")
        
        total_rows_cleared = 0
        
        # Clear each table
        for table_name in tables:
            print(f"  🧹 Clearing table: {table_name}")
            rows_cleared = clear_table_data(cursor, table_name)
            total_rows_cleared += rows_cleared
            print(f"    ✅ Cleared {rows_cleared} rows")
            
            # Reset auto-increment counter
            reset_auto_increment(cursor, table_name)
        
        # Commit all changes
        conn.commit()
        
        # Vacuum the database to reclaim space
        print("🔧 Vacuuming database to reclaim space...")
        cursor.execute("VACUUM")
        
        # Get final database size
        conn.close()
        final_size = db_path.stat().st_size / 1024
        
        print(f"\n✅ Database cleared successfully!")
        print(f"📊 Total rows cleared: {total_rows_cleared}")
        print(f"📊 Database size after: {final_size:.1f} KB")
        print(f"💾 Space reclaimed: {db_path.stat().st_size / 1024:.1f} KB")
        
        return True
        
    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def verify_database_empty():
    """Verify that the database is empty."""
    db_path = get_database_path()
    
    try:
        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()
        
        tables = get_all_tables(cursor)
        total_rows = 0
        
        print("\n🔍 Verifying database is empty:")
        for table_name in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            total_rows += count
            status = "✅" if count == 0 else "❌"
            print(f"  {status} {table_name}: {count} rows")
        
        conn.close()
        
        if total_rows == 0:
            print("✅ Database is completely empty!")
            return True
        else:
            print(f"⚠️  Database still has {total_rows} rows total")
            return False
            
    except Exception as e:
        print(f"❌ Error verifying database: {e}")
        return False

def main():
    """Main function."""
    print("🧹 SQLite Database Data Clearing Script")
    print("=" * 50)
    
    # Check if running in non-interactive mode
    if len(sys.argv) > 1 and sys.argv[1] == '--force':
        print("🚀 Running in non-interactive mode (--force flag detected)")
    else:
        # Confirm before proceeding
        try:
            response = input("\n⚠️  This will clear ALL DATA from the analytics database. Continue? (y/N): ")
            if response.lower() not in ['y', 'yes']:
                print("❌ Operation cancelled.")
                return
        except EOFError:
            print("❌ Cannot read input. Use --force flag for non-interactive mode.")
            print("Usage: python3 clear_sqlite_data.py --force")
            return
    
    # Clear the database
    success = clear_database()
    
    if success:
        # Verify it's empty
        verify_database_empty()
        print("\n🎉 Database clearing completed successfully!")
        print("💡 You can now re-run your ETL pipeline to populate with fresh data.")
    else:
        print("\n❌ Database clearing failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
