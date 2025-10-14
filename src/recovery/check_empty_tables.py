#!/usr/bin/env python3
"""
Reload tables that have 0 records
"""

import os
import sys

from .recovery import ETLRecovery
from ..loaders.data_sources import SnowflakeDataSource, SQLiteDataSource
from ..config import settings

# Check which tables have 0 records
recovery = ETLRecovery()

# Get current state
print("Checking for empty tables...")
data_source = SnowflakeDataSource(settings.SNOWFLAKE_CONNECTION_URL)
data_source.connect()

empty_tables = []
cursor = data_source.cursor
cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

for table in tables:
    table_name = table[1]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    if count == 0:
        empty_tables.append(table_name.lower())
        print(f"  ⚠️  {table_name}: 0 records")

data_source.disconnect()

if empty_tables:
    print(f"\nFound {len(empty_tables)} empty tables")
    print("These tables might need data reloading or have legitimate reasons for being empty.")
    print("\nEmpty tables:", ", ".join(empty_tables))
else:
    print("\nAll tables have data!")
