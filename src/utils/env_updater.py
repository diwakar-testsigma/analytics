"""
Utility to update .env file values programmatically
"""

import os
import re
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


def update_env_file(updates: Dict[str, str], env_file: str = '.env') -> bool:
    """
    Update environment variables in .env file
    
    Args:
        updates: Dictionary of key-value pairs to update
        env_file: Path to .env file (default: '.env')
        
    Returns:
        True if successful, False otherwise
    """
    try:
        env_path = Path(env_file)
        
        # Read existing content
        if env_path.exists():
            with open(env_path, 'r') as f:
                lines = f.readlines()
        else:
            lines = []
        
        # Update or add variables
        updated_lines = []
        updated_keys = set()
        
        for line in lines:
            # Skip empty lines and comments
            if not line.strip() or line.strip().startswith('#'):
                updated_lines.append(line)
                continue
            
            # Check if line contains a variable
            match = re.match(r'^(\w+)=(.*)$', line.strip())
            if match:
                key = match.group(1)
                if key in updates:
                    # Update existing variable
                    updated_lines.append(f"{key}={updates[key]}\n")
                    updated_keys.add(key)
                else:
                    # Keep existing variable
                    updated_lines.append(line)
            else:
                updated_lines.append(line)
        
        # Add new variables that weren't in the file
        for key, value in updates.items():
            if key not in updated_keys:
                updated_lines.append(f"{key}={value}\n")
        
        # Write back to file
        with open(env_path, 'w') as f:
            f.writelines(updated_lines)
        
        # Also update os.environ for current process
        for key, value in updates.items():
            os.environ[key] = value
        
        logger.info(f"Updated {len(updates)} variables in {env_file}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update .env file: {e}")
        return False


def update_extraction_state(extract_date: str, extract_direction: str, skip_extraction: bool = False) -> bool:
    """
    Update extraction state in .env file
    
    Args:
        extract_date: Date to set for EXTRACT_DATE
        extract_direction: Direction for extraction
        skip_extraction: Whether to skip extraction
        
    Returns:
        True if successful
    """
    updates = {
        'EXTRACT_DATE': extract_date,
        'EXTRACT_DIRECTION': extract_direction,
        'SKIP_EXTRACTION': 'true' if skip_extraction else 'false'
    }
    
    return update_env_file(updates)


def reset_skip_flags() -> bool:
    """Reset skip flags to false"""
    updates = {
        'SKIP_EXTRACTION': 'false'
    }
    
    return update_env_file(updates)
