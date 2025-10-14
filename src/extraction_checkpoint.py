"""
Extraction Checkpoint System

Manages extraction dates for incremental loading by saving the last successful
extraction timestamp and automatically updating it after each successful run.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict
import logging

from .config import settings


class ExtractionCheckpoint:
    """Manages extraction checkpoint for incremental loading"""
    
    def __init__(self):
        self.checkpoint_file = Path(settings.EXTRACT_CHECKPOINT_FILE)
        self.logger = logging.getLogger(__name__)
    
    def get_last_extraction_date(self) -> Optional[str]:
        """
        Get the last successful extraction date
        
        Returns:
            Date string in YYYY-MM-DD format or None
        """
        if not self.checkpoint_file.exists():
            self.logger.info("No extraction checkpoint found - first run")
            return None
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
            
            last_date = data.get('last_extraction_date')
            self.logger.info(f"Last extraction date from checkpoint: {last_date}")
            return last_date
            
        except Exception as e:
            self.logger.error(f"Failed to read extraction checkpoint: {e}")
            return None
    
    def save_extraction_date(self, extraction_date: Optional[str] = None) -> bool:
        """
        Save successful extraction date to checkpoint
        
        Args:
            extraction_date: Date string in YYYY-MM-DD format. Uses today if None.
        
        Returns:
            True if saved successfully
        """
        if not settings.AUTO_UPDATE_START_DATE:
            self.logger.debug("AUTO_UPDATE_START_DATE is disabled - not saving checkpoint")
            return False
        
        try:
            # Use provided date or today
            date_to_save = extraction_date or datetime.now().strftime('%Y-%m-%d')
            
            checkpoint_data = {
                'last_extraction_date': date_to_save,
                'last_extraction_timestamp': datetime.now().isoformat(),
                'environment': settings.ENVIRONMENT,
                'data_store': settings.DATA_STORE
            }
            
            # Ensure directory exists
            self.checkpoint_file.parent.mkdir(exist_ok=True)
            
            with open(self.checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)
            
            self.logger.info(f"✅ Extraction checkpoint saved: {date_to_save}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to save extraction checkpoint: {e}")
            return False
    
    def get_checkpoint_info(self) -> Dict:
        """Get full checkpoint information"""
        if not self.checkpoint_file.exists():
            return {
                'exists': False,
                'message': 'No checkpoint file found - this is the first run'
            }
        
        try:
            with open(self.checkpoint_file, 'r') as f:
                data = json.load(f)
            
            return {
                'exists': True,
                'last_extraction_date': data.get('last_extraction_date'),
                'last_extraction_timestamp': data.get('last_extraction_timestamp'),
                'environment': data.get('environment'),
                'data_store': data.get('data_store'),
                'auto_update_enabled': settings.AUTO_UPDATE_START_DATE
            }
            
        except Exception as e:
            return {
                'exists': True,
                'error': str(e)
            }
    
    def should_update_env_file(self) -> bool:
        """
        Check if we should update .env file with new start date
        
        Note: This is optional and can be dangerous in production.
        For production, use checkpoint file instead of modifying .env
        """
        return False  # Disabled by default - modifying .env is risky
    
    def get_recommended_start_date(self) -> str:
        """
        Get recommended EXTRACT_START_DATE for next run
        
        Returns:
            Date string for next incremental extraction
        """
        last_date = self.get_last_extraction_date()
        
        if last_date:
            return last_date
        elif settings.EXTRACT_START_DATE:
            return settings.EXTRACT_START_DATE
        else:
            # Default to 30 days ago
            from datetime import timedelta
            thirty_days_ago = datetime.now() - timedelta(days=30)
            return thirty_days_ago.strftime('%Y-%m-%d')


# Global checkpoint instance
checkpoint = ExtractionCheckpoint()
