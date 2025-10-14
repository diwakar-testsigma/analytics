"""
ETL Checkpoint System

Provides checkpoint functionality to track ETL progress and enable
resuming from the last successful point after failures.
"""

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Set
import logging


class ETLCheckpoint:
    """Manages ETL checkpoints for failure recovery"""
    
    def __init__(self, job_id: str, checkpoint_dir: str = "checkpoints"):
        self.job_id = job_id
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(exist_ok=True)
        self.checkpoint_file = self.checkpoint_dir / f"{job_id}_checkpoint.json"
        self.logger = logging.getLogger(__name__)
        
        # Initialize or load checkpoint
        self.checkpoint_data = self._load_checkpoint()
    
    def _load_checkpoint(self) -> Dict:
        """Load existing checkpoint or create new one"""
        if self.checkpoint_file.exists():
            with open(self.checkpoint_file, 'r') as f:
                return json.load(f)
        
        return {
            'job_id': self.job_id,
            'started_at': datetime.now().isoformat(),
            'status': 'in_progress',
            'phases': {
                'extraction': {'status': 'pending', 'details': {}},
                'transformation': {'status': 'pending', 'details': {}},
                'loading': {'status': 'pending', 'details': {}}
            },
            'loaded_tables': [],
            'failed_tables': [],
            'skipped_tables': []
        }
    
    def save(self):
        """Save checkpoint to disk"""
        with open(self.checkpoint_file, 'w') as f:
            json.dump(self.checkpoint_data, f, indent=2)
        self.logger.debug(f"Checkpoint saved: {self.checkpoint_file}")
    
    def update_phase(self, phase: str, status: str, details: Optional[Dict] = None):
        """Update phase status"""
        self.checkpoint_data['phases'][phase]['status'] = status
        if details:
            self.checkpoint_data['phases'][phase]['details'].update(details)
        self.checkpoint_data['last_updated'] = datetime.now().isoformat()
        self.save()
    
    def mark_table_loaded(self, table_name: str, record_count: int):
        """Mark a table as successfully loaded"""
        self.checkpoint_data['loaded_tables'].append({
            'table': table_name,
            'records': record_count,
            'loaded_at': datetime.now().isoformat()
        })
        self.save()
    
    def mark_table_failed(self, table_name: str, error: str):
        """Mark a table as failed"""
        self.checkpoint_data['failed_tables'].append({
            'table': table_name,
            'error': error,
            'failed_at': datetime.now().isoformat()
        })
        self.save()
    
    def mark_table_skipped(self, table_name: str, reason: str):
        """Mark a table as skipped"""
        self.checkpoint_data['skipped_tables'].append({
            'table': table_name,
            'reason': reason,
            'skipped_at': datetime.now().isoformat()
        })
        self.save()
    
    def get_loaded_tables(self) -> Set[str]:
        """Get set of successfully loaded table names"""
        return {t['table'] for t in self.checkpoint_data['loaded_tables']}
    
    def get_failed_tables(self) -> Set[str]:
        """Get set of failed table names"""
        return {t['table'] for t in self.checkpoint_data['failed_tables']}
    
    def should_resume(self) -> bool:
        """Check if this job should be resumed"""
        return (
            self.checkpoint_data['status'] == 'in_progress' and
            len(self.checkpoint_data['loaded_tables']) > 0
        )
    
    def complete(self, success: bool = True):
        """Mark checkpoint as complete"""
        self.checkpoint_data['status'] = 'completed' if success else 'failed'
        self.checkpoint_data['completed_at'] = datetime.now().isoformat()
        self.save()
    
    def get_summary(self) -> Dict:
        """Get checkpoint summary"""
        return {
            'job_id': self.job_id,
            'status': self.checkpoint_data['status'],
            'loaded_tables': len(self.checkpoint_data['loaded_tables']),
            'failed_tables': len(self.checkpoint_data['failed_tables']),
            'skipped_tables': len(self.checkpoint_data['skipped_tables']),
            'phases': {
                phase: data['status'] 
                for phase, data in self.checkpoint_data['phases'].items()
            }
        }
    
    def cleanup(self):
        """Remove checkpoint file"""
        if self.checkpoint_file.exists():
            self.checkpoint_file.unlink()
            self.logger.info(f"Checkpoint cleaned up: {self.checkpoint_file}")
