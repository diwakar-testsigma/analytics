"""
Progress tracking and notifications for ETL pipeline
"""

import psutil
import logging
from typing import Optional
from ..notifications import ETLNotifier


logger = logging.getLogger(__name__)


class ProgressTracker:
    """Track and report ETL progress with memory monitoring"""
    
    def __init__(self, etl_id: str):
        self.etl_id = etl_id
        self.current_phase = None
        self.total_items = 0
        self.completed_items = 0
        self.last_reported_percent = -10  # Report every 10%
        self.notifier = ETLNotifier(job_id=etl_id)
        
    def start_phase(self, phase: str, total_items: int):
        """Start tracking a new phase"""
        self.current_phase = phase
        self.total_items = total_items
        self.completed_items = 0
        self.last_reported_percent = -10
        
        # Log and notify start
        memory_info = self._get_memory_info()
        logger.info(f"[{phase}] Starting - Total items: {total_items} | {memory_info}")
        
        # Send Slack notification
        self._send_progress_notification(0, f"Starting {phase}")
        
    def update_progress(self, items_completed: int = 1):
        """Update progress and send notifications if milestone reached"""
        self.completed_items += items_completed
        
        if self.total_items == 0:
            return
            
        percent = int((self.completed_items / self.total_items) * 100)
        
        # Report every 10% or at completion
        if percent >= self.last_reported_percent + 10 or percent == 100:
            self.last_reported_percent = percent
            memory_info = self._get_memory_info()
            
            # Log progress
            logger.info(
                f"[{self.current_phase}] Progress: {percent}% "
                f"({self.completed_items}/{self.total_items}) | {memory_info}"
            )
            
            # Send Slack notification
            status = "Completed" if percent == 100 else f"{percent}% complete"
            self._send_progress_notification(percent, status)
    
    def _get_memory_info(self) -> str:
        """Get current memory usage info"""
        memory = psutil.virtual_memory()
        process = psutil.Process()
        process_memory_mb = process.memory_info().rss / (1024 * 1024)
        
        return (
            f"Memory: {process_memory_mb:.0f}MB process, "
            f"{memory.percent:.0f}% system ({memory.used / (1024**3):.1f}GB/{memory.total / (1024**3):.1f}GB)"
        )
    
    def _send_progress_notification(self, percent: int, status: str):
        """Send progress notification to Slack"""
        memory_info = self._get_memory_info()
        
        # Create custom notification
        memory_parts = memory_info.split(", ")
        process_memory = memory_parts[0].replace("Memory: ", "")
        system_memory = memory_parts[1] if len(memory_parts) > 1 else ""
        
        try:
            # Send custom progress notification
            self.notifier.send_custom_notification(
                title=f"ETL Progress - {self.current_phase}",
                message=f"{self.current_phase}: {status}",
                color="#3498db" if percent < 100 else "#2ecc71",  # Blue for progress, green for complete
                fields=[
                    {"title": "Progress", "value": f"{percent}% ({self.completed_items}/{self.total_items})", "short": True},
                    {"title": "Process Memory", "value": process_memory, "short": True},
                    {"title": "System Memory", "value": system_memory, "short": True},
                    {"title": "Phase", "value": self.current_phase, "short": True}
                ]
            )
        except Exception as e:
            logger.debug(f"Failed to send Slack notification: {e}")
