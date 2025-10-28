"""
Memory monitoring and limiting utilities to prevent OOM crashes
"""

import os
import gc
import psutil
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class MemoryMonitor:
    """Monitor and enforce memory limits during ETL operations"""
    
    def __init__(self, max_memory_mb: int = None, enable_limit: bool = True):
        """
        Initialize memory monitor
        
        Args:
            max_memory_mb: Maximum memory allowed in MB (defaults to 80% of available)
            enable_limit: Whether to enforce memory limits
        """
        self.enable_limit = enable_limit
        
        if max_memory_mb:
            self.max_memory_mb = max_memory_mb
        else:
            # Default to 80% of available memory
            total_memory = psutil.virtual_memory().total / (1024 * 1024)
            self.max_memory_mb = int(total_memory * 0.8)
        
        self.process = psutil.Process(os.getpid())
        logger.info(f"Memory monitor initialized: limit={self.max_memory_mb}MB, enabled={self.enable_limit}")
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / (1024 * 1024)
    
    def get_memory_percent(self) -> float:
        """Get memory usage as percentage of system total"""
        return self.process.memory_percent()
    
    def check_memory(self, operation: str = "operation") -> None:
        """
        Check current memory usage and raise error if limit exceeded
        
        Args:
            operation: Description of current operation for error message
            
        Raises:
            MemoryError: If memory limit is exceeded
        """
        if not self.enable_limit:
            return
        
        current_mb = self.get_memory_usage()
        
        if current_mb > self.max_memory_mb:
            percent = self.get_memory_percent()
            error_msg = (
                f"Memory limit exceeded during {operation}: "
                f"{current_mb:.1f}MB > {self.max_memory_mb}MB limit "
                f"({percent:.1f}% of system memory)"
            )
            logger.error(error_msg)
            
            # Try to free some memory before failing
            gc.collect()
            
            # Check again after GC
            current_mb = self.get_memory_usage()
            if current_mb > self.max_memory_mb:
                raise MemoryError(error_msg)
            else:
                logger.warning(f"Memory freed by GC, now at {current_mb:.1f}MB")
    
    def check_available_memory(self, required_mb: int, operation: str = "operation") -> bool:
        """
        Check if enough memory is available for an operation
        
        Args:
            required_mb: Memory required in MB
            operation: Description of operation
            
        Returns:
            bool: True if enough memory available
        """
        if not self.enable_limit:
            return True
        
        current_mb = self.get_memory_usage()
        available_mb = self.max_memory_mb - current_mb
        
        if available_mb < required_mb:
            logger.warning(
                f"Insufficient memory for {operation}: "
                f"need {required_mb}MB, have {available_mb}MB available"
            )
            
            # Try garbage collection
            gc.collect()
            
            # Check again
            current_mb = self.get_memory_usage()
            available_mb = self.max_memory_mb - current_mb
            
            if available_mb < required_mb:
                return False
        
        return True
    
    def log_memory_status(self, context: str = "") -> None:
        """Log current memory status"""
        current_mb = self.get_memory_usage()
        percent = self.get_memory_percent()
        
        status = f"Memory usage"
        if context:
            status = f"{context} - {status}"
        
        logger.info(
            f"{status}: {current_mb:.1f}MB / {self.max_memory_mb}MB "
            f"({percent:.1f}% of system)"
        )


def estimate_table_memory(table_data: dict) -> int:
    """
    Estimate memory usage for a table's data in MB
    
    Args:
        table_data: Dictionary with 'records' count and optionally 'sample' data
        
    Returns:
        Estimated memory usage in MB
    """
    if not isinstance(table_data, dict):
        return 0
    
    records = table_data.get('records', 0)
    sample = table_data.get('sample', [])
    
    if not records or not sample:
        return 0
    
    # Estimate based on sample size
    if sample:
        # Get average size of sample records
        import sys
        sample_size = sum(sys.getsizeof(record) for record in sample[:10])
        avg_record_size = sample_size / min(len(sample), 10)
        
        # Estimate total size with overhead
        estimated_bytes = avg_record_size * records * 1.5  # 1.5x for Python overhead
        estimated_mb = estimated_bytes / (1024 * 1024)
        
        return int(estimated_mb)
    
    # Fallback: assume 1KB per record
    return max(1, int(records / 1000))  # At least 1MB
