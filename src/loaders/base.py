import json
import logging
import os
from abc import ABC, abstractmethod
from typing import Dict, Any, List
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class BaseLoader(ABC):
    def __init__(self):
        self.config = self._load_config()
        self.setup_logging()
        
    @abstractmethod
    def _load_config(self) -> Dict:
        pass
    
    def setup_logging(self):
        # Don't reconfigure logging - use existing configuration from pipeline
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def load(self, filepath: str) -> bool:
        pass
    
    def load_json_file(self, filepath: str) -> Dict[str, Any]:
        with open(filepath, 'r') as f:
            return json.load(f)
    
    def load_multiple_files(self, filepaths: List[str]) -> Dict[str, bool]:
        results = {}
        for filepath in filepaths:
            try:
                success = self.load(filepath)
                results[filepath] = success
            except Exception as e:
                self.logger.error(f"Failed to load {filepath}: {e}")
                results[filepath] = False
        return results
