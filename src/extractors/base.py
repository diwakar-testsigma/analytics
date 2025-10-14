import json
import os
from datetime import datetime
from abc import ABC, abstractmethod
from typing import Dict, Any, List
import logging

class BaseExtractor(ABC):
    def __init__(self):
        self.config = self._load_config()
        self.setup_logging()
        
    @abstractmethod
    def _load_config(self) -> Dict:
        pass
    
    def setup_logging(self):
        logging.basicConfig(
            level=getattr(logging, self.config.get('log_level', 'INFO')),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
    
    @abstractmethod
    def extract(self, *args, **kwargs) -> str:
        pass
    
    def save_to_json(self, data: List[Dict[str, Any]], table_name: str) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{table_name}_{timestamp}.json"
        filepath = os.path.join(self.config.get('output_dir', 'output'), filename)
        
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        output_data = {
            'table': table_name,
            'timestamp': timestamp,
            'row_count': len(data),
            'data': data
        }
        
        with open(filepath, 'w') as f:
            json.dump(output_data, f, default=str)
        
        self.logger.info(f"Saved {len(data)} records to {filepath}")
        return filepath
