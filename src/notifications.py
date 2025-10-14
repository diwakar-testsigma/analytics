"""
Notification System for ETL Pipeline

Supports Slack notifications for immediate alerts on ETL status.
"""

import json
import logging
from typing import Dict, Optional, List
from datetime import datetime
import requests

from .config import settings


class SlackNotifier:
    """Send notifications to Slack via webhook"""
    
    def __init__(self):
        self.webhook_url = settings.SLACK_WEBHOOK_URL
        self.enabled = settings.ENABLE_NOTIFICATIONS and bool(self.webhook_url)
        self.logger = logging.getLogger(__name__)
    
    def _send_message(self, blocks: List[Dict], text: str) -> bool:
        """Send message to Slack"""
        if not self.enabled:
            self.logger.debug("Slack notifications disabled")
            return False
        
        try:
            payload = {
                "text": text,
                "blocks": blocks
            }
            
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={'Content-Type': 'application/json'},
                timeout=10
            )
            
            if response.status_code == 200:
                self.logger.info("Slack notification sent successfully")
                return True
            else:
                self.logger.error(f"Slack notification failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            self.logger.error(f"Failed to send Slack notification: {e}")
            return False
    
    def send_etl_started(self, job_id: str) -> bool:
        """Notify that ETL has started"""
        if not (self.enabled and settings.NOTIFICATION_ON_SUCCESS):
            return False
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "🚀 ETL Pipeline Started"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Job ID:*\n{job_id}"},
                    {"type": "mrkdwn", "text": f"*Environment:*\n{settings.ENVIRONMENT}"},
                    {"type": "mrkdwn", "text": f"*Data Store:*\n{settings.DATA_STORE.upper()}"},
                    {"type": "mrkdwn", "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"}
                ]
            }
        ]
        
        return self._send_message(blocks, f"ETL Pipeline Started: {job_id}")
    
    def send_etl_success(
        self, 
        job_id: str, 
        duration: float,
        records_extracted: int,
        records_transformed: int,
        records_loaded: int,
        tables_loaded: int
    ) -> bool:
        """Notify that ETL completed successfully"""
        if not (self.enabled and settings.NOTIFICATION_ON_SUCCESS):
            return False
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "✅ ETL Pipeline Completed Successfully"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Job ID:*\n{job_id}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{duration:.2f}s"},
                    {"type": "mrkdwn", "text": f"*Records Extracted:*\n{records_extracted:,}"},
                    {"type": "mrkdwn", "text": f"*Records Transformed:*\n{records_transformed:,}"},
                    {"type": "mrkdwn", "text": f"*Records Loaded:*\n{records_loaded:,}"},
                    {"type": "mrkdwn", "text": f"*Tables Loaded:*\n{tables_loaded}"}
                ]
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"Completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                ]
            }
        ]
        
        return self._send_message(blocks, f"ETL Success: {job_id}")
    
    def send_etl_partial_success(
        self,
        job_id: str,
        duration: float,
        tables_succeeded: int,
        tables_failed: int,
        failed_tables: List[str],
        records_loaded: int
    ) -> bool:
        """Notify that ETL completed with some failures"""
        if not (self.enabled and settings.NOTIFICATION_ON_PARTIAL):
            return False
        
        failed_list = "\n".join([f"• {table}" for table in failed_tables[:5]])
        if len(failed_tables) > 5:
            failed_list += f"\n• ... and {len(failed_tables) - 5} more"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "⚠️ ETL Pipeline Completed with Errors"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Job ID:*\n{job_id}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{duration:.2f}s"},
                    {"type": "mrkdwn", "text": f"*Tables Succeeded:*\n{tables_succeeded}"},
                    {"type": "mrkdwn", "text": f"*Tables Failed:*\n{tables_failed}"},
                    {"type": "mrkdwn", "text": f"*Records Loaded:*\n{records_loaded:,}"},
                    {"type": "mrkdwn", "text": f"*Load Strategy:*\n{settings.LOAD_STRATEGY}"}
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Failed Tables:*\n{failed_list}"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"⚡ Check logs and use recovery API to retry failed tables"
                    }
                ]
            }
        ]
        
        return self._send_message(blocks, f"ETL Partial Success: {job_id}")
    
    def send_etl_failure(
        self,
        job_id: str,
        duration: float,
        phase: str,
        error: str
    ) -> bool:
        """Notify that ETL failed"""
        if not (self.enabled and settings.NOTIFICATION_ON_FAILURE):
            return False
        
        # Truncate error if too long
        error_display = error[:500] + "..." if len(error) > 500 else error
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": "❌ ETL Pipeline Failed"
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Job ID:*\n{job_id}"},
                    {"type": "mrkdwn", "text": f"*Duration:*\n{duration:.2f}s"},
                    {"type": "mrkdwn", "text": f"*Failed Phase:*\n{phase.upper()}"},
                    {"type": "mrkdwn", "text": f"*Environment:*\n{settings.ENVIRONMENT}"}
                ]
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n```{error_display}```"
                }
            },
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"🔍 Check logs for details • Failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                ]
            }
        ]
        
        return self._send_message(blocks, f"ETL Failed: {job_id} in {phase} phase")


class NotificationService:
    """Central notification service"""
    
    def __init__(self):
        self.slack = SlackNotifier()
        self.logger = logging.getLogger(__name__)
    
    def notify_etl_started(self, job_id: str):
        """Send notification that ETL started"""
        try:
            self.slack.send_etl_started(job_id)
        except Exception as e:
            self.logger.error(f"Failed to send start notification: {e}")
    
    def notify_etl_completed(self, job_id: str, metrics: Dict):
        """Send notification based on ETL completion status"""
        try:
            duration = metrics.get('duration_seconds', 0)
            
            # Check if there were any failures during loading
            loading_metrics = metrics.get('loading', {})
            failed_tables = loading_metrics.get('failed_tables', [])
            
            if not metrics.get('success', False):
                # Complete failure
                errors = metrics.get('errors', [])
                error_msg = errors[0] if errors else "Unknown error"
                
                # Determine which phase failed
                if metrics['extraction'].get('success', True):
                    if metrics['transformation'].get('success', True):
                        phase = 'loading'
                    else:
                        phase = 'transformation'
                else:
                    phase = 'extraction'
                
                self.slack.send_etl_failure(
                    job_id=job_id,
                    duration=duration,
                    phase=phase,
                    error=str(error_msg)
                )
            
            elif failed_tables:
                # Partial success - some tables failed
                self.slack.send_etl_partial_success(
                    job_id=job_id,
                    duration=duration,
                    tables_succeeded=loading_metrics.get('tables_loaded_count', 0),
                    tables_failed=len(failed_tables),
                    failed_tables=[f['table'] for f in failed_tables],
                    records_loaded=loading_metrics.get('records_loaded', 0)
                )
            
            else:
                # Complete success
                self.slack.send_etl_success(
                    job_id=job_id,
                    duration=duration,
                    records_extracted=metrics['extraction'].get('records_extracted', 0),
                    records_transformed=metrics['transformation'].get('records_transformed', 0),
                    records_loaded=loading_metrics.get('records_loaded', 0),
                    tables_loaded=loading_metrics.get('tables_loaded_count', 0)
                )
        
        except Exception as e:
            self.logger.error(f"Failed to send completion notification: {e}")


# Global notification service instance
notifier = NotificationService()
