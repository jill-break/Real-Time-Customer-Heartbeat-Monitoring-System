import logging
from src.utils.logger import get_logger

logger = get_logger(__name__)

class Alerter:
    @staticmethod
    def send_alert(message: str):
        """
        Sends an alert to the Data Engineer.
        Currently logs a critical message, but can be extended to send Email/Slack.
        """
        separator = "=" * 50
        alert_msg = f"\n{separator}\n🚨 ALERT: {message}\n{separator}"
        
        # Log as critical to ensure it stands out
        logger.critical(alert_msg)
        
        # TODO: Integrate with SMTP or Slack API here
        # e.g., send_email("data_eng@example.com", "Pipeline Alert", message)
