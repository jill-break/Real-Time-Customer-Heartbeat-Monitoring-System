import logging
import sys
import os
from logging.handlers import RotatingFileHandler

def get_logger(name: str):
    """
    Configures and returns a production-grade logger.
    - Console Output: Detailed formatting for development/ops.
    - File Output: Rotating logs to prevent disk overflow.
    """
    logger = logging.getLogger(name)
    
    # If logger is already configured, return it to avoid duplicate handlers
    if logger.hasHandlers():
        return logger

    logger.setLevel(logging.INFO)

    # 1. Define Formatters
    # Includes: Timestamp, Log Level, Module Name, and the Message
    log_format = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # 2. Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # 3. Rotating File Handler (Production Essential)
    # Creates 'logs' directory if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Max size: 5MB per file, keeps 5 old backups
    file_handler = RotatingFileHandler(
        filename=os.path.join(log_dir, "heartbeat_system.log"),
        maxBytes=5 * 1024 * 1024, 
        backupCount=5
    )
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    # Prevent logs from propagating to the root logger
    logger.propagate = False
    
    return logger