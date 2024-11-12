import logging
from logging.handlers import TimedRotatingFileHandler
import requests
import os

def setup_logger(name):
    """Set up logger with file and console handlers"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Create logs directory if it doesn't exist
    log_dir = '/home/ggonzalez/airflow_project/airflow/logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # File handler
    file_handler = TimedRotatingFileHandler(
        f'{log_dir}/{name}.log',
        when='midnight',
        interval=1,
        backupCount=7
    )
    file_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )
    
    # Remove existing handlers if any
    logger.handlers = []
    
    # Add handlers
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def fetch_data(url, params=None):
    """
    Fetch data from API endpoint
    
    Args:
        url: API endpoint URL
        params: Optional query parameters
        
    Returns:
        Response content
    """
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.content
    except requests.RequestException as e:
        raise Exception(f"Error fetching data: {str(e)}")
