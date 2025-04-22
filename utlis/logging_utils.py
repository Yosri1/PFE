# utils/logging_utils.py
import logging

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(level=logging.INFO)
    return logging.getLogger(__name__)
