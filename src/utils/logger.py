import sys
import logging 
from logging.handlers import RotatingFileHandler
from src.utils.constants import LOG_FILE_PATH, LOG_MAX_SIZE, LOG_BACKUP_COUNT, LOG_LEVEL

_loggers = {}

def setup_logging():
    LOG_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)

    detailed_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s",
            datefmt='%Y-%m-%d %H:%M:%S'
    )

    simple_formatter = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )

    # File handler with rotation
    file_handler = RotatingFileHandler(
        LOG_FILE_PATH,
        maxBytes=LOG_MAX_SIZE,
        backupCount=LOG_BACKUP_COUNT
    )
    file_handler.setFormatter(detailed_formatter)
    file_handler.setLevel(logging.DEBUG)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(simple_formatter)
    console_handler.setLevel(getattr(logging, LOG_LEVEL))
    
    # Error file handler
    error_handler = RotatingFileHandler(
        LOG_FILE_PATH.parent / 'errors.log',
        maxBytes=LOG_MAX_SIZE,
        backupCount=LOG_BACKUP_COUNT
    )
    error_handler.setFormatter(detailed_formatter)
    error_handler.setLevel(logging.ERROR)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(error_handler)
    
    # Suppress noisy libraries
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('selenium').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)

def get_logger(name):
    if name not in _loggers:
        if not logging.getLogger().handlers:
            setup_logging()
        logger = logging.getLogger(name)
        _loggers[name] = logger
    
    return _loggers[name]

# [HERE] Special logger for tracking scraping progress