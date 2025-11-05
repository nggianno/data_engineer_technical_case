import logging
import sys
import os
from datetime import datetime

def get_logger(
    name: str = "hema_etl",
    level: str = "INFO",
    log_dir: str = "logs",
    log_to_file: bool = True
) -> logging.Logger:
    """
    Returns a configured logger for consistent console AND file output.

    Parameters
    ----------
    name : str
        Logger name (typically module name or application name)
    level : str
        Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL
    log_dir : str
        Directory where log files will be stored
    log_to_file : bool
        Whether to write logs to file in addition to console

    Returns
    -------
    logging.Logger
        Configured logger instance

    Notes
    -----
    - Logs are written to both console (stdout) and file
    - Log files are named with timestamp: hema_etl_YYYYMMDD_HHMMSS.log
    - Creates log directory if it doesn't exist
    - Uses consistent format across all handlers
    """
    logger = logging.getLogger(name)

    # Only configure if not already configured
    if not logger.handlers:
        logger.setLevel(level.upper())
        logger.propagate = False

        # Define consistent formatter
        formatter = logging.Formatter(
            "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        # 1️⃣ Console Handler (stdout) - for real-time monitoring
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(level.upper())
        logger.addHandler(console_handler)

        # 2️⃣ File Handler - for persistent logging
        if log_to_file:
            # Create logs directory if it doesn't exist
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

            # Generate timestamped log filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_filename = f"{name}_{timestamp}.log"
            log_filepath = os.path.join(log_dir, log_filename)

            # Create file handler
            file_handler = logging.FileHandler(log_filepath, mode='a', encoding='utf-8')
            file_handler.setFormatter(formatter)
            file_handler.setLevel(level.upper())
            logger.addHandler(file_handler)

            # Log the log file location
            logger.info(f"📝 Logging to file: {log_filepath}")

    return logger

