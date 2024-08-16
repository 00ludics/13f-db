import logging
import os
from datetime import datetime

# Database configuration
DB_NAME = "13F_DB"
DB_USER = "USER"
DB_PASSWORD = "PASSWORD"
DB_HOST = "localhost"

# Directory setup
DATA_DIR = "data"
STRUCTURED_DATA_DIR = os.path.join(DATA_DIR, "structured_data")
INDIVIDUAL_FILINGS_DIR = os.path.join(DATA_DIR, "individual_filings")
FTD_DIR = os.path.join(DATA_DIR, "ftd")
PROGRESS_DIR = "progress"
LOG_DIR = "logs"

# Date range limits
EARLIEST_ALLOWED_DATE = "2014-Q1"
CURRENT_YEAR = datetime.now().year
CURRENT_QUARTER = (datetime.now().month - 1) // 3 + 1
LATEST_ALLOWED_DATE = f"{CURRENT_YEAR}-Q{CURRENT_QUARTER}"

# Download configuration
BASE_URL = "https://www.sec.gov/Archives/"
STRUCTURED_DATA_URL = (
    "https://www.sec.gov/files/structureddata/data/form-13f-data-sets/"
)
FTD_BASE_URL = "https://www.sec.gov"
FTD_URL = "https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data"
HEADERS = {
    "User-Agent": "Your Name youremail@email.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}

RATE_LIMIT = 10

for directory in [
    DATA_DIR,
    STRUCTURED_DATA_DIR,
    INDIVIDUAL_FILINGS_DIR,
    PROGRESS_DIR,
    LOG_DIR,
    FTD_DIR,
]:
    os.makedirs(directory, exist_ok=True)


# Global logger
logger = None


def setup_logging():
    """
    Set up logging for the application.

    Returns:
        logging.Logger: The configured logger.
    """
    global logger
    logger = logging.getLogger("13f_db")
    logger.setLevel(logging.INFO)

    # Create a formatter
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Check if a file handler already exists
    if not any(isinstance(handler, logging.FileHandler) for handler in logger.handlers):
        # Create a file handler
        current_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"13f_db_{current_time}.log"
        file_handler = logging.FileHandler(os.path.join(LOG_DIR, log_filename))
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


logger = setup_logging()
