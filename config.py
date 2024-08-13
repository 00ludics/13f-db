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
HEADERS = {
    "User-Agent": "Your Name youremail@email.com",
    "Accept-Encoding": "gzip, deflate",
    "Host": "www.sec.gov",
}

RATE_LIMIT = 10
