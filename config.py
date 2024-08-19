import os
from datetime import datetime

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")

# User-Agent configuration
USER_AGENT = os.getenv("USER_AGENT")

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
    "User-Agent": USER_AGENT,
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
