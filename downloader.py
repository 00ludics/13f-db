import asyncio
import json
import logging
import os
import random
import time
import zipfile
from datetime import datetime

import aiohttp
from config import (
    BASE_URL,
    HEADERS,
    INDIVIDUAL_FILINGS_DIR,
    PROGRESS_DIR,
    RATE_LIMIT,
    STRUCTURED_DATA_DIR,
    STRUCTURED_DATA_URL,
)
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

logger = logging.getLogger(__name__)

# Constants
MAX_RETRIES = 10
INITIAL_RETRY_DELAY = 1


class RateLimiter:
    """Implements a token bucket rate limiter."""

    def __init__(self, rate_limit: int):
        """
        Initialize the RateLimiter.

        Args:
            rate_limit (int): The maximum number of requests allowed per second.
        """
        self.rate_limit = rate_limit
        self.tokens = rate_limit
        self.last_refill_time = time.monotonic()

    async def wait(self):
        """
        Wait for a token to become available.

        This method implements the token bucket algorithm to enforce the rate limit.
        """
        while True:
            now = time.monotonic()
            time_since_refill = now - self.last_refill_time
            new_tokens = time_since_refill * self.rate_limit
            if new_tokens > 1:
                self.tokens = min(self.rate_limit, self.tokens + new_tokens)
                self.last_refill_time = now
            if self.tokens >= 1:
                self.tokens -= 1
                return
            await asyncio.sleep(1 / self.rate_limit)


rate_limiter = RateLimiter(RATE_LIMIT)


class ProgressTracker:
    """Tracks download progress."""

    def __init__(
        self, filename: str = os.path.join(PROGRESS_DIR, "download_progress.json")
    ) -> None:
        """
        Initialize the ProgressTracker.

        Args:
            filename (str): The path to the JSON file for storing progress.
        """
        self.filename = filename
        self.data = self._load_progress()

    def _load_progress(self) -> dict:
        """
        Load progress from file or create new progress structure.

        Returns:
            dict: The loaded progress data or a new progress structure.
        """
        if os.path.exists(self.filename):
            with open(self.filename, "r") as f:
                return json.load(f)
        return {
            "structured_data": {
                "downloaded": [],
                "extracted": [],
                "failed": [],
            },
            "individual_filings": {},
            "last_updated": datetime.now().isoformat(),
        }

    def save_progress(self) -> None:
        """Save current progress to file."""
        self.data["last_updated"] = datetime.now().isoformat()
        with open(self.filename, "w") as f:
            json.dump(self.data, f, indent=2)

    def mark_downloaded(self, category: str, item: str) -> None:
        """
        Mark an item as downloaded.

        Args:
            category (str): The category of the item ('structured_data' or 'individual_filings').
            item (str): The identifier of the item.
        """
        if category == "structured_data":
            if item not in self.data[category]["downloaded"]:
                self.data[category]["downloaded"].append(item)
        self.save_progress()

    def mark_completed(self, category: str, year: int, quarter: int, item: str) -> None:
        """
        Mark an item as completed (extracted for structured data, downloaded for individual filings).

        Args:
            category (str): The category of the item ('structured_data' or 'individual_filings').
            year (int): The year of the filing.
            quarter (int): The quarter of the filing.
            item (str): The identifier of the item.
        """
        if category == "structured_data":
            if item not in self.data[category]["extracted"]:
                self.data[category]["extracted"].append(item)
        else:  # individual_filings
            key = f"{year}_Q{quarter}"
            if key not in self.data[category]:
                self.data[category][key] = {"completed": [], "failed": []}
            if item not in self.data[category][key]["completed"]:
                self.data[category][key]["completed"].append(item)
        self.save_progress()

    def mark_failed(self, category: str, year: int, quarter: int, item: str) -> None:
        """
        Mark an item as failed.

        Args:
            category (str): The category of the item ('structured_data' or 'individual_filings').
            year (int): The year of the filing.
            quarter (int): The quarter of the filing.
            item (str): The identifier of the item.
        """
        if category == "structured_data":
            if item not in self.data[category]["failed"]:
                self.data[category]["failed"].append(item)
        else:  # individual_filings
            key = f"{year}_Q{quarter}"
            if key not in self.data[category]:
                self.data[category][key] = {"completed": [], "failed": []}
            if item not in self.data[category][key]["failed"]:
                self.data[category][key]["failed"].append(item)
        self.save_progress()

    def is_downloaded(self, category: str, item: str) -> bool:
        """
        Check if an item is downloaded.

        Args:
            category (str): The category of the item ('structured_data' or 'individual_filings').
            item (str): The identifier of the item.

        Returns:
            bool: True if the item is downloaded, False otherwise.
        """
        if category == "structured_data":
            return item in self.data[category]["downloaded"]
        return False  # For individual filings, we don't track downloads separately

    def is_completed(self, category: str, year: int, quarter: int, item: str) -> bool:
        """
        Check if an item is completed (extracted for structured data, downloaded for individual filings).

        Args:
            category (str): The category of the item ('structured_data' or 'individual_filings').
            year (int): The year of the filing.
            quarter (int): The quarter of the filing.
            item (str): The identifier of the item.

        Returns:
            bool: True if the item is completed, False otherwise.
        """
        if category == "structured_data":
            return item in self.data[category]["extracted"]
        else:  # individual_filings
            key = f"{year}_Q{quarter}"
            return (
                key in self.data[category]
                and item in self.data[category][key]["completed"]
            )

    def remove_download_mark(self, category: str, item: str) -> None:
        """
        Remove the download mark for an item.

        Args:
            category (str): The category of the item ('structured_data' or 'individual_filings').
            item (str): The identifier of the item.
        """
        if category == "structured_data":
            if item in self.data[category]["downloaded"]:
                self.data[category]["downloaded"].remove(item)
        self.save_progress()

    def get_completed_count(
        self, category: str, year: int = None, quarter: int = None
    ) -> int:
        """
        Get the count of completed items for a category and quarter.

        Args:
            category (str): The category to count ('structured_data' or 'individual_filings').
            year (int, optional): The year of the filings.
            quarter (int, optional): The quarter of the filings.

        Returns:
            int: The number of completed items in the category for the specified quarter.
        """
        if category == "structured_data":
            return len(self.data[category]["extracted"])
        else:  # individual_filings
            key = f"{year}_Q{quarter}"
            return len(self.data[category].get(key, {}).get("completed", []))

    def get_completed_items(
        self, category: str, year: int = None, quarter: int = None
    ) -> set:
        """
        Get the set of completed items for a category and quarter.

        Args:
            category (str): The category to get items from ('structured_data' or 'individual_filings').
            year (int, optional): The year of the filings.
            quarter (int, optional): The quarter of the filings.

        Returns:
            set: The set of completed items in the category for the specified quarter.
        """
        if category == "structured_data":
            return set(self.data[category]["extracted"])
        else:  # individual_filings
            key = f"{year}_Q{quarter}"
            return set(self.data[category].get(key, {}).get("completed", []))


progress_tracker = ProgressTracker()


async def download_file(
    session: aiohttp.ClientSession, url: str, filename: str
) -> bool:
    """
    Download a file from the given URL.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        url (str): The URL of the file to download.
        filename (str): The local path where the file should be saved.

    Returns:
        bool: True if the download was successful, False otherwise.
    """
    for attempt in range(MAX_RETRIES):
        await rate_limiter.wait()
        try:
            async with session.get(url, headers=HEADERS) as response:
                if response.status == 200:
                    os.makedirs(os.path.dirname(filename), exist_ok=True)
                    with open(filename, "wb") as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
                    logger.info(f"Downloaded: {filename}")
                    return True
                elif response.status in [403, 404]:
                    logger.warning(
                        f"{'Access forbidden' if response.status == 403 else 'File not found'}: {url}"
                    )
                    return False
                else:
                    logger.error(f"Failed to download {url}: HTTP {response.status}")
        except Exception as e:
            logger.error(f"Error downloading {url} to {filename}: {str(e)}")

        retry_delay = INITIAL_RETRY_DELAY * (2**attempt) + random.uniform(0, 1)
        logger.info(f"Retrying in {retry_delay:.2f} seconds...")
        await asyncio.sleep(retry_delay)

    return False


async def extract_zip(filename: str) -> bool:
    """
    Extract a zip file and delete the original asynchronously.

    Args:
        filename (str): The path to the zip file to extract.

    Returns:
        bool: True if extraction was successful, False otherwise.
    """
    try:
        await asyncio.to_thread(
            zipfile.ZipFile(filename, "r").extractall, os.path.splitext(filename)[0]
        )
        await asyncio.to_thread(os.remove, filename)
        logger.info(f"Extracted: {filename}")
        return True
    except Exception as e:
        logger.error(f"Error extracting {filename}: {str(e)}")
        return False


async def download_structured_data(
    session: aiohttp.ClientSession,
    year: int,
    quarter: int,
    progress: Progress,
    task_id: int,
) -> None:
    """
    Download and extract structured data for a specific year and quarter.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        year (int): The year of the data to download.
        quarter (int): The quarter of the data to download.
        progress (Progress): The progress tracker object.
        task_id (int): The ID of the task in the progress tracker.
    """
    url = f"{STRUCTURED_DATA_URL}{year}q{quarter}_form13f.zip"
    filename = os.path.join(STRUCTURED_DATA_DIR, f"{year}_Q{quarter}.zip")
    item_id = f"{year}_Q{quarter}"

    if progress_tracker.is_completed("structured_data", year, quarter, item_id):
        progress.update(task_id, advance=1)
        return

    if progress_tracker.is_downloaded("structured_data", item_id):
        if not os.path.exists(filename):
            logger.warning(f"File marked as downloaded but not found: {filename}")
            progress_tracker.remove_download_mark("structured_data", item_id)
        else:
            logger.info(f"File already downloaded: {filename}")
            if await extract_zip(filename):
                progress_tracker.mark_completed(
                    "structured_data", year, quarter, item_id
                )
                progress.update(task_id, advance=1)
                return
            else:
                progress_tracker.mark_failed("structured_data", year, quarter, item_id)
                return

    success = await download_file(session, url, filename)
    if success:
        progress_tracker.mark_downloaded("structured_data", item_id)
        if await extract_zip(filename):
            progress_tracker.mark_completed("structured_data", year, quarter, item_id)
            progress.update(task_id, advance=1)
        else:
            progress_tracker.mark_failed("structured_data", year, quarter, item_id)
    else:
        progress_tracker.mark_failed("structured_data", year, quarter, item_id)


async def get_filings_for_quarter(
    session: aiohttp.ClientSession, year: int, quarter: int
) -> list[tuple[int, int, str, str, str, str]]:
    """
    Get all 13F filings for a specific quarter.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        year (int): The year of the filings to retrieve.
        quarter (int): The quarter of the filings to retrieve.

    Returns:
        list[tuple[int, int, str, str, str, str]]: A list of tuples containing filing information.
    """
    url = f"{BASE_URL}edgar/full-index/{year}/QTR{quarter}/master.idx"
    filename = os.path.join(
        INDIVIDUAL_FILINGS_DIR, f"{year}_Q{quarter}", f"master_{year}_Q{quarter}.idx"
    )

    await download_file(session, url, filename)

    filings = []
    if os.path.exists(filename):
        with open(filename, "r") as f:
            for line in f.readlines()[10:]:  # Skip header
                parts = line.strip().split("|")
                if len(parts) == 5 and parts[2] in ["13F-HR", "13F-HR/A"]:
                    cik = parts[0]
                    form_type = parts[2]
                    date = parts[3]
                    file_path = parts[4]
                    filings.append((year, quarter, cik, form_type, date, file_path))

    # Remove the master index file
    os.remove(filename)

    return filings


async def download_individual_filing(
    session: aiohttp.ClientSession,
    year: int,
    quarter: int,
    filing: tuple[int, int, str, str, str, str],
    progress: Progress,
    task_id: int,
):
    """
    Download an individual 13F filing.

    Args:
        session (aiohttp.ClientSession): The aiohttp session to use for the request.
        year (int): The year of the filing.
        quarter (int): The quarter of the filing.
        filing (tuple[int, int, str, str, str, str]): A tuple containing filing information.
        progress (Progress): The progress tracker object.
        task_id (int): The ID of the task in the progress tracker.
    """
    _, _, cik, form_type, date, file_path = filing
    url = f"{BASE_URL}{file_path}"
    file_id = os.path.basename(file_path)
    output_file = os.path.join(INDIVIDUAL_FILINGS_DIR, f"{year}_Q{quarter}", file_id)

    if not progress_tracker.is_completed("individual_filings", year, quarter, file_id):
        success = await download_file(session, url, output_file)
        if success:
            progress_tracker.mark_completed(
                "individual_filings", year, quarter, file_id
            )
            progress.update(task_id, advance=1)
        else:
            progress_tracker.mark_failed("individual_filings", year, quarter, file_id)


async def download_13f_data(quarters: list[tuple[int, int]]):
    """
    Main function to download 13F data for the specified date range.

    Args:
        quarters (list[tuple[int, int]]): A list of tuples containing year and quarter pairs.
    """
    structured_data_quarters = [(y, q) for y, q in quarters if y < 2024]
    individual_filing_quarters = [(y, q) for y, q in quarters if y >= 2024]

    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
        MofNCompleteColumn(),
        TimeRemainingColumn(),
    )

    async with aiohttp.ClientSession() as session:
        # Get all the quarterly indexes
        all_filings = []
        for year, quarter in individual_filing_quarters:
            quarter_filings = await get_filings_for_quarter(session, year, quarter)
            completed_filings = progress_tracker.get_completed_items(
                "individual_filings", year, quarter
            )
            new_filings = [
                f
                for f in quarter_filings
                if os.path.basename(f[5]) not in completed_filings
            ]
            all_filings.extend(new_filings)

        with progress:
            tasks = []

            # Set up progress bar for structured data
            if structured_data_quarters:
                structured_data_task = progress.add_task(
                    "Structured Data",
                    total=len(structured_data_quarters),
                    completed=progress_tracker.get_completed_count("structured_data"),
                )
                tasks.extend(
                    [
                        download_structured_data(
                            session, year, quarter, progress, structured_data_task
                        )
                        for year, quarter in structured_data_quarters
                        if not progress_tracker.is_completed(
                            "structured_data", year, quarter, f"{year}_Q{quarter}"
                        )
                    ]
                )

            # Set up progress bar for individual filings
            if individual_filing_quarters:
                total_individual_filings = len(all_filings) + sum(
                    progress_tracker.get_completed_count("individual_filings", y, q)
                    for y, q in individual_filing_quarters
                )
                completed_individual_filings = sum(
                    progress_tracker.get_completed_count("individual_filings", y, q)
                    for y, q in individual_filing_quarters
                )
                individual_filings_task = progress.add_task(
                    "Individual Filings",
                    total=total_individual_filings,
                    completed=completed_individual_filings,
                )
                tasks.extend(
                    [
                        download_individual_filing(
                            session,
                            filing[0],
                            filing[1],
                            filing,
                            progress,
                            individual_filings_task,
                        )
                        for filing in all_filings
                    ]
                )

            # Run all tasks concurrently if there are any
            if tasks:
                await asyncio.gather(*tasks)

    progress_tracker.save_progress()
