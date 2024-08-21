import asyncio
import atexit
import logging
import os
from datetime import datetime

import typer
from rich.console import Console

from config import (
    EARLIEST_ALLOWED_DATE,
    FTD_DIR,
    LATEST_ALLOWED_DATE,
    LOG_DIR,
)
from db import (
    create_database,
    create_indices,
    create_tables,
    create_views,
    refresh_views,
    reset_database,
)
from downloader import download_13f_data, download_and_process_ftd_data
from processor import clean_cusips, post_process_data, process_13f_data

app = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]}, add_completion=False
)
console = Console()


def setup_logging():
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, "13f_db.log")

    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    def finalize_logging():
        for handler in logging.root.handlers[:]:
            handler.close()
        if os.path.exists(log_file):
            new_log_file = os.path.join(
                LOG_DIR, f"13f_db_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            )
            os.rename(log_file, new_log_file)

    atexit.register(finalize_logging)


# Set up logging at the start of the CLI
setup_logging()


def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    Validate the input date range.

    Args:
        start_date (str): Start date in YYYY-Qi format
        end_date (str): End date in YYYY-Qi format

    Returns:
        bool: True if the date range is valid, False otherwise
    """
    try:
        start = datetime.strptime(start_date, "%Y-Q%d")
        end = datetime.strptime(end_date, "%Y-Q%d")
        earliest = datetime.strptime(EARLIEST_ALLOWED_DATE, "%Y-Q%d")
        latest = datetime.strptime(LATEST_ALLOWED_DATE, "%Y-Q%d")

        if start < earliest:
            console.print(
                f":cross_mark: Error: Start date {start_date} is before the earliest allowed date {EARLIEST_ALLOWED_DATE}",
                style="bold red",
            )
            return False
        if end > latest:
            console.print(
                f":cross_mark: Error: End date {end_date} is after the latest allowed date {LATEST_ALLOWED_DATE}",
                style="bold red",
            )
            return False
        if start > end:
            console.print(
                f":cross_mark: Error: Start date {start_date} is after the end date {end_date}",
                style="bold red",
            )
            return False

        return True
    except ValueError as e:
        console.print(
            f":cross_mark: Error: Invalid date format. Please use YYYY-Qi format. Details: {str(e)}",
            style="bold red",
        )
        return False


def generate_quarters(start_date: str, end_date: str) -> list[tuple[int, int]]:
    """
    Generate a list of (year, quarter) tuples between start_date and end_date.

    Args:
        start_date (str): The start date in the format 'YYYY-Qi'.
        end_date (str): The end date in the format 'YYYY-Qi'.

    Returns:
        list[tuple[int, int]]: A list of (year, quarter) tuples.
    """
    start_year, start_quarter = map(int, start_date.split("-Q"))
    end_year, end_quarter = map(int, end_date.split("-Q"))

    quarters = []
    current_year, current_quarter = start_year, start_quarter

    while (current_year, current_quarter) <= (end_year, end_quarter):
        quarters.append((current_year, current_quarter))
        current_quarter += 1
        if current_quarter > 4:
            current_year += 1
            current_quarter = 1

    return quarters


@app.command("reset")
@app.command("r")
def reset(
    confirm: bool = typer.Option(
        False, "--confirm", "-c", help="Confirm database reset without prompting"
    ),
):
    """Reset the database."""
    if not confirm:
        console.print(
            "Warning: This action will permanently delete all data in the database.",
            style="bold yellow",
        )
        confirmed = typer.confirm("Are you sure you want to proceed?")
        if not confirmed:
            console.print("Database reset cancelled.", style="bold red")
            raise typer.Abort()

    console.print("Resetting database...", style="bold green")
    reset_database()
    console.print("Database reset complete.", style="bold green")


@app.command("download")
@app.command("d")
def download(
    start_date: str = typer.Option(
        EARLIEST_ALLOWED_DATE, help="Start date (YYYY-Qi format)"
    ),
    end_date: str = typer.Option(LATEST_ALLOWED_DATE, help="End date (YYYY-Qi format)"),
):
    """Download 13F data"""
    if not validate_date_range(start_date, end_date):
        raise typer.Exit(code=1)
    console.print("Downloading 13F data...", style="bold green")
    quarters = generate_quarters(start_date, end_date)
    asyncio.run(download_13f_data(quarters))
    console.print("13F data downloaded.", style="bold green")


@app.command("process")
@app.command("p")
def process(
    start_date: str = typer.Option(
        EARLIEST_ALLOWED_DATE, help="Start date (YYYY-Qi format)"
    ),
    end_date: str = typer.Option(LATEST_ALLOWED_DATE, help="End date (YYYY-Qi format)"),
):
    """Process 13F data"""
    if not validate_date_range(start_date, end_date):
        raise typer.Exit(code=1)
    console.print("Creating database...", style="bold green")
    create_database()
    console.print("Database created.", style="bold green")
    console.print("Creating tables...", style="bold green")
    create_tables()
    console.print("Tables created.", style="bold green")
    console.print("Processing 13F data...", style="bold green")
    quarters = generate_quarters(start_date, end_date)
    process_13f_data(quarters)
    console.print("13F data processed.", style="bold green")
    console.print("Creating indices...", style="bold green")
    create_indices()
    console.print("Indices created.", style="bold green")


@app.command("postprocess")
@app.command("pp")
def postprocess():
    """Perform post-processing tasks."""
    console.print("Performing post-processing tasks...", style="bold green")
    post_process_data()
    console.print("Post-processing complete.", style="bold green")

    if not os.path.exists(os.path.join(FTD_DIR, "ftd_data.csv")):
        console.print("Downloading and processing FTD data...", style="bold green")
        asyncio.run(download_and_process_ftd_data())
        console.print("FTD data downloaded and processed.", style="bold green")

    console.print("Cleaning CUSIPs...", style="bold green")
    clean_cusips()
    console.print("CUSIPs cleaned.", style="bold green")

    console.print("Creating views...", style="bold green")
    create_views()
    console.print("Views created.", style="bold green")


@app.command("full")
@app.command("f")
def full_process(
    start_date: str = typer.Option(
        EARLIEST_ALLOWED_DATE, help="Start date (YYYY-Qi format)"
    ),
    end_date: str = typer.Option(LATEST_ALLOWED_DATE, help="End date (YYYY-Qi format)"),
    confirm: bool = typer.Option(
        False, "--confirm", "-c", help="Confirm database reset without prompting"
    ),
):
    """Perform full process: reset, download, process, and postprocess."""
    if not confirm:
        console.print(
            "Warning: This action will permanently delete all data in the database.",
            style="bold yellow",
        )
        confirmed = typer.confirm("Are you sure you want to proceed?")
        if not confirmed:
            console.print("Database reset cancelled.", style="bold red")
            raise typer.Abort()

    reset(confirm=True)
    download(start_date, end_date)
    process(start_date, end_date)
    postprocess()


@app.command("update")
@app.command("u")
def update():
    """Update the database with the current quarter's data."""
    download(LATEST_ALLOWED_DATE, LATEST_ALLOWED_DATE)
    process(LATEST_ALLOWED_DATE, LATEST_ALLOWED_DATE)

    console.print("Performing post-processing tasks...", style="bold green")
    post_process_data()
    console.print("Post-processing complete.", style="bold green")

    console.print("Refreshing views...", style="bold green")
    refresh_views()
    console.print("Views refreshed.", style="bold green")


if __name__ == "__main__":
    app()
