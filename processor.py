import json
import logging
import multiprocessing
import os
import re
import string
import xml.etree.ElementTree as ET
from datetime import datetime
from queue import Empty

import polars as pl
import psycopg2
import psycopg2.extras
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)

from config import (
    DB_NAME,
    DB_PASSWORD,
    DB_USER,
    FTD_DIR,
    INDIVIDUAL_FILINGS_DIR,
    PROGRESS_DIR,
    STRUCTURED_DATA_DIR,
    logger,
)


class ProgressTracker:
    """Tracks processing progress for both structured data and individual filings."""

    def __init__(
        self, filename: str = os.path.join(PROGRESS_DIR, "process_progress.json")
    ):
        """
        Initialize the ProgressTracker.

        Args:
            filename (str): Path to the JSON file for storing progress.
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
                "processed": [],
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

    def mark_processed(self, category: str, year: int, quarter: int, item: str) -> None:
        """
        Mark an item as processed.

        Args:
            category (str): The category of the item ('structured_data' or 'individual_filings').
            year (int): The year of the filing.
            quarter (int): The quarter of the filing.
            item (str): The identifier of the item.
        """
        if category == "structured_data":
            if item not in self.data[category]["processed"]:
                self.data[category]["processed"].append(item)
        else:  # individual_filings
            key = f"{year}_{quarter}"
            if key not in self.data[category]:
                self.data[category][key] = {"processed": [], "failed": []}
            if item not in self.data[category][key]["processed"]:
                self.data[category][key]["processed"].append(item)
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
            key = f"{year}_{quarter}"
            if key not in self.data[category]:
                self.data[category][key] = {"processed": [], "failed": []}
            if item not in self.data[category][key]["failed"]:
                self.data[category][key]["failed"].append(item)
        self.save_progress()

    def is_processed(self, category: str, year: int, quarter: int, item: str) -> bool:
        """
        Check if an item is processed.

        Args:
            category (str): The category of the item ('structured_data' or 'individual_filings').
            year (int): The year of the filing.
            quarter (int): The quarter of the filing.
            item (str): The identifier of the item.

        Returns:
            bool: True if the item is processed, False otherwise.
        """
        if category == "structured_data":
            return item in self.data[category]["processed"]
        else:  # individual_filings
            key = f"{year}_Q{quarter}"
            return (
                key in self.data[category]
                and item in self.data[category][key]["processed"]
            )

    def get_processed_count(
        self, category: str, year: int = None, quarter: int = None
    ) -> int:
        """
        Get the count of processed items for a category and quarter.

        Args:
            category (str): The category to count ('structured_data' or 'individual_filings').
            year (int, optional): The year of the filings.
            quarter (int, optional): The quarter of the filings.

        Returns:
            int: The number of processed items in the category for the specified quarter.
        """
        if category == "structured_data":
            return len(self.data[category]["processed"])
        else:  # individual_filings
            key = f"{year}_Q{quarter}"
            return len(self.data[category].get(key, {}).get("processed", []))


progress_tracker = ProgressTracker()


def parse_xml_filing(file_path):
    """
    Parse a single XML filing and extract filing and holdings data.

    Args:
        file_path (str): Path to the XML file.

    Returns:
        tuple: A tuple containing filing data (dict) and holdings data (list of dicts).
    """

    def safe_find(element, path, namespaces):
        found = element.find(path, namespaces)
        return found.text.strip() if found is not None and found.text else None

    def safe_parse_date(date_str):
        if date_str:
            try:
                return datetime.strptime(date_str, "%m-%d-%Y").date()
            except ValueError:
                return None
        return None

    with open(file_path, "r", encoding="utf-8") as file:
        content = file.read()

    xml_sections = re.findall(r"<XML>(.*?)</XML>", content, re.DOTALL)
    if len(xml_sections) < 2:
        logger.error(f"Could not find both XML sections in {file_path}")
        return None, []

    primary_doc = ET.fromstring(xml_sections[0].strip())
    info_table = ET.fromstring(xml_sections[1].strip())

    ns = {
        "ns": "http://www.sec.gov/edgar/thirteenffiler",
        "com": "http://www.sec.gov/edgar/common",
    }

    other_managers = [
        {
            "cik": safe_find(om, "ns:cik", ns),
            "form13FFileNumber": safe_find(om, "ns:form13FFileNumber", ns),
            "name": safe_find(om, "ns:name", ns),
        }
        for om in primary_doc.findall(".//ns:otherManagersInfo2/ns:otherManager", ns)
    ]

    filing_data = {
        "accession_number": re.search(r"ACCESSION NUMBER:\s+(\S+)", content).group(1),
        "cik": safe_find(primary_doc, ".//ns:cik", ns).zfill(10),
        "filingmanager_name": safe_find(primary_doc, ".//ns:name", ns),
        "submissiontype": safe_find(primary_doc, ".//ns:submissionType", ns),
        "filing_date": datetime.strptime(
            re.search(r"<ACCEPTANCE-DATETIME>(\d+)", content).group(1), "%Y%m%d%H%M%S"
        ).date(),
        "periodofreport": safe_parse_date(
            safe_find(primary_doc, ".//ns:periodOfReport", ns)
        ),
        "reportcalendarorquarter": safe_parse_date(
            safe_find(primary_doc, ".//ns:reportCalendarOrQuarter", ns)
        ),
        "isamendment": safe_find(primary_doc, ".//ns:isAmendment", ns) == "true",
        "amendmentno": int(safe_find(primary_doc, ".//ns:amendmentNumber", ns) or 0),
        "amendmenttype": safe_find(primary_doc, ".//ns:amendmentType", ns),
        "confdeniedexpired": safe_find(primary_doc, ".//ns:confDeniedExpired", ns)
        == "true",
        "datedeniedexpired": safe_parse_date(
            safe_find(primary_doc, ".//ns:dateDeniedExpired", ns)
        ),
        "datereported": safe_parse_date(
            safe_find(primary_doc, ".//ns:dateReported", ns)
        ),
        "reasonfornonconfidentiality": safe_find(
            primary_doc, ".//ns:reasonForNonConfidentiality", ns
        ),
        "filingmanager_street1": safe_find(primary_doc, ".//ns:street1", ns),
        "filingmanager_street2": safe_find(primary_doc, ".//ns:street2", ns),
        "filingmanager_city": safe_find(primary_doc, ".//ns:city", ns),
        "filingmanager_stateorcountry": safe_find(
            primary_doc, ".//ns:stateOrCountry", ns
        ),
        "filingmanager_zipcode": safe_find(primary_doc, ".//ns:zipCode", ns),
        "otherincludedmanagerscount": int(
            safe_find(primary_doc, ".//ns:otherIncludedManagersCount", ns) or 0
        ),
        "tableentrytotal": int(
            safe_find(primary_doc, ".//ns:tableEntryTotal", ns) or 0
        ),
        "tablevaluetotal": float(
            safe_find(primary_doc, ".//ns:tableValueTotal", ns) or 0
        ),
        "isconfidentialomitted": safe_find(
            primary_doc, ".//ns:isConfidentialOmitted", ns
        )
        == "true",
        "reporttype": safe_find(primary_doc, ".//ns:reportType", ns),
        "form13ffilenumber": safe_find(primary_doc, ".//ns:form13FFileNumber", ns),
        "crdnumber": safe_find(primary_doc, ".//ns:crdNumber", ns),
        "secfilenumber": safe_find(primary_doc, ".//ns:secFileNumber", ns),
        "provideinfoforinstruction5": safe_find(
            primary_doc, ".//ns:provideInfoForInstruction5", ns
        )
        == "true",
        "additionalinformation": safe_find(
            primary_doc, ".//ns:additionalInformation", ns
        ),
        "other_managers": json.dumps(other_managers),
    }

    ns = {"ns1": "http://www.sec.gov/edgar/document/thirteenf/informationtable"}
    holdings = [
        {
            "nameofissuer": safe_find(entry, "ns1:nameOfIssuer", ns).strip().upper()
            if safe_find(entry, "ns1:nameOfIssuer", ns)
            else None,
            "titleofclass": safe_find(entry, "ns1:titleOfClass", ns).strip().upper()
            if safe_find(entry, "ns1:titleOfClass", ns)
            else None,
            "cusip": safe_find(entry, "ns1:cusip", ns).strip().upper()
            if safe_find(entry, "ns1:cusip", ns)
            else None,
            "value": float(safe_find(entry, "ns1:value", ns) or 0),
            "sshprnamt": float(
                safe_find(entry, "ns1:shrsOrPrnAmt/ns1:sshPrnamt", ns) or 0
            ),
            "sshprnamttype": safe_find(
                entry, "ns1:shrsOrPrnAmt/ns1:sshPrnamtType", ns
            ).strip()
            if safe_find(entry, "ns1:shrsOrPrnAmt/ns1:sshPrnamtType", ns)
            else None,
            "putcall": safe_find(entry, "ns1:putCall", ns),
            "investmentdiscretion": safe_find(
                entry, "ns1:investmentDiscretion", ns
            ).strip()
            if safe_find(entry, "ns1:investmentDiscretion", ns)
            else None,
            "othermanager": safe_find(entry, "ns1:otherManager", ns),
            "voting_auth_sole": int(
                safe_find(entry, "ns1:votingAuthority/ns1:Sole", ns) or 0
            ),
            "voting_auth_shared": int(
                safe_find(entry, "ns1:votingAuthority/ns1:Shared", ns) or 0
            ),
            "voting_auth_none": int(
                safe_find(entry, "ns1:votingAuthority/ns1:None", ns) or 0
            ),
        }
        for entry in info_table.findall(".//ns1:infoTable", ns)
    ]

    return filing_data, holdings


def process_structured_data(
    folder_path: str, progress_queue: multiprocessing.Queue
) -> None:
    """
    Process a single structured data folder, parsing TSV files and inserting data into the database.

    Args:
        folder_path (str): Path to the folder containing TSV files.
        progress_queue (multiprocessing.Queue): Queue to report progress back to the main process.
    """
    logging.info(f"Processing folder: {folder_path}")
    conn = None
    cur = None
    try:
        # Load TSV files into Polars DataFrames
        submission_df = pl.read_csv(
            os.path.join(folder_path, "SUBMISSION.tsv"),
            separator="\t",
        )
        coverpage_df = pl.read_csv(
            os.path.join(folder_path, "COVERPAGE.tsv"),
            separator="\t",
        )
        summarypage_df = pl.read_csv(
            os.path.join(folder_path, "SUMMARYPAGE.tsv"),
            separator="\t",
        )
        othermanager2_df = pl.read_csv(
            os.path.join(folder_path, "OTHERMANAGER2.tsv"),
            separator="\t",
        )
        infotable_df = pl.read_csv(
            os.path.join(folder_path, "INFOTABLE.tsv"),
            separator="\t",
            schema_overrides={"OTHERMANAGER": pl.Utf8},
        )

        # Join DataFrames based on ACCESSION_NUMBER
        filings_df = submission_df.join(coverpage_df, on="ACCESSION_NUMBER", how="left")
        filings_df = filings_df.join(summarypage_df, on="ACCESSION_NUMBER", how="left")

        # Convert date columns to datetime
        date_columns = [
            "FILING_DATE",
            "PERIODOFREPORT",
            "REPORTCALENDARORQUARTER",
            "DATEDENIEDEXPIRED",
            "DATEREPORTED",
        ]
        for col in date_columns:
            filings_df = filings_df.with_columns(
                pl.when(pl.col(col).is_not_null())
                .then(pl.col(col).str.strptime(pl.Date, "%d-%b-%Y"))
                .otherwise(None)
                .alias(col)
            )

        # Apply transformations and create JSON column for OTHER_MANAGERS
        filings_df = filings_df.with_columns(
            pl.col("CIK").cast(pl.Utf8).str.zfill(10),
            pl.col("OTHERINCLUDEDMANAGERSCOUNT").cast(pl.Int32).fill_null(0),
            pl.col("TABLEENTRYTOTAL").cast(pl.Int32).fill_null(0),
            pl.col("TABLEVALUETOTAL").cast(pl.Float64).fill_null(0),
            pl.col("ISAMENDMENT").eq("Y").fill_null(False),
            pl.col("CONFDENIEDEXPIRED").eq("Y").fill_null(False),
            pl.col("ISCONFIDENTIALOMITTED").eq("Y").fill_null(False),
            pl.col("PROVIDEINFOFORINSTRUCTION5").eq("Y").fill_null(False),
            pl.col("AMENDMENTNO").cast(pl.Int32).fill_null(0),
            pl.col("ACCESSION_NUMBER")
            .map_elements(
                lambda accession_number: json.dumps(
                    othermanager2_df.filter(
                        pl.col("ACCESSION_NUMBER") == accession_number
                    ).to_dicts()
                ),
                return_dtype=pl.Utf8,
            )
            .alias("OTHER_MANAGERS"),
        )

        infotable_df = infotable_df.with_columns(
            [
                pl.col("NAMEOFISSUER").str.strip_chars().str.to_uppercase(),
                pl.col("TITLEOFCLASS").str.strip_chars().str.to_uppercase(),
                pl.col("CUSIP").str.strip_chars().str.to_uppercase(),
                pl.col("INVESTMENTDISCRETION").str.strip_chars(),
                pl.col("SSHPRNAMTTYPE").str.strip_chars(),
            ]
        )

        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()

        try:
            # Get the column names from the DataFrame
            columns = filings_df.columns

            # Create the INSERT query
            insert_query = f"""
                INSERT INTO filings ({', '.join(col.lower() for col in columns)})
                VALUES %s
                ON CONFLICT (accession_number) DO NOTHING
            """

            # Prepare the data for insertion
            insert_data = [tuple(row.values()) for row in filings_df.to_dicts()]

            # Execute the insertion
            psycopg2.extras.execute_values(cur, insert_query, insert_data)

            # Fetch the filing IDs for all accession numbers in infotable_df
            accession_numbers = infotable_df["ACCESSION_NUMBER"].unique().to_list()
            cur.execute(
                """
                SELECT accession_number, id 
                FROM filings
                WHERE accession_number = ANY(%s)
                """,
                (accession_numbers,),
            )
            accession_to_id = dict(cur.fetchall())

            # Insert holdings data
            holdings_data = [
                (
                    accession_to_id.get(row.get("ACCESSION_NUMBER")),
                    row.get("NAMEOFISSUER"),
                    row.get("TITLEOFCLASS"),
                    row.get("CUSIP"),
                    row.get("VALUE"),
                    row.get("SSHPRNAMT"),
                    row.get("SSHPRNAMTTYPE"),
                    row.get("PUTCALL"),
                    row.get("INVESTMENTDISCRETION"),
                    row.get("OTHERMANAGER"),
                    row.get("VOTING_AUTH_SOLE"),
                    row.get("VOTING_AUTH_SHARED"),
                    row.get("VOTING_AUTH_NONE"),
                )
                for row in infotable_df.to_dicts()
                if accession_to_id.get(row.get("ACCESSION_NUMBER")) is not None
            ]

            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO holdings (
                    filing_id, nameofissuer, titleofclass, cusip, value, sshprnamt, 
                    sshprnamttype, putcall, investmentdiscretion, othermanager,  
                    voting_auth_sole, voting_auth_shared, voting_auth_none
                ) 
                VALUES %s
                """,
                holdings_data,
            )

            conn.commit()
            year, quarter = os.path.basename(folder_path).split("_")
            logging.info(f"Processed folder: {folder_path}")
            progress_queue.put(
                (
                    "structured_data",
                    folder_path,
                    int(year),
                    int(quarter[1]),
                    f"{year}_Q{quarter[1]}",
                )
            )
        except Exception as e:
            logging.error(f"Error processing folder {folder_path}: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        logging.error(
            f"Error processing structured data folder {folder_path}: {str(e)}"
        )


def process_individual_filing(
    file_path: str, progress_queue: multiprocessing.Queue
) -> None:
    """
    Process a single XML file and insert its data into the database.

    Args:
        file_path (str): Path to the XML file.
        progress_queue (multiprocessing.Queue): Queue to report progress back to the main process.
    """
    conn = None
    cur = None
    # logger.info(f"Processing file: {file_path}")
    try:
        filing_data, holdings = parse_xml_filing(file_path)

        if filing_data is None:
            logger.error(f"Failed to parse XML file: {file_path}")
            return

        conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
        cur = conn.cursor()

        try:
            # Insert filing data
            cur.execute(
                """
                INSERT INTO filings (
                    accession_number, cik, filingmanager_name, submissiontype, filing_date, periodofreport,
                    reportcalendarorquarter, isamendment, amendmentno, amendmenttype, confdeniedexpired,
                    datedeniedexpired, datereported, reasonfornonconfidentiality, filingmanager_street1,
                    filingmanager_street2, filingmanager_city, filingmanager_stateorcountry, filingmanager_zipcode,
                    otherincludedmanagerscount, tableentrytotal, tablevaluetotal, isconfidentialomitted,
                    reporttype, form13ffilenumber, crdnumber, secfilenumber, provideinfoforinstruction5,
                    additionalinformation, other_managers
                ) 
                VALUES (
                    %(accession_number)s, %(cik)s, %(filingmanager_name)s, %(submissiontype)s, %(filing_date)s, 
                    %(periodofreport)s, %(reportcalendarorquarter)s, %(isamendment)s, %(amendmentno)s, 
                    %(amendmenttype)s, %(confdeniedexpired)s, %(datedeniedexpired)s, %(datereported)s, 
                    %(reasonfornonconfidentiality)s, %(filingmanager_street1)s, %(filingmanager_street2)s, 
                    %(filingmanager_city)s, %(filingmanager_stateorcountry)s, %(filingmanager_zipcode)s, 
                    %(otherincludedmanagerscount)s, %(tableentrytotal)s, %(tablevaluetotal)s, 
                    %(isconfidentialomitted)s, %(reporttype)s, %(form13ffilenumber)s, %(crdnumber)s, 
                    %(secfilenumber)s, %(provideinfoforinstruction5)s, %(additionalinformation)s, %(other_managers)s
                )
                ON CONFLICT (accession_number) DO NOTHING
                RETURNING id
            """,
                filing_data,
            )

            filing_id = cur.fetchone()

            if filing_id:
                filing_id = filing_id[0]

                # Insert holdings data
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO holdings (
                        filing_id, nameofissuer, titleofclass, cusip, value, sshprnamt, 
                        sshprnamttype, putcall, investmentdiscretion, othermanager,  
                        voting_auth_sole, voting_auth_shared, voting_auth_none
                    ) 
                    VALUES %s
                    """,
                    [(filing_id,) + tuple(holding.values()) for holding in holdings],
                )

            conn.commit()
            year, quarter = os.path.basename(os.path.dirname(file_path)).split("_")
            filename = os.path.basename(file_path)
            logger.info(f"Processed XML file: {file_path}")
            progress_queue.put(
                ("individual_filings", file_path, year, quarter, filename)
            )
        except Exception as e:
            logger.error(f"Error processing XML file {file_path}: {str(e)}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()

    except Exception as e:
        logger.error(f"Error processing XML file {file_path}: {str(e)}")


def process_13f_data(quarters: list[tuple[int, int]]) -> None:
    """
    Main function to process 13F data for the specified date range.

    Args:
        quarters (list[tuple[int, int]]): List of (year, quarter) tuples to process.
    """
    progress_tracker = ProgressTracker()

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

    with progress:
        if structured_data_quarters:
            structured_data_total = len(structured_data_quarters)
            structured_data_completed = sum(
                progress_tracker.is_processed("structured_data", y, q, f"{y}_Q{q}")
                for y, q in structured_data_quarters
            )
            structured_data_task = progress.add_task(
                "Structured Data",
                total=structured_data_total,
                completed=structured_data_completed,
            )

        if individual_filing_quarters:
            individual_filings_total = sum(
                len(
                    [
                        f
                        for f in os.listdir(
                            os.path.join(INDIVIDUAL_FILINGS_DIR, f"{y}_Q{q}")
                        )
                        if f.endswith(".txt")
                    ]
                )
                for y, q in individual_filing_quarters
                if os.path.exists(os.path.join(INDIVIDUAL_FILINGS_DIR, f"{y}_Q{q}"))
            )
            individual_filings_completed = sum(
                progress_tracker.get_processed_count("individual_filings", y, q)
                for y, q in individual_filing_quarters
            )
            individual_filings_task = progress.add_task(
                "Individual Filings",
                total=individual_filings_total,
                completed=individual_filings_completed,
            )

        # Prepare tasks for processing
        remaining_structured_data = [
            os.path.join(STRUCTURED_DATA_DIR, f"{year}_Q{quarter}")
            for year, quarter in structured_data_quarters
            if not progress_tracker.is_processed(
                "structured_data", year, quarter, f"{year}_Q{quarter}"
            )
        ]

        remaining_individual_filings = [
            os.path.join(INDIVIDUAL_FILINGS_DIR, f"{year}_Q{quarter}", filename)
            for year, quarter in individual_filing_quarters
            for filename in os.listdir(
                os.path.join(INDIVIDUAL_FILINGS_DIR, f"{year}_Q{quarter}")
            )
            if filename.endswith(".txt")
            and not progress_tracker.is_processed(
                "individual_filings", year, quarter, filename
            )
        ]

        # Process tasks using multiprocessing
        with multiprocessing.Manager() as manager:
            progress_queue = manager.Queue()

            total_workers = multiprocessing.cpu_count()

            with multiprocessing.Pool(processes=total_workers) as pool:
                # Start processes for structured data
                structured_data_results = pool.starmap_async(
                    process_structured_data,
                    [(folder, progress_queue) for folder in remaining_structured_data],
                    chunksize=1,
                )

                # Start processes for individual filings
                individual_filing_results = pool.starmap_async(
                    process_individual_filing,
                    [(file, progress_queue) for file in remaining_individual_filings],
                    chunksize=1,
                )

                total_tasks = len(remaining_structured_data) + len(
                    remaining_individual_filings
                )
                completed_tasks = 0

                while completed_tasks < total_tasks:
                    try:
                        message = progress_queue.get(timeout=1)
                        category, file_path, year, quarter, item = message
                        if category == "structured_data":
                            progress_tracker.mark_processed(
                                category, year, quarter, item
                            )
                            progress.update(structured_data_task, advance=1)
                        elif category == "individual_filings":
                            progress_tracker.mark_processed(
                                category, year, quarter, item
                            )
                            progress.update(individual_filings_task, advance=1)
                        completed_tasks += 1
                    except Empty:
                        if (
                            structured_data_results.ready()
                            and individual_filing_results.ready()
                        ):
                            break
                        continue

                # Ensure all tasks are completed
                structured_data_results.get()
                individual_filing_results.get()

    progress_tracker.save_progress()


def post_process_data():
    """Performs post-processing tasks on the data in the database."""
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD)
    cur = conn.cursor()

    try:
        # 1. Remove filings before 2014
        logger.info("Removing filings before 2014...")
        cur.execute(
            """
            DELETE FROM holdings
            WHERE filing_id IN (
                SELECT id FROM filings
                WHERE EXTRACT(YEAR FROM periodofreport) < 2014
            )
            """
        )
        cur.execute(
            "DELETE FROM filings WHERE EXTRACT(YEAR FROM periodofreport) < 2014"
        )
        conn.commit()
        logger.info("Filings before 2014 removed.")

        # 2. Remove 13F-NT and 13F-NT/A filings
        logger.info("Removing 13F-NT and 13F-NT/A filings...")
        cur.execute(
            """
            DELETE FROM holdings
            WHERE filing_id IN (
                SELECT id FROM filings
                WHERE submissiontype IN ('13F-NT', '13F-NT/A')
            )
            """
        )
        cur.execute(
            "DELETE FROM filings WHERE submissiontype IN ('13F-NT', '13F-NT/A')"
        )
        conn.commit()
        logger.info("13F-NT and 13F-NT/A filings removed.")

        # 3. Pad CIKs with leading zeros
        logger.info("Padding CIKs with leading zeros...")
        cur.execute(
            """
            UPDATE filings
            SET cik = LPAD(cik, 10, '0')
            WHERE LENGTH(cik) < 10
            """
        )
        conn.commit()
        logger.info("CIKs padded with leading zeros.")

        # 4. Handle inconsistent isamendment and amendmenttype
        logger.info("Handling inconsistent isamendment and amendmenttype...")
        cur.execute(
            """
            UPDATE filings
            SET amendmenttype = 'RESTATEMENT'
            WHERE accession_number IN ('0000919185-16-000018', '0001654954-20-012510')
            """
        )
        cur.execute(
            """
            DELETE FROM holdings
            WHERE filing_id IN (
                SELECT id FROM filings
                WHERE cik = '0001780067' AND periodofreport = '2020-12-31'
            );
            DELETE FROM filings
            WHERE cik = '0001780067' AND periodofreport = '2020-12-31'
            """
        )
        conn.commit()
        logger.info("Inconsistent isamendment and amendmenttype handled.")

        # 5. Handle amendments
        logger.info("Handling amendments...")
        cur.execute("""
            CREATE TEMPORARY TABLE filing_summary AS
            SELECT 
                cik, 
                periodofreport,
                MAX(CASE WHEN amendmenttype = 'RESTATEMENT' THEN filing_date END) AS latest_restatement_date,
                MAX(CASE WHEN amendmenttype IS NULL THEN filing_date END) AS latest_original_date
            FROM filings
            GROUP BY cik, periodofreport;

            CREATE TEMPORARY TABLE filings_to_delete AS
            SELECT f.id
            FROM filings f
            JOIN filing_summary fs ON f.cik = fs.cik AND f.periodofreport = fs.periodofreport
            WHERE 
                (f.amendmenttype = 'RESTATEMENT' AND f.filing_date < fs.latest_restatement_date)
                OR (f.amendmenttype IS NULL AND fs.latest_restatement_date IS NOT NULL)
                OR (f.amendmenttype IS NULL AND f.filing_date < fs.latest_original_date);

            DELETE FROM holdings
            WHERE filing_id IN (SELECT id FROM filings_to_delete);

            DELETE FROM filings
            WHERE id IN (SELECT id FROM filings_to_delete);

            DROP TABLE filing_summary;
            DROP TABLE filings_to_delete;
        """)
        conn.commit()
        logger.info("Handled amendments.")

        # 6. Remove option holdings
        logger.info("Removing option holdings...")
        cur.execute("DELETE FROM holdings WHERE putcall IS NOT NULL")
        conn.commit()
        logger.info("Option holdings removed.")

        # 7. Clean CUSIPs
        logger.info("Cleaning CUSIPs...")
        clean_cusips(conn, cur)
        logger.info("CUSIPs cleaned.")

    except Exception as e:
        logger.error(f"Error during post-processing: {str(e)}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def clean_cusips(conn, cur):
    """
    Clean and correct CUSIPs in the holdings table.

    This function performs the following operations:
    1. Fixes misplaced CUSIPs in titleofclass and nameofissuer columns.
    2. Tries naive fixes for short CUSIPs, left pads remaining to 9 chars.

    Args:
        conn (psycopg2.connection): The database connection object.
        cur (psycopg2.cursor): The database cursor object.
    """
    try:
        # Load FTD CUSIPs
        ftd_cusips = set(
            pl.read_csv(os.path.join(FTD_DIR, "ftd_data.csv"))
            .select(pl.col("cusip").str.strip_chars().str.to_uppercase())
            .drop_nulls()
            .to_series()
        )
        logger.info(f"Loaded {len(ftd_cusips)} unique CUSIPs from FTD data")

        # Fetch all holdings
        query = """
            SELECT DISTINCT cusip, titleofclass, nameofissuer 
            FROM holdings
        """
        df = pl.read_database(query, connection=conn)
        df = df.with_columns(pl.all().str.strip_chars().str.to_uppercase())
        logger.info(f"Fetched {len(df)} unique holdings records")

        # Identify misplaced CUSIPs
        cusips_in_titleofclass = df.filter(pl.col("titleofclass").is_in(ftd_cusips))
        cusips_in_nameofissuer = df.filter(pl.col("nameofissuer").is_in(ftd_cusips))
        logger.info(f"Found {len(cusips_in_titleofclass)} CUSIPs in titleofclass")
        logger.info(f"Found {len(cusips_in_nameofissuer)} CUSIPs in nameofissuer")

        # Fix CUSIPs in titleofclass
        cur.execute(
            """
            UPDATE holdings
            SET 
                cusip = titleofclass,
                titleofclass = cusip
            WHERE titleofclass IN %s
        """,
            (tuple(cusips_in_titleofclass["titleofclass"]),),
        )
        logger.info(f"Fixed {cur.rowcount} CUSIPs mislabeled in titleofclass")

        # Fix CUSIPs in nameofissuer
        cur.execute(
            """
            UPDATE holdings
            SET 
                cusip = nameofissuer,
                nameofissuer = titleofclass,
                titleofclass = cusip
            WHERE nameofissuer IN %s
        """,
            (tuple(cusips_in_nameofissuer["nameofissuer"]),),
        )
        logger.info(f"Fixed {cur.rowcount} CUSIPs mislabeled in nameofissuer")

        def compute_check_digit(cusip):
            if len(cusip) != 8 or not cusip.isalnum():
                raise ValueError("CUSIP must be length 8 and alphanumeric characters")

            values = [
                int(c) if c.isdigit() else (ord(c) - ord("A") + 10) for c in cusip
            ]

            total = 0
            for i, value in enumerate(values):
                if i % 2 == 1:
                    value *= 2
                total += sum(divmod(value, 10))

            check_digit = (10 - (total % 10)) % 10
            return str(check_digit)

        def find_valid_cusips(cusip, ftd_cusips):
            if len(cusip) == 8:
                full_cusip = cusip + compute_check_digit(cusip)
                return full_cusip if full_cusip in ftd_cusips else None
            elif len(cusip) == 7:
                for char in string.ascii_uppercase + string.digits:
                    # Try left padding
                    left_padded = char + cusip
                    check_digit = compute_check_digit(left_padded)
                    full_cusip = left_padded + check_digit
                    if full_cusip in ftd_cusips:
                        return full_cusip

                # Try right padding
                right_padded = cusip + char
                check_digit = compute_check_digit(right_padded)
                full_cusip = right_padded + check_digit
                if full_cusip in ftd_cusips:
                    return full_cusip
            return None

        # Handle short CUSIPs
        df_remaining = df.filter(
            ~pl.col("titleofclass").is_in(ftd_cusips)
            & ~pl.col("nameofissuer").is_in(ftd_cusips)
        )
        df_cusips = (
            df_remaining.with_columns(
                pl.col("cusip")
                .map_elements(lambda x: len(x), return_dtype=pl.Int8)
                .alias("cusip_length")
            )
            .filter(pl.col("cusip_length") < 9)
            .select("cusip")
            .unique()
        )

        # 1. Check left padding
        left_padded = df_cusips.with_columns(
            pl.col("cusip").str.zfill(9).alias("fixed_cusip")
        )
        left_matched = left_padded.filter(pl.col("fixed_cusip").is_in(ftd_cusips))
        df_cusips_remaining = left_padded.filter(
            ~pl.col("fixed_cusip").is_in(ftd_cusips)
        ).drop("fixed_cusip")

        # 2. Check right padding
        right_padded = df_cusips_remaining.with_columns(
            pl.col("cusip").str.pad_end(9, "0").alias("fixed_cusip")
        )
        right_matched = right_padded.filter(pl.col("fixed_cusip").is_in(ftd_cusips))
        df_cusips_remaining = right_padded.filter(
            ~pl.col("fixed_cusip").is_in(ftd_cusips)
        ).drop("fixed_cusip")

        # 3. Apply checksum fix
        df_cusips_remaining = df_cusips_remaining.with_columns(
            pl.col("cusip")
            .map_elements(
                lambda x: find_valid_cusips(x, ftd_cusips), return_dtype=pl.String
            )
            .alias("fixed_cusip")
        )
        checksum_matched = df_cusips_remaining.filter(
            pl.col("fixed_cusip").is_not_null()
        )
        df_cusips_remaining = df_cusips_remaining.filter(
            pl.col("fixed_cusip").is_null()
        ).drop("fixed_cusip")

        # lpad remaining to 9 chars
        df_cusips_remaining = df_cusips_remaining.with_columns(
            pl.col("cusip").str.zfill(9).alias("fixed_cusip")
        )

        all_fixes = pl.concat(
            [left_matched, right_matched, checksum_matched, df_cusips_remaining]
        )
        cusip_fixes = dict(zip(all_fixes["cusip"], all_fixes["fixed_cusip"]))

        # Fix / lpad short CUSIPs
        cur.execute(
            """
            UPDATE holdings
            SET cusip = CASE cusip 
                {}
                ELSE cusip 
            END
            WHERE cusip IN %s
        """.format(
                "\n                ".join(
                    f"WHEN '{old}' THEN '{new}'" for old, new in cusip_fixes.items()
                )
            ),
            (tuple(cusip_fixes.keys()),),
        )

        logger.info(f"Fixed {cur.rowcount} short CUSIPs")

        conn.commit()
        logger.info("CUSIP cleaning completed successfully")
    except Exception as e:
        conn.rollback()
        logger.error(f"Error during CUSIP cleaning: {str(e)}")
        raise