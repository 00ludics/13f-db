import os

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER, PROGRESS_DIR, logger


def create_extension(conn):
    """Create the necessary database extension."""
    cur = None
    try:
        cur = conn.cursor()
        cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        conn.commit()
        logger.info("pg_trgm extension created or already exists.")
    except psycopg2.Error as e:
        logger.error(f"Error creating extension: {str(e)}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()


def create_database():
    """Create the database."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname="postgres", user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(DB_NAME)))
        logger.info(f"Database '{DB_NAME}' created successfully.")
    except psycopg2.Error as e:
        logger.error(f"Error creating database: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def drop_database():
    """Drop the database."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname="postgres", user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(
            sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(DB_NAME))
        )
        logger.info(f"Database '{DB_NAME}' dropped successfully.")
    except psycopg2.Error as e:
        logger.error(f"Error dropping database: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def terminate_database_connections():
    """Terminate all connections to the database."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname="postgres", user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT pg_terminate_backend(pg_stat_activity.pid)
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid();
            """,
            (DB_NAME,),
        )
        logger.info(f"All connections to database '{DB_NAME}' terminated.")
    except psycopg2.Error as e:
        logger.error(f"Error terminating database connections: {str(e)}")
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def reset_database():
    """Reset the database by terminating connections, dropping, recreating, and adding extensions."""
    try:
        terminate_database_connections()
        drop_database()
        create_database()
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        create_extension(conn)
        if os.path.exists(os.path.join(PROGRESS_DIR, "processing_progress.json")):
            os.remove(os.path.join(PROGRESS_DIR, "processing_progress.json"))
    except psycopg2.Error as e:
        logger.error(f"Error resetting database: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()


def create_tables():
    """Create the necessary tables in the database."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        cur = conn.cursor()

        # Check if tables exist
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'filings'
            )
        """)
        tables_exist = cur.fetchone()[0]

        if not tables_exist:
            # Create filings table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS filings (
                    id BIGSERIAL PRIMARY KEY,
                    accession_number VARCHAR(25) NOT NULL UNIQUE,
                    cik VARCHAR(10) NOT NULL,
                    filingmanager_name VARCHAR(150) NOT NULL,
                    submissiontype VARCHAR(10) NOT NULL,
                    filing_date DATE NOT NULL,
                    periodofreport DATE NOT NULL,
                    reportcalendarorquarter DATE,
                    isamendment BOOLEAN,
                    amendmentno INTEGER,
                    amendmenttype VARCHAR(20),
                    confdeniedexpired BOOLEAN,
                    datedeniedexpired DATE,
                    datereported DATE,
                    reasonfornonconfidentiality VARCHAR(40),
                    filingmanager_street1 VARCHAR(40),
                    filingmanager_street2 VARCHAR(40),
                    filingmanager_city VARCHAR(30),
                    filingmanager_stateorcountry VARCHAR(2),
                    filingmanager_zipcode VARCHAR(10),
                    otherincludedmanagerscount INTEGER,
                    tableentrytotal INTEGER,
                    tablevaluetotal NUMERIC,
                    isconfidentialomitted BOOLEAN,
                    reporttype VARCHAR(30),
                    form13ffilenumber VARCHAR(17),
                    crdnumber VARCHAR(20),
                    secfilenumber VARCHAR(20),
                    provideinfoforinstruction5 BOOLEAN,
                    additionalinformation TEXT,
                    other_managers JSONB DEFAULT '[]'::jsonb NOT NULL,
                    filing_year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM filing_date)) STORED,
                    filing_quarter INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM filing_date)) STORED,
                    report_year INTEGER GENERATED ALWAYS AS (EXTRACT(YEAR FROM periodofreport)) STORED,
                    report_quarter INTEGER GENERATED ALWAYS AS (EXTRACT(QUARTER FROM periodofreport)) STORED,
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create holdings table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS holdings (
                    id BIGSERIAL PRIMARY KEY,
                    filing_id BIGINT NOT NULL REFERENCES filings(id),
                    nameofissuer VARCHAR(200),
                    titleofclass VARCHAR(150),
                    cusip VARCHAR(9) NOT NULL,
                    value NUMERIC,
                    sshprnamt NUMERIC,
                    sshprnamttype VARCHAR(10),
                    putcall VARCHAR(10),
                    investmentdiscretion VARCHAR(10),
                    othermanager VARCHAR(100),
                    voting_auth_sole BIGINT,
                    voting_auth_shared BIGINT,
                    voting_auth_none BIGINT,
                    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
            logger.info("Tables created successfully.")
        else:
            logger.info("Tables already exist. Skipping table creation.")

    except psycopg2.Error as e:
        logger.error(f"Error creating tables: {str(e)}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def create_indices():
    """Create necessary indices for the database tables."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        cur = conn.cursor()

        logger.info("Starting index creation...")
        index_queries = [
            (
                "idx_filings_cik_periodofreport",
                "CREATE INDEX IF NOT EXISTS idx_filings_cik_periodofreport ON filings (cik, periodofreport);",
            ),
            (
                "idx_filings_periodofreport",
                "CREATE INDEX IF NOT EXISTS idx_filings_periodofreport ON filings (periodofreport);",
            ),
            (
                "idx_filings_filing_date",
                "CREATE INDEX IF NOT EXISTS idx_filings_filing_date ON filings (filing_date);",
            ),
            (
                "idx_filings_filingmanager_name",
                "CREATE INDEX IF NOT EXISTS idx_filings_filingmanager_name ON filings USING gin (filingmanager_name gin_trgm_ops);",
            ),
            (
                "idx_filings_amendmenttype",
                "CREATE INDEX IF NOT EXISTS idx_filings_amendmenttype ON filings (amendmenttype);",
            ),
            (
                "idx_filings_report_year_quarter",
                "CREATE INDEX IF NOT EXISTS idx_filings_report_year_quarter ON filings (report_year, report_quarter);",
            ),
            (
                "idx_holdings_filing_id",
                "CREATE INDEX IF NOT EXISTS idx_holdings_filing_id ON holdings (filing_id);",
            ),
            (
                "idx_holdings_cusip_filing_id",
                "CREATE INDEX IF NOT EXISTS idx_holdings_cusip_filing_id ON holdings (cusip, filing_id);",
            ),
        ]

        for index_name, query in index_queries:
            try:
                cur.execute(query)
                conn.commit()
                logger.info(f"Index {index_name} created successfully.")
            except psycopg2.Error as e:
                logger.error(f"Error creating index {index_name}: {str(e)}")
                conn.rollback()

        logger.info("All indices created.")
    except psycopg2.Error as e:
        logger.error(f"Unexpected error during index creation: {str(e)}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def drop_indices():
    """Drop all indices from the tables."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        cur = conn.cursor()
        try:
            logger.info("Dropping all indices...")
            cur.execute("""
                DROP INDEX IF EXISTS idx_filings_cik_periodofreport;
                DROP INDEX IF EXISTS idx_filings_periodofreport;
                DROP INDEX IF EXISTS idx_filings_filing_date;
                DROP INDEX IF EXISTS idx_filings_filingmanager_name;
                DROP INDEX IF EXISTS idx_filings_amendmenttype;
                DROP INDEX IF EXISTS idx_filings_report_year_quarter;
                DROP INDEX IF EXISTS idx_holdings_filing_id;
                DROP INDEX IF EXISTS idx_holdings_cusip_filing_id;
            """)
            conn.commit()
            logger.info("All indices dropped.")
        except psycopg2.Error as e:
            logger.error(f"Unexpected error during index creation: {str(e)}")
            conn.rollback()
            raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def create_views():
    """Create materialized views for the database."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        cur = conn.cursor()

        # Create aggregate_holdings materialized view
        logger.info("Creating aggregate_holdings materialized view...")
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS aggregate_holdings AS
            SELECT 
                h.filing_id,
                h.cusip,
                h.nameofissuer,
                h.titleofclass,
                SUM(h.value) as value,
                SUM(h.sshprnamt) as sshprnamt,
                h.sshprnamttype,
                h.putcall,
                SUM(h.voting_auth_sole) as voting_auth_sole,
                SUM(h.voting_auth_shared) as voting_auth_shared,
                SUM(h.voting_auth_none) as voting_auth_none,
                f.periodofreport,
                EXTRACT(YEAR FROM f.periodofreport) AS report_year,
                EXTRACT(QUARTER FROM f.periodofreport) AS report_quarter
            FROM 
                holdings h
            JOIN 
                filings f ON h.filing_id = f.id
            GROUP BY 
                h.filing_id, h.cusip, h.nameofissuer, h.titleofclass, h.sshprnamttype, h.putcall,
                f.periodofreport;
        """)

        # Create indices on aggregate_holdings materialized view
        logger.info("Creating indices on aggregate_holdings materialized view...")
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_aggregate_holdings_on_filing_id ON aggregate_holdings (filing_id);
            CREATE INDEX IF NOT EXISTS idx_aggregate_holdings_on_cusip_and_filing_id ON aggregate_holdings (cusip, filing_id);
            CREATE INDEX IF NOT EXISTS idx_aggregate_holdings_cusip_year_quarter_filing_id ON aggregate_holdings (cusip, report_year, report_quarter, filing_id);
        """)
        conn.commit()
        logger.info("aggregate_holdings materialized view and indices created.")

        # Create filers materialized view
        logger.info("Creating filers materialized view...")
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS filers AS 
            WITH most_recent AS (
                SELECT DISTINCT ON (filings.cik)
                    filings.cik,
                    filings.filingmanager_name,
                    filings.filingmanager_city,
                    filings.filingmanager_stateorcountry,
                    filings.filing_date AS most_recent_date_filed
                FROM filings
                ORDER BY filings.cik, filings.filing_date DESC, filings.id
            ), 
            counts AS (
                SELECT 
                    filings.cik,
                    COUNT(*) AS filings_count
                FROM filings
                GROUP BY filings.cik
            )
            SELECT 
                most_recent.cik,
                most_recent.filingmanager_name,
                most_recent.filingmanager_city,
                most_recent.filingmanager_stateorcountry,
                most_recent.most_recent_date_filed,
                counts.filings_count
            FROM most_recent
            JOIN counts ON (most_recent.cik = counts.cik);

            CREATE INDEX IF NOT EXISTS idx_filers_on_lower_name ON filers (lower(filingmanager_name));
            CREATE UNIQUE INDEX IF NOT EXISTS idx_filers_on_cik ON filers (cik);
            CREATE INDEX IF NOT EXISTS idx_filers_on_name ON filers USING gin (filingmanager_name gin_trgm_ops);
        """)
        conn.commit()
        logger.info("filers materialized view and indices created.")

        # Create cusip_quarterly_filings_counts materialized view
        logger.info("Creating cusip_quarterly_filings_counts materialized view...")
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS cusip_quarterly_filings_counts AS
            SELECT 
                ah.cusip,
                ah.report_year,
                ah.report_quarter,
                COUNT(*) AS filings_count
            FROM aggregate_holdings ah
            GROUP BY ah.cusip, ah.report_year, ah.report_quarter
            ORDER BY ah.cusip, ah.report_year, ah.report_quarter;

            CREATE UNIQUE INDEX IF NOT EXISTS idx_cusip_quarterly_filings_unique 
            ON cusip_quarterly_filings_counts (cusip, report_year, report_quarter);
        """)
        conn.commit()
        logger.info(
            "cusip_quarterly_filings_counts materialized view and index created."
        )

        # Create company_cusip_lookups materialized view
        logger.info("Creating company_cusip_lookups materialized view...")
        cur.execute("""
            CREATE MATERIALIZED VIEW IF NOT EXISTS company_cusip_lookups AS 
            WITH holding_counts AS (
                SELECT 
                    aggregate_holdings.cusip,
                    aggregate_holdings.nameofissuer,
                    aggregate_holdings.titleofclass,
                    aggregate_holdings.sshprnam,
                    aggregate_holdings.sshprnamttype,
                    COUNT(*) AS holdings_count
                FROM aggregate_holdings
                GROUP BY aggregate_holdings.cusip, aggregate_holdings.nameofissuer, 
                         aggregate_holdings.titleofclass, aggregate_holdings.sshprnamttype
            ),
            most_common AS (
                SELECT DISTINCT ON (holding_counts.cusip) 
                    holding_counts.cusip,
                    holding_counts.nameofissuer,
                    holding_counts.titleofclass,
                    holding_counts.sshprnamttype,
                    holding_counts.holdings_count
                FROM holding_counts
                ORDER BY holding_counts.cusip, holding_counts.holdings_count DESC, 
                         holding_counts.nameofissuer, holding_counts.titleofclass
            )
            SELECT 
                mc.cusip,
                mc.nameofissuer,
                mc.titleofclass,
                mc.sshprnamttype,
                mc.holdings_count
            FROM most_common mc;

            CREATE INDEX IF NOT EXISTS idx_company_cusip_lookups_on_count_and_name 
            ON company_cusip_lookups (holdings_count, lower(nameofissuer));
            CREATE UNIQUE INDEX IF NOT EXISTS idx_company_cusip_lookups_on_cusip ON company_cusip_lookups (cusip);
            CREATE INDEX IF NOT EXISTS idx_company_cusip_lookups_on_issuer_name ON company_cusip_lookups USING gin (nameofissuer gin_trgm_ops);
        """)
        conn.commit()
        logger.info("company_cusip_lookups materialized view and indices created.")

    except psycopg2.Error as e:
        logger.error(f"Error creating materialized views: {str(e)}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()


def refresh_views():
    """Refresh all materialized views."""
    conn = None
    cur = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
        )
        cur = conn.cursor()

        logger.info("Refreshing materialized views...")
        cur.execute("""
            REFRESH MATERIALIZED VIEW aggregate_holdings;
            REFRESH MATERIALIZED VIEW filers;
            REFRESH MATERIALIZED VIEW cusip_quarterly_filings_counts;
            REFRESH MATERIALIZED VIEW company_cusip_lookups;
        """)
        conn.commit()
        logger.info("All materialized views refreshed.")

    except psycopg2.Error as e:
        logger.error(f"Error refreshing materialized views: {str(e)}")
        conn.rollback()
        raise
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
