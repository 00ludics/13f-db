# app.py
import psycopg2
import psycopg2.extras
from flask import Flask, jsonify, render_template, request

from config import DB_HOST, DB_NAME, DB_PASSWORD, DB_USER

app = Flask(__name__)


def get_db_connection():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST
    )
    return conn


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search")
def search():
    query = request.args.get("query", "")
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Search for managers
    cur.execute(
        """
        SELECT cik, filingmanager_name, filingmanager_city, filingmanager_stateorcountry, filings_count
        FROM filers
        WHERE filingmanager_name ILIKE %s
           OR filingmanager_name ILIKE %s
        ORDER BY 
            CASE 
                WHEN filingmanager_name ILIKE %s THEN 0
                WHEN filingmanager_name ILIKE %s THEN 1
                ELSE 2
            END,
            filings_count DESC
        LIMIT 5
    """,
        (f"{query}%", f"% {query}%", f"{query}%", f"% {query}%"),
    )
    managers = cur.fetchall()

    # Search for securities using security_cusip_lookups
    cur.execute(
        """
        SELECT cusip, nameofissuer, symbol, holdings_count
        FROM security_cusip_lookups
        WHERE (nameofissuer ILIKE %s
           OR nameofissuer ILIKE %s
           OR (symbol IS NOT NULL AND (symbol ILIKE %s OR symbol ILIKE %s)))
           AND holdings_count >= 5
        ORDER BY 
            CASE 
                WHEN nameofissuer ILIKE %s OR symbol ILIKE %s THEN 0
                WHEN nameofissuer ILIKE %s OR symbol ILIKE %s THEN 1
                ELSE 2
            END,
            holdings_count DESC
        LIMIT 5
    """,
        (
            f"{query}%",
            f"% {query}%",
            f"{query}%",
            f"% {query}%",
            f"{query}%",
            f"{query}%",
            f"% {query}%",
            f"% {query}%",
        ),
    )
    securities = cur.fetchall()

    cur.close()
    conn.close()

    results = [
        {
            "type": "manager",
            "name": m["filingmanager_name"],
            "id": m["cik"],
            "location": f"{m['filingmanager_city']}, {m['filingmanager_stateorcountry']}",
            "filings_count": m["filings_count"],
        }
        for m in managers
    ] + [
        {
            "type": "security",
            "name": s["nameofissuer"],
            "id": s["cusip"],
            "symbol": s["symbol"] if s["symbol"] else None,
            "holdings_count": s["holdings_count"],
        }
        for s in securities
    ]

    return jsonify(results)


@app.route("/manager/<cik>")
def manager(cik):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(
        """
        SELECT *
        FROM filers
        WHERE cik = %s
    """,
        (cik,),
    )
    manager_info = cur.fetchone()

    cur.execute(
        """
        SELECT 
            periodofreport, 
            filing_date, 
            tableentrytotal, 
            tablevaluetotal,
            report_year,
            report_quarter,
            CONCAT(report_year::text, '-Q', report_quarter::text) AS report_period,
            SUBMISSIONTYPE AS form_type,
            accession_number
        FROM filings
        WHERE cik = %s AND restated_by IS NULL
        ORDER BY report_year DESC, report_quarter DESC
    """,
        (cik,),
    )
    filings = cur.fetchall()

    # Fetch data for the chart
    cur.execute(
        """
        SELECT 
            CONCAT(report_year::text, '-Q', report_quarter::text) AS period,
            tablevaluetotal AS value
        FROM filings
        WHERE cik = %s AND restated_by IS NULL
        ORDER BY report_year, report_quarter
    """,
        (cik,),
    )
    chart_data = [dict(row) for row in cur.fetchall()]

    cur.close()
    conn.close()

    return render_template(
        "manager.html", manager=manager_info, filings=filings, chart_data=chart_data
    )


@app.route("/security/<cusip>")
def security(cusip):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(
        """
        SELECT scl.*, csm.exchange, csm.sector, csm.industry
        FROM security_cusip_lookups scl
        LEFT JOIN cusip_symbol_mapping csm ON scl.cusip = csm.cusip
        WHERE scl.cusip = %s
    """,
        (cusip,),
    )
    security_info = cur.fetchone()

    cur.execute(
        """
        SELECT report_year, report_quarter, filings_count
        FROM cusip_quarterly_filings_counts
        WHERE cusip = %s
        ORDER BY report_year DESC, report_quarter DESC
    """,
        (cusip,),
    )
    quarterly_counts = cur.fetchall()

    cur.close()
    conn.close()

    return render_template(
        "security.html", security=security_info, quarterly_counts=quarterly_counts
    )


@app.route("/filing/<cik>/<period_accession>")
def filing(cik, period_accession):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    # Split the period_accession into period and accession_number
    parts = period_accession.split("-")
    period = "-".join(parts[:3])  # This will be YYYY-MM-DD
    accession_number = "-".join(parts[3:])  # This will be the accession number

    cur.execute(
        """
        SELECT *
        FROM filings
        WHERE cik = %s AND periodofreport = %s AND accession_number = %s
    """,
        (cik, period, accession_number),
    )
    filing_info = cur.fetchone()

    if filing_info is None:
        return (
            f"Filing not found for CIK: {cik}, Period-Accession Number: {period_accession}",
            404,
        )

    cur.execute(
        """
        SELECT ah.*, scl.symbol,
               (ah.value / f.tablevaluetotal * 100) AS percentage
        FROM aggregate_holdings ah
        LEFT JOIN security_cusip_lookups scl ON ah.cusip = scl.cusip
        JOIN filings f ON ah.filing_id = f.id
        WHERE ah.filing_id = %s
        ORDER BY ah.value DESC
    """,
        (filing_info["id"],),
    )
    holdings = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("filing.html", filing=filing_info, holdings=holdings)


@app.route("/security_manager/<cusip>/<cik>")
def security_manager(cusip, cik):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(
        """
        WITH manager_info AS (
            SELECT * FROM filers WHERE cik = %s
        ),
        security_info AS (
            SELECT scl.*, csm.exchange, csm.sector, csm.industry
            FROM security_cusip_lookups scl
            LEFT JOIN cusip_symbol_mapping csm ON scl.cusip = csm.cusip
            WHERE scl.cusip = %s
        ),
        total_values AS (
            SELECT 
                f.report_year,
                f.report_quarter,
                SUM(f.tablevaluetotal) as total_value
            FROM filings f
            WHERE f.cik = %s AND f.restated_by IS NULL
            GROUP BY f.report_year, f.report_quarter
        ),
        holdings_data AS (
            SELECT 
                ah.report_year,
                ah.report_quarter,
                ah.value,
                ah.sshprnamt,
                f.periodofreport,
                f.accession_number,
                CONCAT(ah.report_year::text, '-Q', ah.report_quarter::text) AS report_period,
                (ah.value / tv.total_value * 100) AS percentage,
                ROW_NUMBER() OVER (PARTITION BY ah.report_year, ah.report_quarter ORDER BY ah.value DESC) as rank
            FROM aggregate_holdings ah
            JOIN filings f ON ah.filing_id = f.id
            JOIN total_values tv ON f.report_year = tv.report_year AND f.report_quarter = tv.report_quarter
            WHERE ah.cusip = %s AND f.cik = %s AND f.restated_by IS NULL
        )
        SELECT 
            json_build_object('manager', (SELECT row_to_json(manager_info) FROM manager_info)) AS manager_info,
            json_build_object('security', (SELECT row_to_json(security_info) FROM security_info)) AS security_info,
            json_agg(json_build_object(
                'report_period', hd.report_period,
                'value', hd.value,
                'sshprnamt', hd.sshprnamt,
                'periodofreport', hd.periodofreport::text,
                'percentage', hd.percentage,
                'accession_number', hd.accession_number
            ) ORDER BY hd.periodofreport DESC) AS holdings,
            json_agg(json_build_object(
                'period', hd.report_period,
                'value', hd.value
            ) ORDER BY hd.report_year, hd.report_quarter) AS chart_data
        FROM holdings_data hd
        WHERE hd.rank = 1
    """,
        (cik, cusip, cik, cusip, cik),
    )

    result = cur.fetchone()

    cur.close()
    conn.close()

    if result is None:
        return "Data not found", 404

    return render_template(
        "security_manager.html",
        manager=result["manager_info"]["manager"],
        security=result["security_info"]["security"],
        holdings=result["holdings"],
        chart_data=result["chart_data"],
    )


@app.route("/security_holders/<cusip>/<int:year>/<int:quarter>")
def security_holders(cusip, year, quarter):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        # Get security info from security_cusip_lookups
        cur.execute(
            """
            SELECT scl.*, csm.exchange, csm.sector, csm.industry
            FROM security_cusip_lookups scl
            LEFT JOIN cusip_symbol_mapping csm ON scl.cusip = csm.cusip
            WHERE scl.cusip = %s
        """,
            (cusip,),
        )
        security_info = cur.fetchone()

        if not security_info:
            return "Security not found", 404

        # Get total count of holders from cusip_quarterly_filings_counts
        cur.execute(
            """
            SELECT filings_count
            FROM cusip_quarterly_filings_counts
            WHERE cusip = %s AND report_year = %s AND report_quarter = %s
        """,
            (cusip, year, quarter),
        )
        count_result = cur.fetchone()
        total_holders = count_result["filings_count"] if count_result else 0

        # Get all holders using aggregate_holdings, filings, and filers
        cur.execute(
            """
            WITH total_holdings AS (
                SELECT SUM(value) as total_value
                FROM aggregate_holdings
                WHERE cusip = %s AND report_year = %s AND report_quarter = %s
            )
            SELECT 
                fil.cik,
                f.filingmanager_name,
                ah.value,
                ah.sshprnamt,
                (ah.value / th.total_value * 100) AS percentage
            FROM aggregate_holdings ah
            JOIN filings fil ON ah.filing_id = fil.id
            JOIN filers f ON fil.cik = f.cik
            CROSS JOIN total_holdings th
            WHERE ah.cusip = %s AND ah.report_year = %s AND ah.report_quarter = %s AND restated_by IS NULL
            ORDER BY ah.value DESC
        """,
            (cusip, year, quarter, cusip, year, quarter),
        )
        holders = cur.fetchall()

    except Exception as e:
        app.logger.error(f"Error fetching security holders: {str(e)}")
        return "An error occurred", 500
    finally:
        cur.close()
        conn.close()

    return render_template(
        "security_holders.html",
        security=security_info,
        year=year,
        quarter=quarter,
        holders=holders,
        total_holders=total_holders,
    )


if __name__ == "__main__":
    app.run(debug=True)
