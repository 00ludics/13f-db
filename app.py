# app.py
import time

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
        ORDER BY filings_count DESC
        LIMIT 5
    """,
        (f"%{query}%",),
    )
    managers = cur.fetchall()

    # Search for securities using company_cusip_lookups
    cur.execute(
        """
        SELECT cusip, nameofissuer, holdings_count
        FROM company_cusip_lookups
        WHERE nameofissuer ILIKE %s
        ORDER BY holdings_count DESC
        LIMIT 5
    """,
        (f"%{query}%",),
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
            SUBMISSIONTYPE AS form_type
        FROM filings
        WHERE cik = %s AND restated_by IS NULL
        ORDER BY report_year DESC, report_quarter DESC
    """,
        (cik,),
    )
    filings = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("manager.html", manager=manager_info, filings=filings)


@app.route("/security/<cusip>")
def security(cusip):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(
        """
        SELECT *
        FROM company_cusip_lookups
        WHERE cusip = %s
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


@app.route("/filing/<cik>/<period>")
def filing(cik, period):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute(
        """
        SELECT *
        FROM filings
        WHERE cik = %s AND periodofreport = %s AND restated_by IS NULL
    """,
        (cik, period),
    )
    filing_info = cur.fetchone()

    if filing_info is None:
        return "Filing not found", 404

    cur.execute(
        """
        SELECT ah.*
        FROM aggregate_holdings ah
        WHERE ah.filing_id = %s
        ORDER BY ah.value DESC
    """,
        (filing_info["id"],),
    )
    holdings = cur.fetchall()

    cur.close()
    conn.close()

    return render_template("filing.html", filing=filing_info, holdings=holdings)


@app.route("/security_holders/<cusip>/<int:year>/<int:quarter>")
def security_holders(cusip, year, quarter):
    start_time = time.time()
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    try:
        # Get security info from company_cusip_lookups
        security_time = time.time()
        cur.execute(
            """
            SELECT *
            FROM company_cusip_lookups
            WHERE cusip = %s
        """,
            (cusip,),
        )
        security_info = cur.fetchone()
        app.logger.info(
            f"Security info query time: {time.time() - security_time:.2f} seconds"
        )

        if not security_info:
            return "Security not found", 404

        # Get total count of holders from cusip_quarterly_filings_counts
        count_time = time.time()
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
        app.logger.info(f"Count query time: {time.time() - count_time:.2f} seconds")

        # Get all holders using aggregate_holdings, filings, and filers
        holders_time = time.time()
        cur.execute(
            """
            SELECT DISTINCT ON (fil.cik)
                fil.cik,
                f.filingmanager_name,
                ah.value,
                ah.sshprnamt
            FROM aggregate_holdings ah
            JOIN filings fil ON ah.filing_id = fil.id
            JOIN filers f ON fil.cik = f.cik
            WHERE ah.cusip = %s AND ah.report_year = %s AND ah.report_quarter = %s
            ORDER BY fil.cik, ah.value DESC
        """,
            (cusip, year, quarter),
        )
        holders = cur.fetchall()
        app.logger.info(f"Holders query time: {time.time() - holders_time:.2f} seconds")

    except Exception as e:
        app.logger.error(f"Error fetching security holders: {str(e)}")
        return "An error occurred", 500
    finally:
        cur.close()
        conn.close()

    total_time = time.time() - start_time
    app.logger.info(f"Total function time: {total_time:.2f} seconds")

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
