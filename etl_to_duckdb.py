#!/usr/bin/env python3
"""
etl_to_duckdb.py
================
Reads the master data sheet from an Excel workbook (or a live database)
and loads it into DuckDB — the "dark DB" that pivot_query_engine7.py
reads from via DuckDBBackend.

This is the bridge between your current Excel-based system and the
DuckDB backend.

Two source modes
----------------
  MODE 1 — Excel (what you have now)
    Read the master sheet from the client's .xlsx directly.
    Use this when the client gives you a file dump.

  MODE 2 — Live database (what you want long-term)
    Connect to the client's actual DB, pull only what you need,
    stream in chunks so 150M rows don't blow RAM.

Usage
-----
  # From Excel (current workflow)
  python etl_to_duckdb.py \\
      --xlsx "Sales Dump Data to Pivots/master.xlsx" \\
      --sheet "Sales Data Dump" \\
      --header-row 2 \\
      --db dark_db.duckdb

  # From a live database (future workflow)
  python etl_to_duckdb.py \\
      --source-db mssql \\
      --host CLIENT_SERVER --port 1433 \\
      --database CLIENT_DB --user sa --password SECRET \\
      --query "SELECT * FROM dbo.SalesMaster WHERE Year >= 2024" \\
      --db dark_db.duckdb

  # Refresh only (re-run any time new data is available)
  python etl_to_duckdb.py --xlsx master.xlsx --sheet "Sales Data Dump" --db dark_db.duckdb

After running, verify:
  python etl_to_duckdb.py --db dark_db.duckdb --info
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Config defaults
# ---------------------------------------------------------------------------

DEFAULT_TABLE    = "master_data"
DEFAULT_DB       = "dark_db.duckdb"
DEFAULT_CHUNK    = 200_000      # rows per streaming chunk for live DB
LOG_FILE         = "etl.log"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source: Excel
# ---------------------------------------------------------------------------

def load_from_excel(
    xlsx_path: str,
    sheet_name: str,
    header_row: int = 1,
) -> pd.DataFrame:
    """
    Load the master sheet from an Excel file into a DataFrame.

    header_row is 1-based (same convention as pivot_extractor_v2-A.py).
    """
    path = Path(xlsx_path)
    if not path.exists():
        log.error(f"File not found: {xlsx_path}")
        sys.exit(1)

    log.info(f"Reading Excel: {xlsx_path!r}")
    log.info(f"  sheet={sheet_name!r}  header_row={header_row}")

    df = pd.read_excel(
        xlsx_path,
        sheet_name=sheet_name,
        header=header_row - 1,   # pandas is 0-based
        engine="openpyxl",
    )
    log.info(f"  Loaded {len(df):,} rows × {len(df.columns)} columns")
    return df


# ---------------------------------------------------------------------------
# Source: Live database (pluggable)
# ---------------------------------------------------------------------------

def get_live_connection(args):
    """
    Return a DB-API connection based on --source-db flag.
    Install the appropriate driver first:
      SQL Server  → pip install pyodbc
      PostgreSQL  → pip install psycopg2-binary
      MySQL       → pip install mysql-connector-python
      Oracle      → pip install cx_Oracle
    """
    db_type = args.source_db.lower()

    if db_type == "mssql":
        import pyodbc
        cs = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={args.host},{args.port};"
            f"DATABASE={args.database};"
            f"UID={args.user};PWD={args.password}"
        )
        return pyodbc.connect(cs)

    elif db_type == "postgres":
        import psycopg2
        return psycopg2.connect(
            host=args.host, port=args.port or 5432,
            dbname=args.database, user=args.user, password=args.password,
        )

    elif db_type == "mysql":
        import mysql.connector
        return mysql.connector.connect(
            host=args.host, port=args.port or 3306,
            database=args.database, user=args.user, password=args.password,
        )

    elif db_type == "oracle":
        import cx_Oracle
        dsn = cx_Oracle.makedsn(args.host, args.port or 1521, service_name=args.database)
        return cx_Oracle.connect(user=args.user, password=args.password, dsn=dsn)

    else:
        log.error(f"Unknown source DB type: {db_type}")
        sys.exit(1)


def load_from_live_db(args) -> pd.DataFrame:
    """
    Stream data from the client's live database in chunks to avoid
    blowing RAM on 150M row tables.
    """
    query = args.query
    if not query:
        log.error("--query is required when using --source-db")
        sys.exit(1)

    log.info(f"Connecting to {args.source_db.upper()} at {args.host}:{args.port}")
    conn   = get_live_connection(args)
    chunks = []
    total  = 0
    t0     = time.time()

    log.info("Streaming data in chunks…")
    try:
        for chunk in pd.read_sql(query, conn, chunksize=DEFAULT_CHUNK):
            chunks.append(chunk)
            total += len(chunk)
            log.info(f"  …{total:,} rows  ({time.time()-t0:.1f}s)")
    finally:
        conn.close()

    df = pd.concat(chunks, ignore_index=True)
    log.info(f"Streamed {len(df):,} rows × {len(df.columns)} columns in {time.time()-t0:.1f}s")
    return df


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Light normalisation — keeps column names exactly as they are in the
    Excel sheet so that pivot JSON field names still match.

    Only strips leading/trailing whitespace from column names (a common
    Excel artifact) and drops fully-empty rows.
    """
    # Strip whitespace from column names only — do NOT rename them
    # because pivot_query_engine7.py matches field names exactly.
    df.columns = [str(c).strip() for c in df.columns]

    before = len(df)
    df = df.dropna(how="all")
    dropped = before - len(df)
    if dropped:
        log.info(f"  Dropped {dropped:,} fully-empty rows")

    # Add ETL timestamp column (useful for debugging — does not affect pivots)
    df["_etl_loaded_at"] = datetime.now(timezone.utc).isoformat()

    log.info(f"  Transform complete: {len(df):,} rows, {len(df.columns)} columns")
    return df


# ---------------------------------------------------------------------------
# Load into DuckDB
# ---------------------------------------------------------------------------

def load_to_duckdb(
    df: pd.DataFrame,
    db_path: str,
    table: str = DEFAULT_TABLE,
) -> None:
    """
    Write the DataFrame to DuckDB, replacing any previous snapshot.
    """
    log.info(f"Loading into DuckDB: {db_path!r}  table={table!r}")
    t0  = time.time()
    con = duckdb.connect(db_path)
    try:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        # DuckDB can create a table directly from a pandas DataFrame in one call
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")

        row_count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log.info(f"  Loaded {row_count:,} rows in {time.time()-t0:.2f}s")

        # Write ETL metadata so the API / frontend can show freshness info
        con.execute("DROP TABLE IF EXISTS _etl_meta")
        con.execute("""
            CREATE TABLE _etl_meta (
                last_run     TIMESTAMP,
                row_count    BIGINT,
                table_name   VARCHAR,
                column_count INTEGER
            )
        """)
        con.execute(
            "INSERT INTO _etl_meta VALUES (?, ?, ?, ?)",
            [
                datetime.now(timezone.utc),
                row_count,
                table,
                len(df.columns),
            ],
        )
        log.info(f"  Metadata written to _etl_meta")

        # Useful summary
        log.info("  Columns loaded:")
        for col in df.columns:
            dtype = str(df[col].dtype)
            log.info(f"    {col:<40} {dtype}")

    finally:
        con.close()


# ---------------------------------------------------------------------------
# Info / verify
# ---------------------------------------------------------------------------

def print_info(db_path: str, table: str = DEFAULT_TABLE) -> None:
    """Print a quick summary of the DuckDB contents."""
    if not Path(db_path).exists():
        print(f"DB not found: {db_path}")
        return

    con = duckdb.connect(db_path, read_only=True)
    try:
        # ETL metadata
        try:
            meta = con.execute("SELECT * FROM _etl_meta ORDER BY last_run DESC LIMIT 1").fetchone()
            print(f"\n── ETL Metadata ──────────────────────────────")
            print(f"  Last run   : {meta[0]}")
            print(f"  Row count  : {meta[1]:,}")
            print(f"  Table      : {meta[2]}")
            print(f"  Columns    : {meta[3]}")
        except Exception:
            print("  (no _etl_meta table — run ETL first)")

        # Table summary
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            print(f"\n── Table: {table} ──────────────────────────────")
            print(f"  Rows   : {count:,}")
            cols = con.execute(f"DESCRIBE {table}").fetchdf()
            print(f"  Columns ({len(cols)}):")
            for _, row in cols.iterrows():
                print(f"    {row['column_name']:<40} {row['column_type']}")
        except Exception as e:
            print(f"  Error reading table: {e}")

        # List all tables
        tables = con.execute("SHOW TABLES").fetchdf()
        print(f"\n── All tables in {db_path} ──────────────────")
        for t in tables["name"].tolist():
            n = con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            print(f"  {t:<40} {n:,} rows")

    finally:
        con.close()


# ---------------------------------------------------------------------------
# Orchestrate
# ---------------------------------------------------------------------------

def run_etl(args) -> None:
    log.info("=" * 60)
    log.info("ETL started")
    t_start = time.time()

    # ── Source ────────────────────────────────────────────────────────────────
    if args.xlsx:
        df = load_from_excel(args.xlsx, args.sheet, args.header_row)
    elif args.source_db:
        df = load_from_live_db(args)
    else:
        log.error("Provide either --xlsx or --source-db")
        sys.exit(1)

    # ── Transform ─────────────────────────────────────────────────────────────
    df = transform(df)

    # ── Load ──────────────────────────────────────────────────────────────────
    load_to_duckdb(df, args.db, table=args.table)

    elapsed = time.time() - t_start
    log.info(f"ETL complete in {elapsed:.1f}s  →  {args.db}")
    log.info("=" * 60)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="ETL: load master data into DuckDB for pivot_query_engine7.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    ap.add_argument("--db",         default=DEFAULT_DB,
                    help=f"DuckDB output path [default: {DEFAULT_DB}]")
    ap.add_argument("--table",      default=DEFAULT_TABLE,
                    help=f"Table name inside DuckDB [default: {DEFAULT_TABLE}]")
    ap.add_argument("--info",       action="store_true",
                    help="Print DuckDB contents and exit (no ETL run)")

    # Excel source
    excel = ap.add_argument_group("Excel source (MODE 1)")
    excel.add_argument("--xlsx",       help="Path to .xlsx workbook")
    excel.add_argument("--sheet",      default=None,
                       help="Worksheet name (required with --xlsx)")
    excel.add_argument("--header-row", type=int, default=1,
                       help="1-based header row in worksheet [default: 1]")

    # Live DB source
    live = ap.add_argument_group("Live database source (MODE 2)")
    live.add_argument("--source-db",  choices=["mssql","postgres","mysql","oracle"],
                      help="Client database type")
    live.add_argument("--host",       default="localhost")
    live.add_argument("--port",       type=int, default=None)
    live.add_argument("--database",   default=None)
    live.add_argument("--user",       default=None)
    live.add_argument("--password",   default=None)
    live.add_argument("--query",      default=None,
                      help="SQL SELECT query to pull master data")

    args = ap.parse_args()

    if args.info:
        print_info(args.db, args.table)
        return

    # Validate
    if args.xlsx and not args.sheet:
        ap.error("--sheet is required when using --xlsx")

    run_etl(args)


if __name__ == "__main__":
    main()
