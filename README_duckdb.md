 # DuckDB Backend — Integration Guide
### Plugs into your existing pivot_query_engine7.py

---

## What was added (3 new files)

| File | What it does |
|---|---|
| `etl_to_duckdb.py` | Reads your Excel master sheet → writes into `dark_db.duckdb` |
| `duckdb_backend.py` | `DuckDBBackend` class — drop-in replacement for `ExcelBackend` |
| `duckdb_backend.py` | Also includes `run_with_duckdb()` — the runner function |

**Your existing files are NOT modified:**
- `pivot_extractor_v2-A.py` — unchanged
- `pivot_query_engine7.py` — unchanged
- `pivot_viewer2.html` — unchanged

---

## How it fits into your existing flow

### Before (current)
```
client .xlsx
    ↓
pivot_extractor_v2-A.py  →  pivots.json
    ↓
pivot_query_engine7.py (ExcelBackend — loads full .xlsx into RAM)
    ↓
dashboard.html  (static, slicers don't re-query)
```

### After (with DuckDB)
```
client DB  (or .xlsx file)
    ↓
etl_to_duckdb.py  →  dark_db.duckdb  ← the "dark DB"
                           ↑
pivot_extractor_v2-A.py → pivots.json
    ↓
duckdb_backend.py  (DuckDBBackend — queries DuckDB on demand)
    ↓
dashboard.html  (same format, same HTML output — but data came from DuckDB)
```

---

## Step-by-step setup

### 1. Install dependencies

```bash
pip install duckdb pandas openpyxl numpy
```

### 2. Run your extractor as usual

Nothing changes here:

```bash
python pivot_extractor_v2-A.py "master.xlsx" "Sales Data Dump" --header-row 2 -o pivots.json -v
```

### 3. Load master data into DuckDB

**From Excel (your current source):**
```bash
python etl_to_duckdb.py \
    --xlsx "master.xlsx" \
    --sheet "Sales Data Dump" \
    --header-row 2 \
    --db dark_db.duckdb
```

**From client's live database (once you know the DB type):**
```bash
# SQL Server example
python etl_to_duckdb.py \
    --source-db mssql \
    --host CLIENT_SERVER \
    --port 1433 \
    --database CLIENT_DB \
    --user your_user \
    --password your_password \
    --query "SELECT * FROM dbo.SalesMaster" \
    --db dark_db.duckdb
```

Verify it loaded correctly:
```bash
python etl_to_duckdb.py --db dark_db.duckdb --info
```

### 4. Run pivots against DuckDB

```bash
# All pivots → combined HTML
python duckdb_backend.py \
    --json pivots.json \
    --db dark_db.duckdb \
    --combined \
    --output dashboard.html

# Single pivot
python duckdb_backend.py \
    --json pivots.json \
    --db dark_db.duckdb \
    --pivot-id PT_PRESALES_CHANNEL_1
```

Or call it from Python:
```python
from duckdb_backend import run_with_duckdb

run_with_duckdb(
    json_path="pivots.json",
    db_path="dark_db.duckdb",
    combined=True,
    output="dashboard.html",
)
```

---

## How the DuckDB backend works

### `etl_to_duckdb.py` — what it does

1. Reads the master Excel sheet (or live DB) into pandas
2. Strips whitespace from column names (keeps names identical to Excel so pivot field links still work)
3. Drops fully-empty rows
4. Writes the table to `dark_db.duckdb` as `master_data` (full replace)
5. Writes `_etl_meta` table with timestamp and row count

### `DuckDBBackend` — what it does

`load()`:
- Connects to DuckDB read-only
- Pulls a **5,000-row sample** into pandas (enough for dtype detection, filter inspection, and hidden-item logic)
- Returns the sample DataFrame tagged with `._duckdb_full = True`
- The full 150M rows stay in DuckDB

`groupby_agg()`:
- When called with the tagged sample, it **builds a SQL GROUP BY query** and runs it in DuckDB
- Translates pandas agg names → DuckDB SQL functions (`sum` → `SUM`, `mean` → `AVG`, etc.)
- Complex aggs that can't be pushed down (e.g. `np.prod`) fall back to pulling full data into pandas for just that column
- Returns only the small aggregated result to the engine

**Everything else** (page filters, hidden items, show_data_as, calculated fields, HTML rendering) runs exactly as before in `pivot_query_engine7.py` — untouched.

---

## For the 150M row client database

The key design for large tables:

1. **ETL streams in 200k-row chunks** — never loads 150M rows at once
2. **DuckDB stores a compressed snapshot** — DuckDB's columnar storage compresses well, a 150M row table of typical sales data usually compresses to 2–5 GB on disk
3. **GROUP BY runs in DuckDB** — DuckDB's vectorised query engine aggregates 150M rows very fast (typically seconds, not minutes)
4. **Only the aggregated result** (usually dozens to thousands of rows) comes back to pandas
5. **Schedule ETL** to refresh daily/weekly so the dark DB stays current

---

## Scheduling the ETL

### Linux/Mac (cron) — daily at 2 AM
```bash
0 2 * * * cd /path/to/project && python etl_to_duckdb.py \
    --xlsx master.xlsx \
    --sheet "Sales Data Dump" \
    --header-row 2 \
    --db dark_db.duckdb >> etl.log 2>&1
```

### Windows Task Scheduler
- Program: `python`
- Arguments: `etl_to_duckdb.py --xlsx master.xlsx --sheet "Sales Data Dump" --db dark_db.duckdb`
- Start in: `C:\path\to\project`

---

## File structure

```
your project folder/
├── pivot_extractor_v2-A.py      ← your existing file (unchanged)
├── pivot_query_engine7.py       ← your existing file (unchanged)
├── pivot_viewer2.html           ← your existing file (unchanged)
│
├── etl_to_duckdb.py             ← NEW: loads master data into DuckDB
├── duckdb_backend.py            ← NEW: DuckDBBackend + CLI runner
│
├── dark_db.duckdb               ← generated after first ETL run
├── etl.log                      ← generated after first ETL run
└── pivots.json                  ← generated by pivot_extractor_v2-A.py
```
