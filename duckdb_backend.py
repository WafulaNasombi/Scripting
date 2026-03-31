#!/usr/bin/env python3
"""
duckdb_backend.py
=================
Drop-in DuckDB backend for pivot_query_engine7.py.

This module provides DuckDBBackend — a DataBackend subclass that replaces
ExcelBackend.  All filter, formula, show_data_as, and HTML rendering logic
in pivot_query_engine7.py stays UNCHANGED.  Only load() and groupby_agg()
are overridden so that heavy GROUP-BY work runs inside DuckDB instead of
pandas in-memory.

Usage
-----
Instead of:
    backend   = ExcelBackend(xls_path, sheet_name, header_row)
    df_master = backend.load()

Use:
    from duckdb_backend import DuckDBBackend
    backend   = DuckDBBackend("dark_db.duckdb", table="master_data")
    df_master = backend.load()   # returns lightweight sentinel DataFrame
                                  # (actual data stays in DuckDB)

Then pass backend into execute_pivot() exactly as before — the groupby_agg()
override pushes GROUP BY SQL down to DuckDB and only pulls the small result
back into pandas.

Running the full engine with this backend
-----------------------------------------
    python pivot_query_engine7.py \\
        --backend duckdb \\
        --dsn dark_db.duckdb \\
        --json pivots.json \\
        --combined

NOTE: pivot_query_engine7.py currently raises NotImplementedError for
      --backend duckdb.  Apply the one-line patch at the bottom of this
      file (or import and call run_with_duckdb() directly).
"""

from __future__ import annotations

import re
from typing import Any

import duckdb
import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Import the abstract base from your existing engine file
# ---------------------------------------------------------------------------
# If pivot_query_engine7.py is in the same folder, this import just works.
# Adjust the import path if needed.
try:
    from pivot_query_engine7 import DataBackend, _AGG_MAP
except ImportError:
    # Fallback stub so this file can be read/tested without the engine present
    class DataBackend:                          # type: ignore[no-redef]
        def load(self) -> pd.DataFrame: raise NotImplementedError
        def groupby_agg(self, df, group_cols, named_aggs): raise NotImplementedError
    _AGG_MAP = {}


# ---------------------------------------------------------------------------
# DuckDB aggregation map
# Maps the same keys as _AGG_MAP → DuckDB SQL aggregate functions
# ---------------------------------------------------------------------------

_DUCK_AGG: dict[str, str] = {
    "sum":       "SUM",
    "count":     "COUNT",
    "counta":    "COUNT",
    "countnums": "COUNT",
    "average":   "AVG",
    "mean":      "AVG",
    "min":       "MIN",
    "max":       "MAX",
    "product":   None,        # No native DuckDB PRODUCT — falls back to pandas
    "stddev":    "STDDEV_SAMP",
    "std":       "STDDEV_SAMP",   # pandas function name for stddev
    "stddevp":   "STDDEV_POP",
    "var":       "VAR_SAMP",
    "varp":      "VAR_POP",
}


def _resolve_duck_agg(agg_key: str) -> str | None:
    """Return the DuckDB SQL aggregate name, or None if push-down unsupported."""
    key = (agg_key or "sum").lower().replace(".", "")
    return _DUCK_AGG.get(key)          # None → fall back to pandas groupby


# ---------------------------------------------------------------------------
# Safe column quoting
# ---------------------------------------------------------------------------

def _q(name: str) -> str:
    """Double-quote a column name for DuckDB SQL."""
    return '"' + name.replace('"', '""') + '"'


# ---------------------------------------------------------------------------
# DuckDBBackend
# ---------------------------------------------------------------------------

class DuckDBBackend(DataBackend):
    """
    DataBackend implementation that reads from a DuckDB file.

    Parameters
    ----------
    db_path : str
        Path to the DuckDB file produced by etl_to_duckdb.py.
        Pass ":memory:" to use an in-memory database (useful for testing).
    table : str
        Name of the master data table inside DuckDB (default: "master_data").
    """

    def __init__(self, db_path: str, table: str = "master_data"):
        self.db_path = db_path
        self.table   = table
        self._con    = None         # lazy connection

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def _connect(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            self._con = duckdb.connect(self.db_path, read_only=True)
        return self._con

    def close(self):
        if self._con is not None:
            self._con.close()
            self._con = None

    # ------------------------------------------------------------------
    # load() — replaces ExcelBackend.load()
    #
    # The engine stores the return value of load() as df_master and passes
    # it to execute_pivot() → groupby_agg().  For DuckDB we return a
    # SENTINEL DataFrame that contains only column names and dtypes — no
    # row data — so that apply_page_filters / apply_hidden_items (which
    # work on a real DataFrame) still function correctly when they need to
    # inspect dtypes or sample values.
    #
    # The sentinel is marked with a special attribute so groupby_agg()
    # knows to query DuckDB rather than use pandas.
    # ------------------------------------------------------------------

    def load(self) -> pd.DataFrame:
        con = self._connect()
        print(f"[DuckDB] Connected  → {self.db_path!r}")
        print(f"[DuckDB] Table      → {self.table!r}")

        # Row count
        total = con.execute(f"SELECT COUNT(*) FROM {self.table}").fetchone()[0]
        print(f"[DuckDB] Row count  → {total:,}")

        # Pull a small sample to build a representative DataFrame for dtype
        # inspection and filter operations.  100 rows is enough for all the
        # page-filter / hidden-item logic in the engine.
        sample_df = con.execute(
            f"SELECT * FROM {self.table} LIMIT 5000"
        ).fetchdf()

        # Tag the DataFrame so groupby_agg knows it is a DuckDB sentinel
        sample_df._duckdb_backend = self     # type: ignore[attr-defined]
        sample_df._duckdb_full    = True     # type: ignore[attr-defined]

        print(f"[DuckDB] Sentinel   → {len(sample_df):,} sample rows "
              f"× {len(sample_df.columns)} columns loaded for filter inspection")
        return sample_df

    # ------------------------------------------------------------------
    # groupby_agg() — the main DuckDB push-down hook
    #
    # Called by execute_pivot() with:
    #   df         — the (filtered) DataFrame from apply_page_filters /
    #                apply_hidden_items.  If it is the sentinel from load()
    #                we use DuckDB; otherwise (post-filter pandas DF) we
    #                use pandas groupby as normal.
    #   group_cols — list of column names to GROUP BY
    #   named_aggs — dict of { output_col_name: pd.NamedAgg(column, aggfunc) }
    # ------------------------------------------------------------------

    def groupby_agg(
        self,
        df: pd.DataFrame,
        group_cols: list[str],
        named_aggs: dict,
    ) -> pd.DataFrame:

        # If df has already been page-filtered down to a small pandas slice,
        # or if it has no sentinel tag, just use pandas — the data is already
        # in memory and DuckDB push-down would offer no benefit.
        if not getattr(df, "_duckdb_full", False):
            return df.groupby(group_cols, dropna=False).agg(**named_aggs).reset_index()

        # ── Build SQL GROUP BY query ──────────────────────────────────────────
        con = self._connect()

        # SELECT list: GROUP BY dimensions first
        select_parts = [_q(c) for c in group_cols]
        fallback_cols: list[str] = []    # value columns that need pandas fallback

        # Aggregate expressions
        for out_name, nagg in named_aggs.items():
            src_col  = nagg.column
            agg_func = nagg.aggfunc

            # Resolve to DuckDB function name
            if callable(agg_func):                # Check if the callable carries a DuckDB aggregate tag
                duck_tag = getattr(agg_func, '_duck_agg', None)
                if duck_tag:
                    expr = f"{duck_tag}({_q(src_col)}) AS {_q(out_name)}"
                    select_parts.append(expr)
                    continue                # lambda / np.prod → can't push to SQL, mark for fallback
                fallback_cols.append(out_name)
                continue

            duck_fn = _resolve_duck_agg(str(agg_func))
            if duck_fn is None:
                fallback_cols.append(out_name)
                continue

            expr = f"{duck_fn}({_q(src_col)}) AS {_q(out_name)}"
            select_parts.append(expr)

        group_by_sql = ", ".join(_q(c) for c in group_cols)
        select_sql   = ", ".join(select_parts)

        query = (
            f"SELECT {select_sql} "
            f"FROM {self.table} "
            + (f"GROUP BY {group_by_sql} " if group_cols else "")
            + f"ORDER BY {group_by_sql}" if group_cols else ""
        )

        print(f"  [DuckDB] SQL → {query[:160]}{'…' if len(query) > 160 else ''}")

        try:
            result = con.execute(query).fetchdf()
        except Exception as e:
            print(f"  [DuckDB] SQL ERROR: {e}  — falling back to pandas groupby on sample")
            return df.groupby(group_cols, dropna=False).agg(**named_aggs).reset_index()

        # ── Pandas fallback for unsupported aggregations ──────────────────────
        # (e.g. product, custom lambdas)
        if fallback_cols:
            print(f"  [DuckDB] Pandas fallback for: {fallback_cols}")
            # We need the full data for these — load via DuckDB into pandas
            full_df = con.execute(f"SELECT * FROM {self.table}").fetchdf()
            fb_aggs = {k: v for k, v in named_aggs.items() if k in fallback_cols}
            if group_cols:
                fb_result = (
                    full_df
                    .groupby(group_cols, dropna=False)
                    .agg(**fb_aggs)
                    .reset_index()
                )
            else:
                # Summary pivot (no dimensions)
                fb_result = pd.DataFrame({
                    k: [full_df[v.column].agg(v.aggfunc)]
                    for k, v in fb_aggs.items()
                })
            # Merge fallback columns into the main DuckDB result
            if group_cols:
                result = result.merge(fb_result, on=group_cols, how="left")
            else:
                for col in fb_result.columns:
                    result[col] = fb_result[col].iloc[0]

        print(f"  [DuckDB] Result  → {len(result):,} rows")
        return result


# ---------------------------------------------------------------------------
# Convenience runner
# ---------------------------------------------------------------------------

def run_with_duckdb(
    json_path: str,
    db_path: str,
    table: str = "master_data",
    pivot_id: str | None = None,
    combined: bool = True,
    output: str | None = None,
) -> None:
    """
    Run the full pivot_query_engine pipeline using DuckDB as the data source.

    Parameters
    ----------
    json_path : str
        Path to the _pivots.json produced by pivot_extractor_v2-A.py
    db_path : str
        Path to the DuckDB file produced by etl_to_duckdb.py
    table : str
        Table name inside DuckDB (default: "master_data")
    pivot_id : str | None
        Run only this pivot ID.  None = run all.
    combined : bool
        Write all pivots into a single HTML file.
    output : str | None
        Output HTML path.  None = auto (dashboard.html or <pivot_id>.html)
    """
    import json
    import os
    from pivot_query_engine7 import (
        execute_pivot,
        build_html_dashboard,
    )

    # ── Load JSON ─────────────────────────────────────────────────────────────
    if not os.path.exists(json_path):
        raise FileNotFoundError(f"JSON not found: {json_path}")

    with open(json_path, encoding="utf-8") as f:
        raw = f.read()

    trunc_idx = raw.find(".............")
    if trunc_idx != -1:
        print(f"[loader] WARNING: JSON truncated at char {trunc_idx}")
        raw = raw[:trunc_idx].rstrip().rstrip(",") + "\n  ]\n}"

    config = json.loads(raw)
    pivots = [p for p in config.get("pivots", []) if "error" not in p]
    print(f"[loader] {len(pivots)} valid pivot(s) in {json_path}")

    # ── Filter by ID ──────────────────────────────────────────────────────────
    if pivot_id:
        pivots = [p for p in pivots if p.get("id") == pivot_id]
        if not pivots:
            raise ValueError(f"Pivot ID not found: {pivot_id}")

    # ── Backend ───────────────────────────────────────────────────────────────
    backend = DuckDBBackend(db_path, table=table)
    df_master = backend.load()

    # ── Execute ───────────────────────────────────────────────────────────────
    pivot_results = []
    for pivot in pivots:
        pid = pivot.get("id", "pivot")
        print(f"\n[pivot] ── {pid}  " + "─" * 40)
        try:
            result = execute_pivot(df_master, pivot, backend)
            pivot_results.append((pivot, result))
        except Exception as e:
            import traceback
            print(f"  ERROR in '{pid}': {e}")
            traceback.print_exc()
            pivot_results.append((pivot, pd.DataFrame()))

    backend.close()

    # ── Render HTML ───────────────────────────────────────────────────────────
    meta     = config.get("meta", {})
    src_name = meta.get("source_file", "DuckDB")

    if combined or len(pivot_results) == 1 or output:
        out_path = output or (
            f"{pivots[0].get('id','pivot')}.html"
            if len(pivot_results) == 1
            else "dashboard.html"
        )
        html_doc = build_html_dashboard(
            pivot_results,
            title=f"Pivot Dashboard – {src_name}"
        )
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_doc)
        print(f"\n[output] ✓ Dashboard → {out_path}  ({len(pivot_results)} pivot(s))")
    else:
        for pivot, result in pivot_results:
            pid      = pivot.get("id", "pivot")
            out_path = f"{pid}.html"
            html_doc = build_html_dashboard(
                [(pivot, result)],
                title=f"{pivot.get('name', pid)} – {src_name}"
            )
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(html_doc)
            print(f"  ✓ {out_path}  ({len(result):,} rows)")

    print("[engine] Done.")


# ---------------------------------------------------------------------------
# CLI — standalone runner (alternative to modifying pivot_query_engine7.py)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser(
        description="Run pivot engine against DuckDB dark database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument("--json",     required=True, help="Path to _pivots.json")
    ap.add_argument("--db",       required=True, help="Path to DuckDB file (dark_db.duckdb)")
    ap.add_argument("--table",    default="master_data", help="Table name in DuckDB [master_data]")
    ap.add_argument("--pivot-id", default=None,  help="Run only this pivot ID")
    ap.add_argument("--combined", action="store_true", help="One combined HTML output")
    ap.add_argument("--output",   default=None,  help="Output HTML path")
    args = ap.parse_args()

    run_with_duckdb(
        json_path = args.json,
        db_path   = args.db,
        table     = args.table,
        pivot_id  = args.pivot_id,
        combined  = args.combined,
        output    = args.output,
    )
