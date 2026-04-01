#!/usr/bin/env python3
"""
pivot_query_engine.py  v2.0
============================
Executes pivot-table definitions (from a pivot-extractor JSON) against an
Excel master worksheet and produces a SELF-CONTAINED HTML pivot dashboard per
pivot (or one combined dashboard), with full collapse/expand, freeze panes,
sorting, and number formatting.

Architecture
------------
  DataBackend (abstract)
    └── ExcelBackend   ← Step 1 (current):  pandas + openpyxl
    └── DuckDBBackend  ← Step 2 (future):   plug-in replacement, no engine changes

Usage
-----
  # List available pivot IDs
  python pivot_query_engine.py data.xlsx --json pivots.json --list-pivots

  # Run a single pivot → single HTML file
  python pivot_query_engine.py data.xlsx --json pivots.json --pivot-id PT_PRESALES_CHANNEL_1

  # Run all pivots → one HTML per pivot  (or --combined for one big HTML)
  python pivot_query_engine.py data.xlsx --json pivots.json

  # Combined single-file dashboard (all pivots in one HTML with tab nav)
  python pivot_query_engine.py data.xlsx --json pivots.json --combined

  # DuckDB (Step 2 – future)
  python pivot_query_engine.py --backend duckdb --dsn "duckdb:///sales.db" \\
         --json pivots.json --pivot-id PT_PRESALES_CHANNEL_1
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sys
from pathlib import Path
from collections import defaultdict
from typing import Any

import numpy as np
import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# BACKEND ABSTRACTION
# ─────────────────────────────────────────────────────────────────────────────

class DataBackend:
    """Abstract base – all pivot logic uses this interface."""

    def load(self) -> pd.DataFrame:
        raise NotImplementedError

    # Step 2 hook: override this to push GROUP-BY down to the DB engine.
    # For ExcelBackend the default (pandas groupby) is used.
    def groupby_agg(self, df: pd.DataFrame, group_cols: list[str],
                    named_aggs: dict) -> pd.DataFrame:
        return df.groupby(group_cols, dropna=False).agg(**named_aggs).reset_index()


class ExcelBackend(DataBackend):
    """
    Loads master sheet into a DataFrame once; all pivots run against it.
    DuckDB replacement: keep __init__ signature, override load() + groupby_agg().
    """

    def __init__(self, xls_path: str, sheet_name: str, header_row: int):
        self.xls_path   = xls_path
        self.sheet_name = sheet_name
        # JSON header_row is 1-based; pandas header= is 0-based
        self.header_idx = header_row - 1

    def load(self) -> pd.DataFrame:
        print(f"[backend] Loading  {self.xls_path!r}")
        print(f"          sheet={self.sheet_name!r}  header_row={self.header_idx + 1}")
        df = pd.read_excel(
            self.xls_path,
            sheet_name=self.sheet_name,
            header=self.header_idx,
            engine="openpyxl",
        )
        print(f"[backend] Loaded   {len(df):,} rows × {len(df.columns)} columns")
        return df


# ─────────────────────────────────────────────────────────────────────────────
# AGGREGATION MAP
# All aggregation keys from the JSON schema → pandas / numpy names
# ─────────────────────────────────────────────────────────────────────────────

def _stddevp(x):
    """Population standard deviation (ddof=0)."""
    return x.std(ddof=0)
_stddevp._duck_agg = "STDDEV_POP"   # type: ignore[attr-defined]

def _varp(x):
    """Population variance (ddof=0)."""
    return x.var(ddof=0)
_varp._duck_agg = "VAR_POP"         # type: ignore[attr-defined]

_AGG_MAP: dict[str, str | callable] = {
    "sum":       "sum",
    "count":     "count",   # COUNT non-blank
    "counta":    "count",
    "countnums": "count",   # COUNT numbers only — close enough for pandas
    "average":   "mean",
    "min":       "min",
    "max":       "max",
    "product":   np.prod,
    "stddev":    "std",
    "stddevp":   _stddevp,
    "var":       "var",
    "varp":      _varp,
}


def _resolve_agg(agg_key: str):
    key = (agg_key or "sum").lower().replace(".", "")
    return _AGG_MAP.get(key, "sum")


# ─────────────────────────────────────────────────────────────────────────────
# DATE UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

_EXCEL_EPOCH = pd.Timestamp("1899-12-30")


def _to_timestamp(val: Any) -> pd.Timestamp | None:
    """Try to convert an ISO string or Excel serial number to Timestamp."""
    if val is None:
        return None
    try:
        return pd.Timestamp(val)
    except Exception:
        pass
    try:
        return _EXCEL_EPOCH + pd.Timedelta(days=float(val))
    except Exception:
        return None


def _coerce_numeric(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


# ─────────────────────────────────────────────────────────────────────────────
# PAGE FILTER APPLICATION
# ─────────────────────────────────────────────────────────────────────────────

def _is_date_field(filt: dict, pivot: dict) -> bool:
    """Return True if the pivot cache metadata says this field is a date."""
    fname = filt.get("field", "")
    fmeta = pivot.get("fields", {}).get(fname, {})
    return fmeta.get("data_type") == "date"


def _selected_is_zero_sentinel(selected: Any, col: pd.Series) -> bool:
    """
    Detect when selected_item=0 (or 0.0) was stored as an index sentinel
    rather than a real column value.  Treat as show_all when:
      - selected is numeric 0 / 0.0
      - the column has no rows equal to 0 / 0.0
      - OR the column dtype is text/object (meaning 0 is almost certainly wrong)
    """
    try:
        if float(selected) != 0.0:
            return False
    except (TypeError, ValueError):
        return False
    if pd.api.types.is_object_dtype(col) or pd.api.types.is_string_dtype(col):
        return True
    col_num = pd.to_numeric(col, errors="coerce")
    return not (col_num == 0.0).any()


def apply_page_filters(df: pd.DataFrame, pivot: dict) -> pd.DataFrame:
    """
    Apply axisPage filters (pivot['filters']).
    show_all=True  → no restriction.
    show_all=False → restrict to selected_item value.

    Filter resolution order (stops at first successful match):
      1. Column is datetime64  → Timestamp compare (date part)
      2. Field metadata says date AND column is numeric (Excel serial ints)
         → convert selected ISO string to Excel serial, compare as int
      3. Column is numeric, selected parses as number → numeric ==
      4. String fallback: astype(str).strip() ==

    Special cases:
      - selected_item=0/0.0 on a text column → treated as show_all (sentinel bug
        in some pivot extractors that store selected_index instead of selected_value)
      - field not in DataFrame → skipped with warning
    """
    for filt in pivot.get("filters", []):
        if filt.get("show_all", True):
            continue
        field    = filt["field"]
        selected = filt.get("selected_item")
        if selected is None:
            continue
        if field not in df.columns:
            print(f"  [filter] SKIP  '{field}' – column not in data")
            continue

        col    = df[field]
        before = len(df)

        # ── Sentinel: selected=0 on a text column means "All" ────────────────
        if _selected_is_zero_sentinel(selected, col):
            print(f"  [filter] SKIP  '{field}' = {selected!r} – treated as (All) sentinel")
            continue

        # ── Case 1: column already datetime64 ────────────────────────────────
        if pd.api.types.is_datetime64_any_dtype(col):
            ts = _to_timestamp(selected)
            if ts is None:
                print(f"  [filter] WARN  '{field}' – cannot parse {selected!r}")
                continue
            # Strip timezone: tz-aware vs tz-naive comparison always returns False
            col_naive = col.dt.tz_convert(None) if (hasattr(col.dt, "tz") and col.dt.tz is not None) else col
            if col_naive is not col:
                print(f"  [filter] DEBUG  '{field}' stripped tz={col.dt.tz}")
            # Verbose debug: show sample values so mismatches are obvious
            sample = [str(v)[:10] for v in col_naive.dropna().unique()[:4]]
            print(f"  [filter] DEBUG  '{field}' dtype={col_naive.dtype} sample={sample} target={ts.date()}")
            mask = col_naive.dt.normalize() == ts.normalize()
            if not mask.any():
                # Fallback: month-level match (daily data filtered by month marker)
                month_mask = (col_naive.dt.year == ts.year) & (col_naive.dt.month == ts.month)
                if month_mask.any():
                    df = df[month_mask]
                    print(f"  [filter] '{field}' month={ts.strftime('%Y-%m')} (month-fallback) kept {len(df):,}/{before:,}")
                    continue
                print(f"  [filter] WARN  '{field}' = {ts.date()} NO MATCH  min={col_naive.min()} max={col_naive.max()}")
            else:
                df = df[mask]
                print(f"  [filter] '{field}' = {ts.date()}  (datetime64) kept {len(df):,}/{before:,}")
            continue

        # ── Case 2: field is typed as date, column is numeric (Excel serials) ─
        if _is_date_field(filt, pivot):
            ts = _to_timestamp(selected)
            if ts is not None:
                col_num = pd.to_numeric(col, errors="coerce")
                if col_num.notna().any():
                    serial = int((ts - _EXCEL_EPOCH).days)
                    mask   = col_num == serial
                    if mask.any():
                        df = df[mask]
                        print(f"  [filter] '{field}' = {ts.date()}  (serial={serial})  kept {len(df):,}/{before:,}")
                        continue
                    # Serial didn't match – try month-level matching
                    # (column might store first-of-month serial or daily serials)
                    month_start = serial
                    # last day of that month
                    next_month = ts + pd.offsets.MonthEnd(0) + pd.Timedelta(days=1)
                    month_end  = int((next_month - _EXCEL_EPOCH).days)
                    mask = (col_num >= month_start) & (col_num < month_end)
                    if mask.any():
                        df = df[mask]
                        print(f"  [filter] '{field}' month={ts.strftime('%Y-%m')} (serial range {month_start}–{month_end-1})  kept {len(df):,}/{before:,}")
                        continue

        # ── Case 3: numeric column, numeric selected value ────────────────────
        num_val = pd.to_numeric(selected, errors="coerce")
        if not pd.isna(num_val):
            col_num = pd.to_numeric(col, errors="coerce")
            if col_num.notna().any():
                mask = col_num == num_val
                if mask.any():
                    df = df[mask]
                    print(f"  [filter] '{field}' = {num_val}  (numeric)  kept {len(df):,}/{before:,}")
                    continue

        # ── Case 4: string fallback ───────────────────────────────────────────
        sel_str  = str(selected).strip()
        col_strs = col.astype(str).str.strip()
        mask     = col_strs == sel_str
        if mask.any():
            df = df[mask]
            print(f"  [filter] '{field}' = {sel_str!r}  (string)  kept {len(df):,}/{before:,}")
        else:
            print(f"  [filter] WARN  '{field}' = {sel_str!r} – no matching rows (0/{before:,})")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# HIDDEN ITEM EXCLUSIONS
# ─────────────────────────────────────────────────────────────────────────────

def apply_hidden_items(df: pd.DataFrame, pivot: dict) -> pd.DataFrame:
    """
    Exclude rows whose dimension field value appears in hidden_items[].
    Hidden items are raw cache values (strings, numbers, blanks).
    Matching is done with type coercion to catch both "1.0" and 1.
    """
    axis_fields = (
        [r["field"] for r in pivot.get("rows", [])    if r.get("type") == "field"] +
        [c["field"] for c in pivot.get("columns", []) if c.get("type") == "field"]
    )
    for fname in axis_fields:
        fmeta  = pivot.get("fields", {}).get(fname, {})
        hidden = fmeta.get("hidden_items", [])
        if not hidden or fname not in df.columns:
            continue

        col    = df[fname]
        # Build a set of string representations for comparison
        hidden_str = {str(h).strip() for h in hidden}
        # Also keep numeric versions where possible
        hidden_num = set()
        for h in hidden:
            try:
                hidden_num.add(float(h))
            except (ValueError, TypeError):
                pass

        col_str = col.astype(str).str.strip()
        mask    = col_str.isin(hidden_str)

        # Also catch numeric matches
        if hidden_num:
            col_num = pd.to_numeric(col, errors="coerce")
            mask   |= col_num.isin(hidden_num)

        before = len(df)
        df = df[~mask]
        excluded = before - len(df)
        if excluded:
            print(f"  [hidden] '{fname}'  excluded {excluded:,} rows ({len(hidden)} hidden values)")

    return df


# ─────────────────────────────────────────────────────────────────────────────
# PIVOT FILTER APPLICATION (in-pivot label / value / top10 filters)
# ─────────────────────────────────────────────────────────────────────────────

def apply_pivot_filters(df: pd.DataFrame, result: pd.DataFrame,
                        pivot: dict) -> pd.DataFrame:
    """
    Apply pivot_filters[] — these operate on the RESULT (post-aggregation)
    for value/top10 filters, or on the raw df for label filters.
    Currently implements: valueList, top10. Others logged as warnings.
    """
    for pf in pivot.get("pivot_filters", []):
        field = pf.get("field")
        for cond in pf.get("criteria", {}).get("conditions", []):
            kind = cond.get("kind")
            if kind == "top10":
                top     = cond.get("top", True)
                pct     = cond.get("percent", False)
                n       = int(float(cond.get("val", 10)))
                # Find the first numeric value column to rank by
                val_cols = [c for c in result.columns if c not in
                            [r["field"] for r in pivot.get("rows", []) if r.get("type") == "field"]]
                if not val_cols:
                    continue
                rank_col = val_cols[0]
                col_num  = pd.to_numeric(result[rank_col], errors="coerce")
                if pct:
                    threshold = col_num.quantile(1 - n / 100 if top else n / 100)
                    mask = col_num >= threshold if top else col_num <= threshold
                else:
                    if top:
                        mask = col_num >= col_num.nlargest(n).min()
                    else:
                        mask = col_num <= col_num.nsmallest(n).max()
                result = result[mask]
            elif kind == "valueList":
                values = cond.get("values", [])
                if field in result.columns and values:
                    result = result[result[field].astype(str).isin([str(v) for v in values])]
            else:
                print(f"  [pf] pivot_filter kind={kind!r} not implemented – skipping")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# CALCULATED FIELD EVALUATION
# ─────────────────────────────────────────────────────────────────────────────

_SAFE_BUILTINS: dict = {"__builtins__": {}}


def _eval_formula(formula: str, row: pd.Series) -> float:
    """
    Evaluate a simple Excel calculated-field formula against a post-agg row.

    Supported:
      - Bracket syntax:  [Field Name]
      - Quoted syntax:   'Field Name'
      - Bare identifiers: fieldName
      - Operators:  + - * /
      - Basic Excel functions: IF, IFERROR, ABS, ROUND, INT, SQRT
      - Division by zero → NaN
    """
    # Strip leading '='
    expr = formula.strip().lstrip("=").strip()

    token_map: dict[str, float] = {}
    ctr = [0]

    def _tok(field: str) -> str:
        t = f"_t{ctr[0]}_"
        ctr[0] += 1
        raw = row.get(field)
        if raw is None:
            raw = row.get(field.strip())
        val = pd.to_numeric(raw, errors="coerce")
        token_map[t] = 0.0 if pd.isna(val) else float(val)
        return t

    # [Field Name] syntax
    expr = re.sub(r"\[([^\]]+)\]", lambda m: _tok(m.group(1)), expr)
    # 'Field Name' syntax (single-quoted multi-word)
    expr = re.sub(r"'([^']+)'",    lambda m: _tok(m.group(1)), expr)
    # Bare identifiers (only if not already a token)
    expr = re.sub(r"\b([A-Za-z_][A-Za-z0-9_%\.]*)\b",
                  lambda m: _tok(m.group(1)) if not m.group(1).startswith("_t")
                                                and m.group(1) not in ("IF","IFERROR","ABS","ROUND","INT","SQRT")
                  else m.group(1), expr)

    # Substitute tokens
    for tok, val in token_map.items():
        expr = expr.replace(tok, repr(val))

    # Map common Excel functions → Python
    expr = (expr
            .replace("IFERROR(", "_iferror(")
            .replace("IF(",      "_if(")
            .replace("ABS(",     "abs(")
            .replace("ROUND(",   "round(")
            .replace("INT(",     "int(")
            .replace("SQRT(",    "pow(")
            )

    def _if(cond, t, f):    return t if cond else f
    def _iferror(v, alt):
        try: return float(v)
        except Exception: return float(alt)

    safe_env = {
        "__builtins__": {},
        "_if": _if, "_iferror": _iferror,
        "abs": abs, "round": round, "int": int, "pow": pow,
    }

    try:
        return float(eval(expr, safe_env))   # noqa: S307
    except ZeroDivisionError:
        return float("nan")
    except Exception:
        return float("nan")


# ─────────────────────────────────────────────────────────────────────────────
# SHOW DATA AS TRANSFORMS  (post-aggregation, per value field)
# ─────────────────────────────────────────────────────────────────────────────

def apply_show_data_as(result: pd.DataFrame, vs: dict, col_name: str,
                       grand_total: float | None) -> pd.Series:
    """
    Transform a raw aggregated column according to show_data_as rules.
    Currently implements the most common transforms; others fall through as-is.
    """
    sda        = (vs.get("show_data_as") or "normal").lower()
    base_field = vs.get("base_field")
    base_item  = vs.get("base_item")
    col        = pd.to_numeric(result[col_name], errors="coerce")

    if sda == "normal":
        return col

    if sda == "percentoftotal":
        total = grand_total if grand_total else col.sum()
        return col.divide(total).where(total != 0)

    if sda == "percentofrow":
        row_totals = col  # can't derive row total from aggregated flat data without full context
        return col.divide(col.sum()).where(col.sum() != 0)

    if sda == "runtotal":
        return col.cumsum()

    if sda in ("percent", "difference", "percentdiff") and base_field and base_item:
        # Find the base row in result
        if base_field in result.columns:
            base_mask = result[base_field].astype(str) == str(base_item)
            base_vals = result.loc[base_mask, col_name]
            if not base_vals.empty:
                base_val = pd.to_numeric(base_vals.iloc[0], errors="coerce")
                if sda == "percent":
                    return col.divide(base_val).where(base_val != 0)
                elif sda == "difference":
                    return col - base_val
                elif sda == "percentdiff":
                    return (col - base_val).divide(base_val).where(base_val != 0)

    if sda in ("rankascending", "rankdescending"):
        ascending = (sda == "rankascending")
        return col.rank(method="min", ascending=ascending, na_option="bottom")

    # index, percentOfParent, etc. — not implemented, return raw
    return col


# ─────────────────────────────────────────────────────────────────────────────
# MAIN PIVOT EXECUTION
# ─────────────────────────────────────────────────────────────────────────────

def execute_pivot(df_master: pd.DataFrame, pivot: dict,
                  backend: DataBackend) -> pd.DataFrame:
    """
    Execute a single pivot definition against the master DataFrame.
    Returns a flat DataFrame: row-dimension columns + all value columns.
    """
    pid = pivot.get("id", pivot.get("name", "?"))
    print(f"\n[pivot] ── {pid}  ─────────────────────────────────────")

    df = df_master.copy()

    # ── 1. Page filters ──────────────────────────────────────────────────────
    df = apply_page_filters(df, pivot)
    print(f"        rows after page-filters     : {len(df):,}")
    if len(df) == 0:
        print("        WARNING: page filter produced 0 rows – check field name & date format")

    # ── 2. Hidden item exclusions ────────────────────────────────────────────
    df = apply_hidden_items(df, pivot)
    print(f"        rows after hidden exclusions : {len(df):,}")

    # ── 3. Determine GROUP-BY dimensions ─────────────────────────────────────
    row_fields = [r["field"] for r in pivot.get("rows", [])
                  if r.get("type") == "field" and r.get("field") != "__VALUES__"]
    col_fields = [c["field"] for c in pivot.get("columns", [])
                  if c.get("type") == "field" and c.get("field") != "__VALUES__"]

    group_by    = [f for f in row_fields + col_fields if f in df.columns]
    missing_dim = [f for f in row_fields + col_fields if f not in df.columns]
    if missing_dim:
        print(f"        WARNING: dimension columns missing from data: {missing_dim}")

    # ── 4. Build aggregation specs ───────────────────────────────────────────
    value_specs    = pivot.get("values", [])
    fields_meta    = pivot.get("fields", {})
    calc_fields    = {cf["name"]: cf for cf in pivot.get("calculated_fields", [])}

    # non-calculated aggs
    agg_specs: dict[str, tuple[str, Any]] = {}  # display_name → (source_col, agg_fn)
    for vs in value_specs:
        if vs.get("is_calculated"):
            continue
        display = (vs.get("display_name") or "").strip()
        source  = vs.get("source_field", "")
        if source == "__VALUES__" or not display:
            continue
        if source not in df.columns:
            print(f"        WARNING: value source '{source}' not in data (display='{display}')")
            continue
        agg_fn = _resolve_agg(vs.get("aggregation", "sum"))
        agg_specs[display] = (source, agg_fn)

    # ── 5. Execute GROUP BY (or global agg) ──────────────────────────────────
    if not agg_specs:
        print(f"        WARNING: no aggregatable value columns found")
        return pd.DataFrame()

    # coerce numeric for all source columns
    src_set = {src for (src, _) in agg_specs.values()}
    df_agg  = df.copy()
    for col in src_set:
        df_agg[col] = _coerce_numeric(df_agg[col])

    if group_by:
        # Build pandas named aggs
        # Multiple displays can map to same source → keep unique safe keys
        named: dict[str, pd.NamedAgg] = {}
        safe_key_map: dict[str, str] = {}   # safe_key → display_name

        for display, (source, agg_fn) in agg_specs.items():
            base = re.sub(r"[^A-Za-z0-9]", "_", display)[:48]
            key  = base
            n    = 0
            while key in named:
                n += 1; key = f"{base}_{n}"
            named[key]         = pd.NamedAgg(column=source, aggfunc=agg_fn)
            safe_key_map[key]  = display

        result = backend.groupby_agg(df_agg, group_by, named)
        result.rename(columns=safe_key_map, inplace=True)
    else:
        # No dimensions – single summary row
        rec: dict[str, Any] = {}
        for display, (source, agg_fn) in agg_specs.items():
            s = df_agg[source]
            if callable(agg_fn):
                rec[display] = agg_fn(s)
            else:
                fn = getattr(s, agg_fn, None)
                if fn:
                    rec[display] = fn()
                else:
                    try:
                        rec[display] = s.agg(agg_fn)
                    except Exception:
                        print(f"        WARNING: unknown agg '{agg_fn}' for '{display}' – skipping")
                        rec[display] = float('nan')
        result = pd.DataFrame([rec])

    # ── 6. show_data_as transforms ───────────────────────────────────────────
    for vs in value_specs:
        if vs.get("is_calculated"):
            continue
        display = (vs.get("display_name") or "").strip()
        if display not in result.columns:
            continue
        sda = (vs.get("show_data_as") or "normal").lower()
        if sda != "normal":
            grand = pd.to_numeric(result[display], errors="coerce").sum()
            result[display] = apply_show_data_as(result, vs, display, grand)

    # ── 7. Calculated fields (post-aggregation, row-by-row formula eval) ─────
    for vs in value_specs:
        if not vs.get("is_calculated"):
            continue
        display    = (vs.get("display_name") or "").strip()
        src_field  = vs.get("source_field", "")

        # Find formula: check calculated_fields[] first, then fields[], then vs itself
        formula = None
        if src_field in calc_fields:
            formula = calc_fields[src_field].get("formula")
        if not formula:
            fmeta   = fields_meta.get(src_field, {})
            formula = fmeta.get("formula") or vs.get("formula")
        if not formula and display in calc_fields:
            formula = calc_fields[display].get("formula")
        if not formula:
            print(f"        WARNING: no formula for calculated field '{display}' – skipping")
            continue

        result[display] = result.apply(
            lambda row, f=formula: _eval_formula(f, row), axis=1
        )
        print(f"        CALC: '{display}' = {formula[:60]}")

    # ── 8. Grand totals ───────────────────────────────────────────────────────
    if pivot.get("row_grand_total") and group_by and len(result) > 0:
        grand_row: dict[str, Any] = {}
        for dim in group_by:
            grand_row[dim] = "Grand Total"
        for vs in value_specs:
            display = (vs.get("display_name") or "").strip()
            if display not in result.columns:
                continue
            if vs.get("is_calculated"):
                # Re-evaluate formula on the grand-total row (sum of component cols)
                grand_row[display] = float("nan")  # placeholder; recalculated below
            else:
                col_s = pd.to_numeric(result[display], errors="coerce")
                agg   = vs.get("aggregation", "sum").lower()
                if agg in ("sum", "count", "counta", "countnums"):
                    grand_row[display] = col_s.sum()
                elif agg == "average":
                    grand_row[display] = col_s.mean()
                elif agg == "min":
                    grand_row[display] = col_s.min()
                elif agg == "max":
                    grand_row[display] = col_s.max()
                else:
                    grand_row[display] = col_s.sum()

        gt_series = pd.Series(grand_row)
        # Recalculate calculated fields on grand total row
        for vs in value_specs:
            if not vs.get("is_calculated"):
                continue
            display   = (vs.get("display_name") or "").strip()
            src_field = vs.get("source_field", "")
            formula   = None
            if src_field in calc_fields:
                formula = calc_fields[src_field].get("formula")
            if not formula:
                fmeta   = fields_meta.get(src_field, {})
                formula = fmeta.get("formula") or vs.get("formula")
            if formula:
                gt_series[display] = _eval_formula(formula, gt_series)

        grand_df = pd.DataFrame([grand_row])
        result   = pd.concat([result, grand_df], ignore_index=True)

    # ── 9. In-pivot filters on result ─────────────────────────────────────────
    result = apply_pivot_filters(df, result, pivot)

    print(f"        result shape: {result.shape}")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# NUMBER FORMATTING
# ─────────────────────────────────────────────────────────────────────────────

def _excel_format_to_python(num_format: str | None) -> str | None:
    """Map Excel num_format string to a Python format hint (best-effort)."""
    if not num_format or num_format in ("General", "@"):
        return None
    nf = num_format.lower()
    if "%" in nf:
        return "pct"
    if "#,##0.00" in nf:
        return ",.2f"
    if "#,##0" in nf:
        return ",d"
    if "0.00" in nf:
        return ".2f"
    if "0.0" in nf:
        return ".1f"
    return None


def _format_value(val: Any, fmt_hint: str | None) -> str:
    """Format a cell value for HTML display."""
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return ""
    if fmt_hint == "pct":
        try:
            v = float(val)
            if abs(v) < 2:          # stored as 0.xx fraction
                return f"{v:.1%}"
            else:                    # stored as percentage points
                return f"{v:.1f}%"
        except (ValueError, TypeError):
            return str(val)
    if fmt_hint == ",d":
        try:
            return f"{int(round(float(val))):,}"
        except (ValueError, TypeError):
            return str(val)
    if fmt_hint and fmt_hint.startswith(","):
        try:
            return format(float(val), fmt_hint)
        except (ValueError, TypeError):
            return str(val)
    if fmt_hint:
        try:
            return format(float(val), fmt_hint)
        except (ValueError, TypeError):
            return str(val)
    # Auto-detect: if float and integer-valued, show without decimals
    if isinstance(val, float):
        if np.isnan(val):
            return ""
        return f"{val:,.4g}"
    return str(val)


# ─────────────────────────────────────────────────────────────────────────────
# HTML PIVOT TABLE RENDERER
# ─────────────────────────────────────────────────────────────────────────────

_HTML_STYLE = """
<style>
  :root {
    --bg: #0f1117;
    --surface: #1a1d27;
    --surface2: #22263a;
    --border: #2e3352;
    --accent: #4f8ef7;
    --accent2: #6ec6a0;
    --text: #e2e8f0;
    --text-muted: #8892b0;
    --grand: #2a3a5c;
    --header-bg: #1e2540;
    --frozen-bg: #1a2035;
    --row-even: #1a1d27;
    --row-odd: #1f2235;
    --row-hover: #263055;
    --font-ui: 'IBM Plex Sans', 'Segoe UI', system-ui, sans-serif;
    --font-mono: 'IBM Plex Mono', 'Fira Code', monospace;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  html, body { background:var(--bg); color:var(--text); font-family:var(--font-ui); font-size:13px; line-height:1.4; min-height:100vh; }

  .tab-bar { display:flex; flex-wrap:wrap; gap:4px; padding:12px 16px 0; background:var(--bg); border-bottom:1px solid var(--border); position:sticky; top:0; z-index:200; }
  .tab-btn { padding:6px 14px; border:1px solid var(--border); border-bottom:none; border-radius:4px 4px 0 0; background:var(--surface); color:var(--text-muted); cursor:pointer; font-size:11px; font-weight:500; transition:all .15s; white-space:nowrap; }
  .tab-btn:hover { background:var(--surface2); color:var(--text); }
  .tab-btn.active { background:var(--header-bg); color:var(--accent); border-color:var(--accent); border-bottom-color:var(--header-bg); }

  .pivot-panel { display:none; padding:16px; }
  .pivot-panel.active { display:block; }

  .pivot-header { margin-bottom:12px; }
  .pivot-title { font-size:15px; font-weight:600; color:var(--accent); letter-spacing:.02em; margin-bottom:6px; }
  .pivot-meta { display:flex; flex-wrap:wrap; gap:6px; }
  .meta-tag { background:var(--surface2); border:1px solid var(--border); border-radius:3px; padding:1px 7px; font-size:10px; letter-spacing:.04em; color:var(--text-muted); }
  .meta-tag.filter { border-color:var(--accent2); color:var(--accent2); }

  .controls { display:flex; gap:8px; margin-bottom:8px; flex-wrap:wrap; align-items:center; }
  .ctrl-btn { padding:4px 10px; background:var(--surface2); border:1px solid var(--border); border-radius:4px; color:var(--text-muted); cursor:pointer; font-size:11px; font-family:var(--font-ui); transition:all .15s; }
  .ctrl-btn:hover { border-color:var(--accent); color:var(--accent); }
  .search-box { padding:4px 8px; background:var(--surface2); border:1px solid var(--border); border-radius:4px; color:var(--text); font-size:11px; font-family:var(--font-ui); outline:none; width:200px; transition:border .15s; }
  .search-box:focus { border-color:var(--accent); }
  .row-count { font-size:11px; color:var(--text-muted); margin-left:auto; }

  .table-wrap { overflow:auto; max-height:72vh; border:1px solid var(--border); border-radius:6px; }

  table { border-collapse:separate; border-spacing:0; width:max-content; min-width:100%; }

  thead th {
    background:var(--header-bg); color:var(--text-muted); font-size:10px; font-weight:600;
    letter-spacing:.06em; text-transform:uppercase; padding:7px 10px;
    border-bottom:2px solid var(--accent); border-right:1px solid var(--border);
    position:sticky; top:0; z-index:10; white-space:nowrap; cursor:pointer; user-select:none;
  }
  thead th:last-child { border-right:none; }
  thead th:hover { color:var(--accent); }
  .sort-icon { margin-left:4px; font-size:8px; opacity:.3; }
  th.sort-asc  .sort-icon::after { content:"▲"; opacity:1; color:var(--accent); }
  th.sort-desc .sort-icon::after { content:"▼"; opacity:1; color:var(--accent); }

  /* Frozen dim (row-label) columns */
  td.col-dim, th.col-dim {
    background:var(--frozen-bg) !important;
    position:sticky; z-index:5;
    border-right:1px solid #4f8ef755 !important;
    text-align:left; white-space:nowrap;
  }
  th.col-dim { z-index:15; background:#162040 !important; color:var(--accent2) !important; }

  td {
    padding:5px 10px; border-bottom:1px solid var(--border);
    border-right:1px solid rgba(46,51,82,.4); white-space:nowrap;
    text-align:right; font-variant-numeric:tabular-nums; font-size:12px; color:#b0bec5;
  }
  td:last-child { border-right:none; }
  tr:nth-child(even) td { background:var(--row-even); }
  tr:nth-child(odd)  td { background:var(--row-odd); }
  tr:hover td { background:var(--row-hover) !important; }

  tr.grand-total td {
    background:var(--grand) !important; color:#e2e8f0 !important;
    font-weight:700; border-top:2px solid var(--accent);
  }

  /* ── COLLAPSE / EXPAND ─────────────────────────────────── */
  /* Every data row gets data-gid (group id) and data-pid (parent group id)  */
  /* Rows with children get a .toggle button in their first dim cell         */
  tr[data-pid]:not([data-pid=""]) { }          /* child rows – no special style by default */
  tr.grp-collapsed { }                          /* sentinel class on the header row */

  .toggle {
    display:inline-flex; align-items:center; justify-content:center;
    width:15px; height:15px;
    background:var(--surface2); border:1px solid var(--border); border-radius:3px;
    margin-right:5px; cursor:pointer; font-size:8px; color:var(--accent);
    flex-shrink:0; vertical-align:middle; user-select:none; transition:all .15s;
    line-height:1;
  }
  .toggle:hover { background:var(--accent); color:#fff; border-color:var(--accent); }
  .grp-collapsed .toggle::after { content:"▶"; }
  .grp-expanded  .toggle::after { content:"▼"; }

  /* dim indent levels */
  .d0 { padding-left:6px  !important; }
  .d1 { padding-left:20px !important; }
  .d2 { padding-left:34px !important; }
  .d3 { padding-left:48px !important; }
  .d4 { padding-left:62px !important; }
  .d5 { padding-left:76px !important; }

  tr.hidden-row { display:none; }

  .empty-state { padding:48px; text-align:center; color:var(--text-muted); }
  .empty-state code { display:block; margin-top:8px; font-family:var(--font-mono); font-size:11px; color:var(--accent); }

  ::-webkit-scrollbar { width:7px; height:7px; }
  ::-webkit-scrollbar-track { background:var(--bg); }
  ::-webkit-scrollbar-thumb { background:var(--surface2); border-radius:4px; }
  ::-webkit-scrollbar-thumb:hover { background:var(--accent); }

  /* ── COLUMN FILTER BUTTON (inside th) ── */
  .col-filter-btn {
    display:inline-flex; align-items:center; justify-content:center;
    width:14px; height:14px; padding:0; margin-left:4px;
    background:transparent; border:1px solid var(--border); border-radius:2px;
    color:var(--text-muted); cursor:pointer; font-size:9px; vertical-align:middle;
    transition:all .12s; line-height:1;
  }
  th:hover .col-filter-btn, .col-filter-btn:hover { background:var(--accent); color:#fff; border-color:var(--accent); }
  .filter-badge { font-size:9px; color:var(--accent2); font-weight:700; }

  /* ── COLUMN FILTER DROPDOWN ── */
  .col-filter-dd {
    position:absolute; z-index:9999;
    background:var(--surface); border:1px solid var(--accent);
    border-radius:6px; min-width:220px; max-width:320px;
    box-shadow:0 8px 32px rgba(0,0,0,.6);
    font-family:var(--font-ui); font-size:12px; color:var(--text);
  }
  .cfd-head {
    display:flex; align-items:center; gap:4px;
    padding:8px 8px 6px; border-bottom:1px solid var(--border);
  }
  .cfd-search {
    flex:1; padding:3px 7px; background:var(--surface2); border:1px solid var(--border);
    border-radius:3px; color:var(--text); font-size:11px; outline:none;
    font-family:var(--font-ui);
  }
  .cfd-search:focus { border-color:var(--accent); }
  .cfd-all {
    padding:2px 7px; background:var(--surface2); border:1px solid var(--border);
    border-radius:3px; color:var(--text-muted); cursor:pointer; font-size:10px;
    font-family:var(--font-ui); white-space:nowrap;
  }
  .cfd-all:hover { border-color:var(--accent); color:var(--accent); }
  .cfd-list {
    max-height:240px; overflow-y:auto; padding:4px 0;
  }
  .cfd-item {
    display:flex; align-items:center; gap:6px;
    padding:4px 10px; cursor:pointer; transition:background .1s;
  }
  .cfd-item:hover { background:var(--row-hover); }
  .cfd-item input[type=checkbox] { accent-color:var(--accent); cursor:pointer; flex-shrink:0; }
  .cfd-val { flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .cfd-cnt { font-size:10px; color:var(--text-muted); flex-shrink:0; }
  .cfd-foot {
    display:flex; gap:6px; padding:6px 8px;
    border-top:1px solid var(--border);
  }
  .cfd-apply {
    flex:1; padding:4px; background:var(--accent); border:none; border-radius:4px;
    color:#fff; cursor:pointer; font-size:11px; font-weight:600;
    font-family:var(--font-ui); transition:opacity .15s;
  }
  .cfd-apply:hover { opacity:.85; }
  .cfd-clear {
    padding:4px 10px; background:var(--surface2); border:1px solid var(--border);
    border-radius:4px; color:var(--text-muted); cursor:pointer; font-size:11px;
    font-family:var(--font-ui);
  }
  .cfd-clear:hover { border-color:var(--accent); color:var(--accent); }

  /* ── PAGE FILTER BAR ── */
  .page-filter-bar {
    display:flex; flex-wrap:wrap; align-items:center; gap:6px;
    padding:8px 0 4px; border-top:1px solid var(--border); margin-top:6px;
  }
  .pf-label { font-size:10px; color:var(--text-muted); letter-spacing:.05em; text-transform:uppercase; margin-right:2px; }
  .pf-group { display:inline-flex; align-items:center; gap:2px; }
  .pf-btn {
    padding:3px 9px; background:var(--surface2); border:1px solid var(--border);
    border-radius:4px; color:var(--text); cursor:pointer; font-size:11px;
    font-family:var(--font-ui); transition:all .15s; white-space:nowrap;
  }
  .pf-btn:hover { border-color:var(--accent); color:var(--accent); }
  .pf-btn.active { border-color:var(--accent2); color:var(--accent2); background:rgba(110,198,160,.1); }
  .pf-badge { font-size:10px; color:var(--accent2); font-weight:700; min-width:4px; }

  /* ── PER-COLUMN EXPAND/COLLAPSE ROW ── */
  tr.ctrl-row th {
    background:var(--bg) !important; padding:2px 4px !important;
    border-bottom:none !important; top:0; position:sticky; z-index:15;
  }
  tr.ctrl-row th.col-dim { z-index:20; background:var(--bg) !important; }
  .ctrl-btn.sm {
    padding:1px 5px; font-size:10px; min-width:22px;
    background:var(--surface2); border:1px solid var(--border);
    border-radius:3px; color:var(--text-muted); cursor:pointer;
    font-family:var(--font-ui); transition:all .12s;
  }
  .ctrl-btn.sm:hover { border-color:var(--accent); color:var(--accent); }

  /* subtotal row highlight when collapsed */
  tr.grp-collapsed td { color:#90caf9 !important; font-style:italic; }
  tr.grp-collapsed td.col-dim { color:#b0d4f7 !important; font-style:normal; }
</style>
"""

_HTML_JS = """
<script>
// init queue: panel <script> tags push callbacks; flushed after DOM ready
var _pivotInitQueue = [];
document.addEventListener('DOMContentLoaded', function() {
  _pivotInitQueue.forEach(function(fn){ try{ fn(); } catch(e){ console.warn('pivot init:', e); } });
  _pivotInitQueue = [];
});

function showTab(id){document.querySelectorAll('.tab-btn').forEach(b=>b.classList.toggle('active',b.dataset.tab===id));document.querySelectorAll('.pivot-panel').forEach(p=>p.classList.toggle('active',p.id===id));}

const _state={};
function _getState(tid){
  if(!_state[tid]){
    const tb=document.getElementById(tid).querySelector('tbody');
    _state[tid]={allRows:Array.from(tb.rows).filter(r=>!r.classList.contains('grand-total')),filterState:{},pageState:{},searchQ:'',sortCol:-1,sortAsc:true};
  }
  return _state[tid];
}

function _applyAll(tid){
  const st=_getState(tid);const tb=document.getElementById(tid).querySelector('tbody');const q=st.searchQ.toLowerCase();
  
  const leaves=st.allRows.filter(r=>!r.dataset.isGroup);
  let vis=leaves.filter(r=>{
    for(const[f,vals]of Object.entries(st.pageState)){if(!vals||!vals.size)continue;const rv=r.dataset['pf_'+f]||'';if(!vals.has(rv))return false;}
    for(const[ci,vals]of Object.entries(st.filterState)){
      if(!vals||!vals.size)continue;
      const cell=r.cells[parseInt(ci)];if(!cell)return false;
      const txt=_cellText(cell,ci===0);
      if(!vals.has(txt))return false;
    }
    if(q&&!r.textContent.toLowerCase().includes(q))return false;
    return true;
  });
  
  if(st.sortCol>=0)vis.sort((a,b)=>{
    const av=(a.cells[st.sortCol]?.textContent||'').trim().replace(/[,%]/g,'');
    const bv=(b.cells[st.sortCol]?.textContent||'').trim().replace(/[,%]/g,'');
    const an=parseFloat(av),bn=parseFloat(bv);
    if(!isNaN(an)&&!isNaN(bn))return st.sortAsc?an-bn:bn-an;
    return st.sortAsc?av.localeCompare(bv):bv.localeCompare(av);
  });
  
  st.allRows.forEach(r=>r.classList.add('hidden-row'));
  
  const visPids=new Set();
  const table = document.getElementById(tid);
  const meta = JSON.parse(table.dataset.meta ? table.dataset.meta.replace(/&quot;/g, '"') : '{}');
  const groupAggs = { "__GRAND__": {} };

  function addRaw(target, raw) {
      for(let k in raw) {
          if(typeof raw[k] === 'number') {
              if (target[k] === undefined) {
                  target[k] = raw[k];
                  target[k + "_max"] = raw[k];
                  target[k + "_min"] = raw[k];
              } else {
                  target[k] += raw[k];
                  if(raw[k] > target[k + "_max"]) target[k + "_max"] = raw[k];
                  if(raw[k] < target[k + "_min"]) target[k + "_min"] = raw[k];
              }
          }
      }
      target.__count = (target.__count || 0) + 1;
  }

  vis.forEach(r=>{
    let pid=r.dataset.pid;
    while(pid){
      visPids.add(pid);
      pid=pid.substring(0,pid.lastIndexOf('|'));
    }
    
    const raw = JSON.parse(r.dataset.raw ? r.dataset.raw.replace(/'/g, '"') : '{}');
    addRaw(groupAggs["__GRAND__"], raw);
    let parts = r.dataset.gid ? r.dataset.gid.split('|') : [];
    for(let i=1; i<parts.length; i++) {
         let subGid = parts.slice(0,i).join('|');
         if(!groupAggs[subGid]) groupAggs[subGid] = {};
         addRaw(groupAggs[subGid], raw);
    }
  });

  function evalFormula(formula, vars) {
      try {
          const keys = Object.keys(vars).filter(k=>/^[a-zA-Z_$][0-9a-zA-Z_$]*$/.test(k));
          const vals = keys.map(k=>Number(vars[k])||0);
          const fn = new Function(...keys, "return " + formula + ";");
          return fn(...vals);
      } catch(e) { return null; }
  }

  function formatValue(v, fmt) {
      if(v === null || isNaN(v) || typeof v === 'undefined' || v === Infinity || v === -Infinity) return "";
      if(!fmt) {
          if(Number.isInteger(v)) return v.toLocaleString();
          return v.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
      }
      fmt = fmt.trim().toLowerCase();
      if(fmt === "0.0%") return (v*100).toFixed(1) + "%";
      if(fmt === "0%") return (v*100).toFixed(0) + "%";
      if(fmt === "0,0") return Math.round(v).toLocaleString();
      return v.toLocaleString(undefined, {minimumFractionDigits:2, maximumFractionDigits:2});
  }

  function updateCells(tr, agg) {
      if(!agg) return;
      const calcFormulas = meta.calc_formulas || {};
      const valCols = meta.val_cols || [];
      const fmtMap = meta.fmt_map || {};
      const aggFnMap = meta.agg_fn_map || {};

      for(let dn in calcFormulas) {
          if (valCols.includes(dn)) {
              agg[dn] = evalFormula(calcFormulas[dn], agg);
          }
      }
      valCols.forEach((col, idx) => {
          let cellIdx = (parseInt(table.dataset.ndims)>0 ? 1 : 0) + idx;
          let cell = tr.cells[cellIdx];
          if(cell) {
             let val = agg[col];
             let fn = (aggFnMap[col] || "sum").toLowerCase();
             let computed = val;
             if(fn === "count") computed = agg.__count;
             else if(fn === "average") computed = (val || 0) / (agg.__count || 1);
             else if(fn === "max") computed = agg[col + "_max"];
             else if(fn === "min") computed = agg[col + "_min"];
             
             if(calcFormulas[col]) computed = agg[col];
             
             cell.innerHTML = formatValue(computed, fmtMap[col]);
             if(tr.classList.contains('grand-total')) {
                 cell.innerHTML = "<strong>" + cell.innerHTML + "</strong>";
             }
          }
      });
  }

  st.allRows.forEach(r=>{
    if(r.dataset.isGroup&&visPids.has(r.dataset.gid)){
        r.classList.remove('hidden-row');
        updateCells(r, groupAggs[r.dataset.gid]);
    } else if (r.classList.contains('grand-total')) {
        updateCells(r, groupAggs["__GRAND__"]);
    }
  });

  vis.forEach(r=>r.classList.remove('hidden-row'));

  if(st.sortCol>=0){
    vis.forEach(r=>tb.appendChild(r));
  }
  
  const rc=tb.closest('.pivot-panel').querySelector('.row-count');
  if(rc)rc.textContent=vis.length.toLocaleString()+' leaf rows';
}

function _cellText(cell,isDim){
  if(!isDim)return(cell.textContent||'').trim();
  return Array.from(cell.childNodes).filter(n=>n.nodeType===3||(n.nodeType===1&&!n.classList.contains('toggle')&&!n.classList.contains('col-filter-btn'))).map(n=>n.textContent).join('').trim();
}

function sortTable(th,tid){
  const st=_getState(tid);const ths=Array.from(th.closest('tr').querySelectorAll('th'));const ci=ths.indexOf(th);
  st.sortAsc=(st.sortCol===ci)?!st.sortAsc:true;st.sortCol=ci;
  ths.forEach(h=>h.classList.remove('sort-asc','sort-desc'));th.classList.add(st.sortAsc?'sort-asc':'sort-desc');
  _applyAll(tid);
}

let _openDD=null;
function _closeDD(){if(_openDD){_openDD.remove();_openDD=null;}document.removeEventListener('click',_outsideClick);}
function _outsideClick(e){if(_openDD&&!_openDD.contains(e.target))_closeDD();}

function _buildDD(vals,counts,current,applyFn){
  const dd=document.createElement('div');dd.className='col-filter-dd';
  dd.innerHTML='<div class="cfd-head"><input class="cfd-search" placeholder="Search values…" oninput="filterDDSearch(this)"/><button class="cfd-all" onclick="selectAllDD(this,true)">All</button><button class="cfd-all" onclick="selectAllDD(this,false)">None</button></div>'
  +'<div class="cfd-list">'+vals.map(v=>{const chk=current.size===0||current.has(v);const esc=v.replace(/"/g,'&quot;');return'<label class="cfd-item"><input type="checkbox" value="'+esc+'"'+(chk?' checked':'')+'><span class="cfd-val">'+(v||'(blank)')+'</span><span class="cfd-cnt">'+(counts[v]||'')+'</span></label>';}).join('')+'</div>'
  +'<div class="cfd-foot"><button class="cfd-apply">Apply</button><button class="cfd-clear">Clear</button></div>';
  dd.querySelector('.cfd-apply').onclick=()=>applyFn(dd,false);
  dd.querySelector('.cfd-clear').onclick=()=>applyFn(dd,true);
  return dd;
}
function _posDD(dd,btn){const r=btn.getBoundingClientRect();dd.style.top=(r.bottom+window.scrollY+3)+'px';dd.style.left=Math.max(0,r.left+window.scrollX)+'px';document.body.appendChild(dd);_openDD=dd;setTimeout(()=>document.addEventListener('click',_outsideClick),0);}

function openColFilter(btn,tid,ci){
  _closeDD();const st=_getState(tid);const ndims=parseInt(document.getElementById(tid).dataset.ndims||'0');
  const cur=st.filterState[ci]||new Set();const counts={};
  st.allRows.forEach(r=>{const cell=r.cells[ci];const txt=_cellText(cell,ci===0);if(!r.dataset.isGroup){counts[txt]=(counts[txt]||0)+1;}});
  const vals=Object.keys(counts).sort((a,b)=>{const an=parseFloat(a.replace(/,/g,'')),bn=parseFloat(b.replace(/,/g,''));return(!isNaN(an)&&!isNaN(bn))?an-bn:a.localeCompare(b);});
  const dd=_buildDD(vals,counts,cur,(dd,clr)=>{
    if(clr){st.filterState[ci]=new Set();}
    else{const cbs=dd.querySelectorAll('input[type=checkbox]');const sel=new Set(Array.from(cbs).filter(c=>c.checked).map(c=>c.value));const all=new Set(Array.from(cbs).map(c=>c.value));st.filterState[ci]=sel.size===all.size?new Set():sel;}
    const th=document.getElementById(tid).querySelectorAll('thead th')[ci];if(th){const b=th.querySelector('.filter-badge');if(b)b.textContent=st.filterState[ci].size>0?' ['+st.filterState[ci].size+']':'';}
    _closeDD();_applyAll(tid);
  });
  _posDD(dd,btn);
}

function openPageFilter(btn,tid,field,allVals){
  _closeDD();const st=_getState(tid);const cur=st.pageState[field]||new Set();
  const counts={};st.allRows.filter(r=>!r.dataset.isGroup).forEach(r=>{const v=r.dataset['pf_'+field]||'';counts[v]=(counts[v]||0)+1;});
  const vals=[...new Set([...allVals,...Object.keys(counts)])].sort();
  const dd=_buildDD(vals,counts,cur,(dd,clr)=>{
    if(clr){st.pageState[field]=new Set();}
    else{const cbs=dd.querySelectorAll('input[type=checkbox]');const sel=new Set(Array.from(cbs).filter(c=>c.checked).map(c=>c.value));const all=new Set(Array.from(cbs).map(c=>c.value));st.pageState[field]=sel.size===all.size?new Set():sel;}
    const badge=btn.parentElement.querySelector('.pf-badge');if(badge)badge.textContent=st.pageState[field].size>0?' ['+st.pageState[field].size+']':'';
    _closeDD();_applyAll(tid);
  });
  _posDD(dd,btn);
}

function filterDDSearch(inp){const q=inp.value.toLowerCase();inp.closest('.col-filter-dd').querySelectorAll('.cfd-item').forEach(it=>it.style.display=it.textContent.toLowerCase().includes(q)?'':'none');}
function selectAllDD(btn,chk){btn.closest('.col-filter-dd').querySelectorAll('input[type=checkbox]').forEach(cb=>cb.checked=chk);}

function toggleGroup(btn){
  const tr=btn.closest('tr');const gid=tr.dataset.gid;const tb=tr.closest('tbody');const isOpen=tr.classList.contains('grp-expanded');
  tr.classList.toggle('grp-expanded',!isOpen);tr.classList.toggle('grp-collapsed',isOpen);
  if(isOpen){
    Array.from(tb.rows).forEach(r=>{
      const pid=r.dataset.pid||'';
      if(pid===gid||pid.startsWith(gid+'|')){
        r.classList.add('hidden-row');
        if(r.dataset.isGroup){r.classList.remove('grp-expanded');r.classList.add('grp-collapsed');}
      }
    });
  } else {
    Array.from(tb.rows).forEach(r=>{if((r.dataset.pid||'')===gid)r.classList.remove('hidden-row');});
  }
}

function expandAll(tid){
  const tb=document.getElementById(tid).querySelector('tbody');
  Array.from(tb.rows).forEach(r=>{
    r.classList.remove('hidden-row');
    if(r.dataset.isGroup){r.classList.remove('grp-collapsed');r.classList.add('grp-expanded');}
  });
}
function collapseAll(tid){
  const tb=document.getElementById(tid).querySelector('tbody');
  Array.from(tb.rows).forEach(r=>{
    if(r.classList.contains('grand-total'))return;
    if(parseInt(r.dataset.lvl||'0')>0) r.classList.add('hidden-row');
    if(r.dataset.isGroup){r.classList.remove('grp-expanded');r.classList.add('grp-collapsed');}
  });
}

function searchTable(inp,tid){_getState(tid).searchQ=inp.value;_applyAll(tid);}
</script>
"""



def _build_pivot_html(result: pd.DataFrame, pivot: dict, panel_id: str) -> str:
    """Render one pivot table as a full interactive HTML panel."""
    import json as _json
    import re

    pid   = pivot.get("id", "?")
    pname = pivot.get("name", pid)
    host  = pivot.get("host_sheet", "")

    row_fields = [r["field"] for r in pivot.get("rows", [])
                  if r.get("type") == "field" and r.get("field") != "__VALUES__"]
    dim_cols = [f for f in row_fields if f in result.columns]
    val_cols = [c for c in result.columns if c not in dim_cols]
    n_dims   = len(dim_cols)

    # ── value format map ─────────────────────────────────────────────────
    fmt_map: dict[str, str | None] = {}
    for vs in pivot.get("values", []):
        dn = (vs.get("display_name") or "").strip()
        fmt_map[dn] = _excel_format_to_python(vs.get("num_format"))

    # ── aggregation function map (for subtotals) ─────────────────────────
    agg_fn_map: dict[str, str] = {}
    for vs in pivot.get("values", []):
        dn = (vs.get("display_name") or "").strip()
        agg_fn_map[dn] = (vs.get("aggregation") or "sum").lower()

    def _is_grand(row):
        return dim_cols and str(row.get(dim_cols[0], "")).strip() == "Grand Total"

    data_rows  = [row for _, row in result.iterrows() if not _is_grand(row)]
    grand_rows = [row for _, row in result.iterrows() if  _is_grand(row)]

    # ── PAGE FILTERS: build interactive filter bar ────────────────────────
    # Collect all pivot-level (axisPage) filters — both show_all and selective
    page_filters = pivot.get("filters", [])
    # For each filter, gather distinct values from JSON cache
    pf_html_parts = []
    for f in page_filters:
        fname     = f.get("field", "")
        show_all  = f.get("show_all", True)
        selected  = f.get("selected_item")

        # Collect distinct values from JSON cache
        dv_list = [dv.get("value", dv) if isinstance(dv, dict) else dv
                   for dv in pivot.get("fields", {}).get(fname, {}).get("distinct_values", [])]
        # Prettify date strings
        pretty_vals = []
        for v in dv_list:
            try:
                pretty_vals.append(str(pd.Timestamp(v).date()))
            except Exception:
                pretty_vals.append(str(v))

        # Current selection label for badge
        sel_pretty = ""
        if not show_all and selected is not None:
            try:    sel_pretty = str(pd.Timestamp(selected).date())
            except: sel_pretty = str(selected)

        # Safe dataset key: no spaces/hyphens/slashes (must be valid HTML data-* name)
        ds_key = re.sub(r'[^A-Za-z0-9_]', '_', fname)
        # JSON for allVals: encode " as &quot; so it doesn't break onclick="..."
        safe_all_vals = _json.dumps(pretty_vals).replace('"', '&quot;')

        # Default badge: if filter is active show the selected value
        badge_text = f" [{sel_pretty}]" if sel_pretty else ""

        # Use &quot; for all string args in onclick so the " attribute boundary is safe
        btn_html = (f'<span class="pf-group">'
                    f'<button class="pf-btn" '
                    f'onclick="openPageFilter(this,&quot;{panel_id}_tbl&quot;,&quot;{ds_key}&quot;,{safe_all_vals})">'
                    f'▼ {html.escape(fname)}</button>'
                    f'<span class="pf-badge">{html.escape(badge_text)}</span>'
                    f'</span>')
        pf_html_parts.append(btn_html)

    filter_bar_html = ""
    if pf_html_parts:
        filter_bar_html = (f'<div class="page-filter-bar">'
                           f'<span class="pf-label">Page Filters:</span>'
                           + "".join(pf_html_parts)
                           + f'</div>')

    # ── HEADER ────────────────────────────────────────────────────────────
    header = (f'<div class="pivot-header">'
              f'<div class="pivot-title">{html.escape(pname)}</div>'
              f'<div class="pivot-meta">'
              f'<span class="meta-tag">{html.escape(pid)}</span>'
              f'<span class="meta-tag">{html.escape(host)}</span>'
              f'<span class="meta-tag">{len(data_rows):,} rows</span>'
              f'<span class="meta-tag">{len(val_cols)} measures</span>'
              f'</div>'
              f'{filter_bar_html}'
              f'</div>')

    table_id = f"{panel_id}_tbl"

    if result.empty:
        return (f'<div class="pivot-panel" id="{panel_id}">{header}'
                f'<div class="empty-state">No data returned.'
                f'<code>Check page filters and that column names match the Excel sheet.</code>'
                f'</div></div>')

    # ── Pre-compute per-group subtotals at every prefix level ─────────────
    # extract calc formulas
    calc_formulas = {}
    calc_fields = {cf["name"]: cf for cf in pivot.get("calculated_fields", [])}
    fields_meta = pivot.get("fields", {})
    for vs in pivot.get("values", []):
        if not vs.get("is_calculated"): continue
        dn = (vs.get("display_name") or "").strip()
        src = vs.get("source_field", "")
        formula = None
        if src in calc_fields: formula = calc_fields[src].get("formula")
        if not formula: formula = fields_meta.get(src, {}).get("formula") or vs.get("formula")
        if not formula and dn in calc_fields: formula = calc_fields[dn].get("formula")
        if formula:
            calc_formulas[dn] = formula

    if n_dims > 0:
        df_work = pd.DataFrame(data_rows)
        for col in val_cols:
            df_work[col] = pd.to_numeric(df_work[col], errors="coerce")

        group_agg: dict[str, dict[str, str]] = {}
        # For each prefix depth 0..n_dims-2, group and aggregate
        for depth in range(n_dims - 1):
            group_keys = dim_cols[:depth + 1]
            agg_spec   = {}
            for vc in val_cols:
                if vc in calc_formulas: continue
                fn = agg_fn_map.get(vc.strip(), "sum")
                if fn == "count":    agg_spec[vc] = "count"
                elif fn == "average": agg_spec[vc] = "mean"
                elif fn == "max":    agg_spec[vc] = "max"
                elif fn == "min":    agg_spec[vc] = "min"
                else:                agg_spec[vc] = "sum"

            try:
                grp = df_work.groupby(group_keys, sort=False).agg(agg_spec).reset_index()
                
                # Re-evaluate calculated fields for subtotals
                for dn, formula in calc_formulas.items():
                    if dn in val_cols:
                        grp[dn] = grp.apply(lambda r, f=formula: _eval_formula(f, r), axis=1)

                for _, grow in grp.iterrows():
                    # subtotal key is only the prefix path
                    prefix_key = "|".join(str(grow.get(k, "") or "").strip() for k in group_keys)
                    agg_vals = {}
                    for vc in val_cols:
                        raw = grow.get(vc)
                        fmt = fmt_map.get(vc.strip())
                        agg_vals[vc.strip()] = _format_value(raw, fmt)
                    group_agg[prefix_key] = agg_vals
            except Exception as e:
                pass  # subtotals unavailable for this level
    else:
        group_agg = {}

    # ── TABLE HEADER ──────────────────────────────────────────────────────
    # Row 1: actual column headers with sort + filter
    th_cells = ""
    if n_dims > 0:
        th_cells += '<th class="col-dim" style="left:0px">Row Labels</th>'
    
    for i, col in enumerate(val_cols):
        safe_col = col.strip().replace("'", "\\'").replace('"', '\\"')
        filter_btn = (f'<button class="col-filter-btn" '
                      f'onclick="openColFilter(this,\'{table_id}\',{i + (1 if n_dims > 0 else 0)});event.stopPropagation()">▾</button>'
                      f'<span class="filter-badge"></span>')
        th_cells += (f'<th data-colname="{html.escape(col.strip())}" '
                     f'onclick="sortTable(this,\'{table_id}\')">'
                     f'{html.escape(col.strip())} {filter_btn}'
                     f'<span class="sort-icon"></span></th>')

    thead = (f'<thead>'
             f'<tr>{th_cells}</tr>'
             f'</thead>')

    # ── Controls bar ──────────────────────────────────────────────────────
    controls = (f'<div class="controls">'
                f'<button class="ctrl-btn" onclick="expandAll(\'{table_id}\')">⊞ Expand All</button>'
                f'<button class="ctrl-btn" onclick="collapseAll(\'{table_id}\')">⊟ Collapse All</button>'
                f'<input class="search-box" placeholder="🔍 Search…" oninput="searchTable(this,\'{table_id}\')"/>'
                f'<span class="row-count">{len(data_rows):,} leaf rows</span>'
                f'</div>')

    # ── TABLE BODY ────────────────────────────────────────────────────────
    def get_pf_attrs(row, pf_fields):
        pf_attrs = ""
        for pf_field in pf_fields:
            raw_val = str(row.get(pf_field, "") or "").strip()
            try:    pv = str(pd.Timestamp(raw_val).date())
            except: pv = raw_val
            ds_k = re.sub(r'[^A-Za-z0-9_]', '_', pf_field)
            pf_attrs += f' data-pf_{ds_k}="{html.escape(pv)}"'
        return pf_attrs

    pf_fields = [f.get("field", "") for f in page_filters if f.get("field")]
    tbody_rows: list[str] = []
    prev_path = ["__SENTINEL__"] * n_dims

    for row in data_rows:
        path = [str(row.get(dim_cols[l], "") or "").strip() for l in range(n_dims)]
        
        diverge_lvl = 0
        while diverge_lvl < n_dims and path[diverge_lvl] == prev_path[diverge_lvl]:
            diverge_lvl += 1
            
        for l in range(diverge_lvl, n_dims - 1):
            sub_key = "|".join(path[:l+1])
            pid_l = "|".join(path[:l]) if l > 0 else ""
            agg_data = group_agg.get(sub_key, {})
            
            v_str = html.escape(path[l])
            indent = f"d{min(l, 5)}"
            tog = '<span class="toggle" onclick="toggleGroup(this)"></span>'
            dim_td = f'<td class="col-dim {indent}" style="left:0px">{tog}{v_str}</td>'
            
            val_tds = ""
            for vcol in val_cols:
                v = agg_data.get(vcol.strip(), "")
                val_tds += f"<td>{html.escape(v)}</td>"
                
            safe_gid = sub_key.replace('"', '&quot;').replace("'", "&#39;")
            safe_pid = pid_l.replace('"', '&quot;').replace("'", "&#39;")
            
            # Group headers start expanded
            tbody_rows.append(
                f'<tr class="grp-expanded" data-gid="{safe_gid}" data-pid="{safe_pid}" data-lvl="{l}" data-is-group="1">'
                f'{dim_td}{val_tds}</tr>'
            )

        gid = "|".join(path)
        pid = "|".join(path[:n_dims - 1]) if n_dims > 1 else ""
        clvl = n_dims - 1
        v_str = html.escape(path[clvl]) if n_dims > 0 else ""
        indent = f"d{min(clvl, 5)}"
        
        dim_td = f'<td class="col-dim {indent}" style="left:0px">{v_str}</td>' if n_dims > 0 else ''
        
        raw_vals = {}
        needed_vars = set(val_cols)
        import re
        for f in calc_formulas.values():
            needed_vars.update(re.findall(r'[a-zA-Z_]\w*', f))
        for k in needed_vars:
            if k in row:
                try: raw_vals[k] = float(row[k])
                except: pass
        raw_json = _json.dumps(raw_vals).replace("'", "&#39;")

        val_tds = ""
        for vcol in val_cols:
            v      = row.get(vcol)
            if pd.isna(v) and vcol in calc_formulas:
                v = _eval_formula(calc_formulas[vcol], row)
            fmt    = fmt_map.get(vcol.strip())
            fmtd   = _format_value(v, fmt)
            val_tds += f"<td>{html.escape(fmtd)}</td>"
            
        pf_attrs = get_pf_attrs(row, pf_fields)
        safe_gid = gid.replace('"', '&quot;').replace("'", "&#39;")
        safe_pid = pid.replace('"', '&quot;').replace("'", "&#39;")
        
        tbody_rows.append(
            f'<tr data-gid="{safe_gid}" data-pid="{safe_pid}" data-lvl="{clvl}" data-raw=\'{raw_json}\'{pf_attrs}>'
            f'{dim_td}{val_tds}</tr>'
        )
        prev_path = path[:]

    # Grand total
    for row in grand_rows:
        dim_td = f'<td class="col-dim d0" style="left:0px"><strong>Grand Total</strong></td>' if n_dims > 0 else ''
        val_tds = ""
        for vcol in val_cols:
            v   = row.get(vcol)
            if pd.isna(v) and vcol in calc_formulas:
                v = _eval_formula(calc_formulas[vcol], row)
            fmt = fmt_map.get(vcol.strip())
            val_tds += f"<td><strong>{html.escape(_format_value(v, fmt))}</strong></td>"
        tbody_rows.append(f'<tr class="grand-total">{dim_td}{val_tds}</tr>')

    tbody = "<tbody>" + "\n".join(tbody_rows) + "</tbody>"
    
    meta = {
        "val_cols": val_cols,
        "agg_fn_map": agg_fn_map,
        "fmt_map": fmt_map,
        "calc_formulas": calc_formulas,
    }
    meta_json = _json.dumps(meta).replace("'", "&#39;").replace('"', '&quot;')
    
    table = (f'<div class="table-wrap">'
             f'<table id="{table_id}" data-ndims="{n_dims}" data-meta="{meta_json}">'
             f'{thead}{tbody}</table></div>')

    # ── Initialise page-filter state on load (apply JSON defaults) ────────
    # Collect init calls into a registry that _HTML_JS flushes after DOMContentLoaded
    init_js_parts = []
    for f in page_filters:
        fname    = f.get("field", "")
        show_all = f.get("show_all", True)
        selected = f.get("selected_item")
        if show_all or selected is None:
            continue
        try:    sel_pretty = str(pd.Timestamp(selected).date())
        except: sel_pretty = str(selected)
        safe_field = re.sub(r'[^A-Za-z0-9_]', '_', fname)
        init_js_parts.append(
            f'_pivotInitQueue.push(function(){{'
            f'var st=_getState("{table_id}");'
            f'st.pageState["{safe_field}"]=new Set(["{sel_pretty}"]);'
            f'_applyAll("{table_id}");'
            f'}});'
        )

    init_script = ""
    if init_js_parts:
        init_script = "<script>" + "\n".join(init_js_parts) + "</script>"

    return (f'<div class="pivot-panel" id="{panel_id}">'
            f'{header}{controls}{table}{init_script}</div>')


def build_html_dashboard(pivot_results: list[tuple[dict, pd.DataFrame]],
                         title: str = "Pivot Dashboard") -> str:
    """
    Build a complete self-contained HTML dashboard with one tab per pivot.
    pivot_results: list of (pivot_dict, result_dataframe)
    """
    gfonts = (
        '<link rel="preconnect" href="https://fonts.googleapis.com">'
        '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
        '<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600'
        '&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">'
    )

    # Build tabs
    tab_buttons = ""
    panels      = ""
    first       = True
    for i, (pivot, result) in enumerate(pivot_results):
        pid      = pivot.get("id", f"pivot_{i}")
        pname    = pivot.get("name", pid)
        host     = pivot.get("host_sheet", "")
        panel_id = f"panel_{i}"
        active   = "active" if first else ""
        short    = pname[:22] + "…" if len(pname) > 24 else pname
        tab_buttons += (
            f'<button class="tab-btn {active}" data-tab="{panel_id}" '
            f'onclick="showTab(\'{panel_id}\')">'
            f'{html.escape(short)}'
            f'</button>'
        )
        panels += _build_pivot_html(result, pivot, panel_id)
        first   = False

    tab_bar = f'<div class="tab-bar">{tab_buttons}</div>' if len(pivot_results) > 1 else ""
    if len(pivot_results) == 1:
        panels = panels.replace('class="pivot-panel"', 'class="pivot-panel active"')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>{html.escape(title)}</title>
  {gfonts}
  {_HTML_STYLE}
  {_HTML_JS}
</head>
<body>
  {tab_bar}
  {panels}
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def _find_json_alongside(xls_path: str) -> str | None:
    p = Path(xls_path)
    for j in p.parent.glob("*.json"):
        return str(j)
    return None


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Execute pivot JSON definitions against an Excel sheet → HTML dashboard",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("xls_file",  nargs="?", help="Path to Excel workbook (master data)")
    ap.add_argument("--json",    dest="json_file", default=None,
                    help="Path to pivot JSON file (default: auto-detect alongside xls)")
    ap.add_argument("--sheet",   default=None,
                    help="Master worksheet name (default: from JSON meta)")
    ap.add_argument("--header-row", type=int, default=None,
                    help="1-based header row (default: from JSON meta)")
    ap.add_argument("--pivot-id", default=None,
                    help="Run only this pivot ID (default: run all)")
    ap.add_argument("--output",  default=None,
                    help="Output HTML path (default: <pivot_id>.html or dashboard.html)")
    ap.add_argument("--combined", action="store_true",
                    help="Write all pivots into a single HTML file (default: one per pivot)")
    ap.add_argument("--list-pivots", action="store_true",
                    help="Print available pivot IDs and exit")
    ap.add_argument("--backend", choices=["excel", "duckdb"], default="excel",
                    help="Data backend (default: excel; duckdb = Step 2)")
    args = ap.parse_args()

    # ── Locate JSON ──────────────────────────────────────────────────────────
    json_path = args.json_file
    if not json_path and args.xls_file:
        json_path = _find_json_alongside(args.xls_file)
    if not json_path or not os.path.exists(json_path):
        print("ERROR: pivot JSON file not found. Use --json <path>")
        sys.exit(1)

    with open(json_path, encoding="utf-8") as f:
        raw = f.read()

    # Gracefully handle truncated JSON (e.g. "............." placeholders)
    trunc_idx = raw.find(".............")
    if trunc_idx != -1:
        print(f"[loader] WARNING: JSON is truncated at char {trunc_idx} – only partial pivots available")
        raw = raw[:trunc_idx].rstrip().rstrip(",") + "\n  ]\n}"

    config    = json.loads(raw)
    meta      = config.get("meta", {})
    ms_info   = config.get("master_sheet_info", {})
    pivots    = config.get("pivots", [])

    # Skip failed pivots
    valid_pivots = [p for p in pivots if "error" not in p]
    failed       = len(pivots) - len(valid_pivots)
    if failed:
        print(f"[loader] Skipping {failed} failed pivot(s) (have 'error' key)")

    # ── List mode ────────────────────────────────────────────────────────────
    if args.list_pivots:
        print(f"\n{'ID':<40} {'Name':<30} {'Host Sheet'}")
        print("─" * 85)
        for p in valid_pivots:
            print(f"{p.get('id',''):<40} {p.get('name',''):<30} {p.get('host_sheet','')}")
        print(f"\n{len(valid_pivots)} pivot(s) available")
        return

    if not args.xls_file:
        print("ERROR: xls_file argument is required (or use --list-pivots)")
        sys.exit(1)

    # ── Resolve sheet + header from JSON ─────────────────────────────────────
    # Priority: CLI args > master_sheet_info > meta
    sheet_name = args.sheet or ms_info.get("sheet") or meta.get("master_sheet")
    header_row = args.header_row or ms_info.get("header_row") or meta.get("header_row") or 1

    if not sheet_name:
        print("ERROR: could not determine master sheet name – use --sheet")
        sys.exit(1)

    print(f"[config] sheet={sheet_name!r}  header_row={header_row}")

    # ── Backend ──────────────────────────────────────────────────────────────
    if args.backend == "duckdb":
        raise NotImplementedError(
            "DuckDB backend (Step 2) not yet implemented. "
            "Create DuckDBBackend(DataBackend) and add it here."
        )
    backend   = ExcelBackend(args.xls_file, sheet_name, int(header_row))
    df_master = backend.load()

    # ── Select pivots ────────────────────────────────────────────────────────
    if args.pivot_id:
        selected = [p for p in valid_pivots if p.get("id") == args.pivot_id]
        if not selected:
            print(f"ERROR: pivot id '{args.pivot_id}' not found")
            print("Run with --list-pivots to see available IDs")
            sys.exit(1)
    else:
        selected = valid_pivots

    print(f"[engine] Executing {len(selected)} pivot(s)")

    # ── Execute ───────────────────────────────────────────────────────────────
    pivot_results: list[tuple[dict, pd.DataFrame]] = []
    for pivot in selected:
        pid = pivot.get("id", pivot.get("name", "pivot"))
        try:
            result = execute_pivot(df_master, pivot, backend)
            pivot_results.append((pivot, result))
        except Exception as e:
            import traceback
            print(f"  ERROR in '{pid}': {e}")
            traceback.print_exc()
            # Add empty result so the tab still shows with an error message
            pivot_results.append((pivot, pd.DataFrame()))

    # ── Render HTML ──────────────────────────────────────────────────────────
    if args.combined or len(selected) == 1 or args.output:
        # Single HTML file
        out_path = args.output or (
            f"{selected[0].get('id','pivot')}.html"
            if len(selected) == 1
            else "dashboard.html"
        )
        src_name = meta.get("source_file", Path(args.xls_file).name)
        html_doc = build_html_dashboard(
            pivot_results,
            title=f"Pivot Dashboard – {src_name}"
        )
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_doc)
        print(f"\n[output] ✓ Dashboard → {out_path}  ({len(pivot_results)} pivot(s))")
    else:
        # One HTML per pivot
        for pivot, result in pivot_results:
            pid      = pivot.get("id", "pivot")
            out_path = f"{pid}.html"
            src_name = meta.get("source_file", Path(args.xls_file).name)
            html_doc = build_html_dashboard(
                [(pivot, result)],
                title=f"{pivot.get('name', pid)} – {src_name}"
            )
            with open(out_path, "w", encoding="utf-8") as f:
                f.write(html_doc)
            rows = len(result)
            print(f"  ✓ {out_path}  ({rows:,} rows)")

    print("[engine] Done.")


if __name__ == "__main__":
    main()
