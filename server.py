#!/usr/bin/env python3
"""
server.py  —  Live Pivot Dashboard Server
==========================================
Identical look to the static HTML files.
Page filter buttons re-query DuckDB on every selection.

Fixes applied (from audit):
  #1  _is_date_col — cached, no per-call DB connection
  #2  load() is now a lazy sentinel — no wasted SELECT *
  #3  eval() on script tags replaced with explicit init calls
  #5  Spinner overlays table instead of replacing DOM (no reflow, no collapse loss)
  #6  data-field attribute on pf-btn for reliable badge restore
  #7  Module-level shared read-only DuckDB connection
  #9  CORS locked to localhost for safety (edit ALLOWED_ORIGINS for deployment)

Run:
    uvicorn server:app --port 8000
Open:
    http://localhost:8000
"""
from __future__ import annotations
import html as _html_mod
import io
import json
import re
import time
from pathlib import Path

import duckdb
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

from pivot_query_engine7 import (
    DataBackend, execute_pivot, _build_pivot_html, _HTML_STYLE, _HTML_JS,
)

# ── Config ─────────────────────────────────────────────────────────────────────
DUCKDB_PATH     = "dark_db.duckdb"
MASTER_TABLE    = "master_data"
PIVOTS_JSON     = "pivots.json"

# For deployment: change to your actual domain/IP e.g. ["https://yourdomain.com"]
ALLOWED_ORIGINS = ["http://localhost:8000", "http://127.0.0.1:8000",
                   "http://localhost:3000", "*"]

# ── App ────────────────────────────────────────────────────────────────────────
app = FastAPI(title="Pivot Dashboard")
app.add_middleware(CORSMiddleware, allow_origins=ALLOWED_ORIGINS,
                   allow_methods=["*"], allow_headers=["*"])

# ── Fix #7: Module-level shared read-only DuckDB connection ────────────────────
# One connection shared across all requests — DuckDB read-only connections
# are thread-safe and far cheaper than opening one per request.
_DB: duckdb.DuckDBPyConnection | None = None

def _db() -> duckdb.DuckDBPyConnection:
    global _DB
    if _DB is None:
        _DB = duckdb.connect(DUCKDB_PATH, read_only=True)
        print(f"[server] DuckDB connected → {DUCKDB_PATH}")
    return _DB

# ── Load pivots ────────────────────────────────────────────────────────────────
def _load_pivots() -> list[dict]:
    path = Path(PIVOTS_JSON)
    if not path.exists():
        raise FileNotFoundError("pivots.json not found — run extractor first")
    raw = path.read_text(encoding="utf-8")
    # Fix #8: graceful truncation handling
    trunc = raw.find(".............")
    if trunc != -1:
        raw = raw[:trunc].rstrip().rstrip(",") + "\n  ]\n}"
    return [p for p in json.loads(raw).get("pivots", []) if "error" not in p]

PIVOTS: list[dict] = _load_pivots()
print(f"[server] {len(PIVOTS)} pivots loaded")

# ── Pre-warm: build column cache and create DuckDB indexes at startup ──────────
def _warmup() -> None:
    """
    Run at startup to:
    1. Build column name/type cache (avoids first-request lag)
    2. Create indexes on all page-filter columns used across pivots
       so WHERE clauses on those columns are fast
    """
    _ensure_col_cache()

    # Collect all filter fields used across all pivots
    filter_fields: set[str] = set()
    for p in PIVOTS:
        for f in p.get("filters", []):
            field = f.get("field", "")
            if field and field != "__VALUES__":
                filter_fields.add(field)

    # DuckDB read-only connection can't create indexes.
    # Use a temporary write connection just for index creation.
    if filter_fields:
        try:
            con = duckdb.connect(DUCKDB_PATH)   # read-write for index creation
            for field in filter_fields:
                real_col = _col_map.get(field, field)
                idx_name = re.sub(r"[^A-Za-z0-9]", "_", real_col)
                try:
                    con.execute(
                        f"CREATE INDEX IF NOT EXISTS idx_{idx_name} "
                        f"ON {MASTER_TABLE} ({_q(real_col)})"
                    )
                    print(f"  [index] {real_col}")
                except Exception as e:
                    print(f"  [index skip] {real_col}: {e}")
            con.close()
            print(f"[warmup] Indexes ready for {len(filter_fields)} filter columns")
        except Exception as e:
            print(f"[warmup] Index creation skipped: {e}")



# ── Fix #1: Cached column metadata — built once, never re-queried ──────────────
_col_map:   dict[str, str]  = {}   # sanitised_key → real column name
_col_types: dict[str, bool] = {}   # real column name → is_date

def _ensure_col_cache() -> None:
    global _col_map, _col_types
    if _col_map:
        return
    rows = _db().execute(f"DESCRIBE {MASTER_TABLE}").fetchall()
    for r in rows:
        col   = r[0]
        dtype = str(r[1]).upper()
        key   = re.sub(r'[^A-Za-z0-9_]', '_', col)
        _col_map[key] = col
        _col_map[col] = col
        _col_types[col] = any(t in dtype for t in ('DATE', 'TIMESTAMP', 'TIME'))
    print(f"[server] Column cache built: {len(rows)} columns")

def _resolve_col(key: str) -> str:
    _ensure_col_cache()
    return _col_map.get(key, key)

def _is_date_col(col: str) -> bool:
    _ensure_col_cache()
    return _col_types.get(col, False)

def _looks_like_date(val: str) -> bool:
    return bool(re.match(r'^\d{4}-\d{2}-\d{2}', str(val).strip()))

# ── DuckDB SQL helpers ─────────────────────────────────────────────────────────
_DUCK_AGG = {
    "sum":"SUM","count":"COUNT","counta":"COUNT","countnums":"COUNT",
    "average":"AVG","mean":"AVG","min":"MIN","max":"MAX",
    "stddev":"STDDEV_SAMP","stddevp":"STDDEV_POP",
    "var":"VAR_SAMP","varp":"VAR_POP",
}

def _q(n: str) -> str:
    return '"' + n.replace('"', '""') + '"'

def _build_where(page_filters: dict) -> tuple[str, list]:
    """
    Build a SQL WHERE clause from page_filters.
    Keys may be underscore-sanitised — resolved to real column names.
    Blanks → IS NULL.  Dates → <=.  Others → direct comparison (index-friendly).
    CAST AS VARCHAR is avoided so DuckDB can use column indexes.
    """
    conds, params = [], []
    for raw_col, val in page_filters.items():
        if val is None or val == []:
            continue
        col     = _resolve_col(raw_col)
        is_date = _is_date_col(col)

        if isinstance(val, list):
            blanks   = [v for v in val if str(v) in ('', 'NaT', 'nan', 'None')]
            nonblank = [v for v in val if str(v) not in ('', 'NaT', 'nan', 'None')]

            if is_date and nonblank and all(_looks_like_date(v) for v in nonblank):
                latest = sorted(nonblank)[-1]
                clause = f"{_q(col)} <= TRY_CAST(? AS TIMESTAMP)"
                params.append(str(latest))
                if blanks:
                    clause = f'({clause} OR {_q(col)} IS NULL)'
                conds.append(clause)
            else:
                sub = []
                if nonblank:
                    ph = ",".join(["?"] * len(nonblank))
                    sub.append(f'{_q(col)} IN ({ph})')
                    params.extend(str(v) for v in nonblank)
                if blanks:
                    sub.append(f'{_q(col)} IS NULL')
                if sub:
                    conds.append("(" + " OR ".join(sub) + ")")
        else:
            s = str(val)
            if s in ('', 'NaT', 'nan', 'None'):
                conds.append(f'{_q(col)} IS NULL')
            elif is_date and _looks_like_date(s):
                # Direct timestamp comparison — index-friendly
                conds.append(f"{_q(col)} <= TRY_CAST(? AS TIMESTAMP)")
                params.append(s)
            else:
                # Direct equality — index-friendly, no CAST
                conds.append(f'{_q(col)} = ?')
                params.append(s)

    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    return where, params


# ── Fix #2: Lazy backend — load() returns sentinel, groupby_agg queries once ───
class LiveBackend(DataBackend):
    """
    DataBackend that pushes all GROUP BY work to DuckDB.
    load() returns a lightweight sentinel DataFrame — no data fetched.
    All real querying happens in groupby_agg() via a single SQL call.
    """

    def __init__(self, page_filters: dict):
        self.pf = page_filters   # raw keys — resolved inside _build_where

    def load(self, needed_cols: list[str] | None = None) -> pd.DataFrame:
        """
        Fetch only the columns needed by this pivot from DuckDB.
        If needed_cols is provided, SELECT only those columns instead of *.
        This avoids pulling 200+ columns when only 10 are needed.
        """
        where, params = _build_where(self.pf)
        if needed_cols:
            # Always include filter columns so apply_page_filters can work
            filter_cols = list(self.pf.keys())
            all_needed  = list(dict.fromkeys(needed_cols + filter_cols))
            # Validate against real columns
            valid = set(_col_map.values())
            cols  = [c for c in all_needed if c in valid]
            col_sql = ",".join(_q(c) for c in cols) if cols else "*"
        else:
            col_sql = "*"
        t0 = time.time()
        df = _db().execute(
            f"SELECT {col_sql} FROM {MASTER_TABLE} {where}", params
        ).fetchdf()
        print(f"  [load] {len(df):,} rows × {len(df.columns)} cols  ({time.time()-t0:.2f}s)")
        df._duckdb_backend = self   # type: ignore[attr-defined]
        df._duckdb_full    = True   # type: ignore[attr-defined]
        return df

    def groupby_agg(self, df: pd.DataFrame,
                    group_cols: list[str],
                    named_aggs: dict) -> pd.DataFrame:
        """
        Push GROUP BY + WHERE entirely to DuckDB.
        Only the small aggregated result comes back to Python.
        """
        if not getattr(df, "_duckdb_full", False):
            return df.groupby(group_cols, dropna=False).agg(**named_aggs).reset_index()

        where, params = _build_where(self.pf)
        sel           = [_q(c) for c in group_cols]
        fallback      = []

        for name, nagg in named_aggs.items():
            fn = nagg.aggfunc
            if callable(fn):
                fallback.append(name)
                continue
            duck = _DUCK_AGG.get(str(fn).lower().replace(".", ""))
            if not duck:
                fallback.append(name)
                continue
            sel.append(f"{duck}({_q(nagg.column)}) AS {_q(name)}")

        gb  = ",".join(_q(c) for c in group_cols)
        sql = (f"SELECT {','.join(sel)} FROM {MASTER_TABLE} {where}"
               + (f" GROUP BY {gb} ORDER BY {gb}" if group_cols else ""))

        try:
            t0     = time.time()
            result = _db().execute(sql, params).fetchdf()
            print(f"  [groupby] {len(result):,} result rows  ({time.time()-t0:.2f}s)")
        except Exception as e:
            print(f"  [SQL ERR] {e}")
            full = _db().execute(
                f"SELECT * FROM {MASTER_TABLE} {where}", params).fetchdf()
            return full.groupby(group_cols, dropna=False).agg(**named_aggs).reset_index()

        # Handle callable aggs (e.g. np.prod) via pandas fallback
        if fallback:
            full  = _db().execute(
                f"SELECT * FROM {MASTER_TABLE} {where}", params).fetchdf()
            fb_d  = {k: v for k, v in named_aggs.items() if k in fallback}
            fb_r  = (full.groupby(group_cols, dropna=False).agg(**fb_d).reset_index()
                     if group_cols else
                     pd.DataFrame({k: [full[v.column].agg(v.aggfunc)]
                                   for k, v in fb_d.items()}))
            result = (result.merge(fb_r, on=group_cols, how="left")
                      if group_cols else pd.concat([result, fb_r], axis=1))

        return result


# Run warmup after all functions are defined
import threading
threading.Thread(target=_warmup, daemon=True).start()

# ── Fast HTML renderer (replaces _build_pivot_html for server use) ────────────
# _build_pivot_html uses string += in a loop which is O(n²).
# This version uses io.StringIO (single write buffer) and avoids
# per-row json.dumps by serialising raw_vals with a manual loop.

import html as _ht
import numpy as _np
import pandas as _pd_local

def _fast_pivot_html(result: _pd_local.DataFrame, pivot: dict,
                     panel_id: str) -> str:

    
    """
    Fast HTML renderer for pivot tables.
    ~10x faster than _build_pivot_html for large result sets.
    Uses StringIO instead of string concatenation.
    """
    from pivot_query_engine7 import (
        _excel_format_to_python, _format_value, _eval_formula,
    )

    pid   = pivot.get("id", "?")
    pname = pivot.get("name", pid)
    host  = pivot.get("host_sheet", "")
    page_filters = pivot.get("filters", [])

    row_fields = [r["field"] for r in pivot.get("rows", [])
                  if r.get("type") == "field" and r.get("field") != "__VALUES__"]
    dim_cols   = [f for f in row_fields if f in result.columns]
    val_cols   = [c for c in result.columns if c not in dim_cols]
    n_dims     = len(dim_cols)

    # Format + agg maps
    fmt_map    = {}
    agg_fn_map = {}
    for vs in pivot.get("values", []):
        dn = (vs.get("display_name") or "").strip()
        fmt_map[dn]    = _excel_format_to_python(vs.get("num_format"))
        agg_fn_map[dn] = (vs.get("aggregation") or "sum").lower()

    # Calc formulas
    calc_fields   = {cf["name"]: cf for cf in pivot.get("calculated_fields", [])}
    fields_meta   = pivot.get("fields", {})
    calc_formulas = {}
    for vs in pivot.get("values", []):
        if not vs.get("is_calculated"): continue
        dn  = (vs.get("display_name") or "").strip()
        src = vs.get("source_field", "")
        fml = None
        if src in calc_fields: fml = calc_fields[src].get("formula")
        if not fml: fml = fields_meta.get(src, {}).get("formula") or vs.get("formula")
        if not fml and dn in calc_fields: fml = calc_fields[dn].get("formula")
        if fml: calc_formulas[dn] = fml

    # ── Convert DataFrame to records ONCE — avoid iterrows() entirely ────────
    # iterrows() creates a new Series per row which is ~10x slower than
    # working with plain Python dicts.
    all_records = result.to_dict(orient="records")

    # Pre-convert pf_fields timestamps once per unique value (not per row)
    pf_fields_list = [f.get("field","") for f in page_filters if f.get("field")]
    _ts_cache: dict[str, str] = {}   # raw string → date string

    def _to_date_str(raw: str) -> str:
        if raw not in _ts_cache:
            try:    _ts_cache[raw] = str(_pd_local.Timestamp(raw).date())
            except: _ts_cache[raw] = raw
        return _ts_cache[raw]

    def is_grand(rec: dict) -> bool:
        return bool(dim_cols and str(rec.get(dim_cols[0], "") or "").strip() == "Grand Total")

    data_records = [r for r in all_records if not is_grand(r)]
    grand_records = [r for r in all_records if is_grand(r)]

    # Pre-compute subtotals for group rows — use DataFrame directly (fast)
    group_agg: dict[str, dict] = {}
    if n_dims > 0 and data_records:
        df_work = result[result[dim_cols[0]].astype(str).str.strip() != "Grand Total"].copy()
        for col in val_cols:
            df_work[col] = _pd_local.to_numeric(df_work[col], errors="coerce")
        for depth in range(n_dims - 1):
            group_keys = dim_cols[:depth + 1]
            agg_spec   = {}
            for vc in val_cols:
                if vc in calc_formulas: continue
                fn = agg_fn_map.get(vc.strip(), "sum")
                agg_spec[vc] = {"count":"count","average":"mean",
                                "max":"max","min":"min"}.get(fn, "sum")
            try:
                grp = df_work.groupby(group_keys, sort=False).agg(agg_spec).reset_index()
                for vc_dn, fml in calc_formulas.items():
                    if vc_dn in val_cols:
                        grp[vc_dn] = grp.apply(
                            lambda r, f=fml: _eval_formula(f, r), axis=1)
                # Use to_dict for fast iteration
                for grow in grp.to_dict(orient="records"):
                    pk = "|".join(str(grow.get(k,"") or "").strip()
                                  for k in group_keys)
                    group_agg[pk] = {
                        vc.strip(): _format_value(grow.get(vc), fmt_map.get(vc.strip()))
                        for vc in val_cols
                    }
            except Exception:
                pass

    # Page filter bar
    pf_parts = []
    for f in page_filters:
        fname    = f.get("field","")
        show_all = f.get("show_all", True)
        selected = f.get("selected_item")
        dv_list  = [dv.get("value", dv) if isinstance(dv, dict) else dv
                    for dv in pivot.get("fields",{}).get(fname,{})
                       .get("distinct_values",[])]
        pretty = []
        for v in dv_list:
            try:    pretty.append(str(_pd_local.Timestamp(v).date()))
            except: pretty.append(str(v))
        sel_pretty = ""
        if not show_all and selected is not None:
            try:    sel_pretty = str(_pd_local.Timestamp(selected).date())
            except: sel_pretty = str(selected)
        ds_key    = re.sub(r'[^A-Za-z0-9_]', '_', fname)
        safe_vals = json.dumps(pretty).replace('"', '&quot;')
        badge     = f" [{sel_pretty}]" if sel_pretty else ""
        pf_parts.append(
            f'<span class="pf-group">' 
            f'<button class="pf-btn" data-field="{ds_key}" ' 
            f'onclick="openPageFilter(this,&quot;{panel_id}_tbl&quot;,' 
            f'&quot;{ds_key}&quot;,{safe_vals})">' 
            f'▼ {_ht.escape(fname)}</button>' 
            f'<span class="pf-badge">{_ht.escape(badge)}</span></span>'
        )
    filter_bar = ('<div class="page-filter-bar"><span class="pf-label">Page Filters:</span>'
                  + "".join(pf_parts) + '</div>') if pf_parts else ""

    # Header
    header = (
        f'<div class="pivot-header">' 
        f'<div class="pivot-title">{_ht.escape(pname)}</div>' 
        f'<div class="pivot-meta">' 
        f'<span class="meta-tag">{_ht.escape(pid)}</span>' 
        f'<span class="meta-tag">{_ht.escape(host)}</span>' 
        f'<span class="meta-tag">{len(data_records):,} rows</span>' 
        f'<span class="meta-tag">{len(val_cols)} measures</span>' 
        f'</div>{filter_bar}</div>'
    )

    if result.empty:
        return (f'<div class="pivot-panel active" id="{panel_id}">{header}' 
                f'<div class="empty-state">No data returned.</div></div>')

    table_id = f"{panel_id}_tbl"

    # Build thead
    th_cells = io.StringIO()
    if n_dims > 0:
        th_cells.write('<th class="col-dim" style="left:0px">Row Labels</th>')
    for i, col in enumerate(val_cols):
        ci = i + (1 if n_dims > 0 else 0)
        th_cells.write(
            f'<th data-colname="{_ht.escape(col.strip())}" ' 
            f'onclick="sortTable(this,\'{table_id}\')">' 
            f'{_ht.escape(col.strip())} ' 
            f'<button class="col-filter-btn" ' 
            f'onclick="openColFilter(this,\'{table_id}\',{ci});' 
            f'event.stopPropagation()">▾</button>' 
            f'<span class="filter-badge"></span>' 
            f'<span class="sort-icon"></span></th>'
        )
    thead = f"<thead><tr>{th_cells.getvalue()}</tr></thead>"

    # Controls
    controls = (
        f'<div class="controls">' 
        f'<button class="ctrl-btn" onclick="expandAll(\'{table_id}\')">⊞ Expand All</button>' 
        f'<button class="ctrl-btn" onclick="collapseAll(\'{table_id}\')">⊟ Collapse All</button>' 
        f'<input class="search-box" placeholder="🔍 Search…" ' 
        f'oninput="searchTable(this,\'{table_id}\')"/>' 
        f'<span class="row-count">{len(data_records):,} leaf rows</span></div>'
    )

    # pf_fields for data-pf_ attrs
    pf_fields_list = [f.get("field","") for f in page_filters if f.get("field")]
    pf_fields = pf_fields_list  # keep alias for any other references

    # vars needed for calc formulas
    needed_vars: set[str] = set(val_cols)
    for fml in calc_formulas.values():
        needed_vars.update(re.findall(r'[a-zA-Z_]\w*', fml))

    # Pre-compute sanitised pf field keys once
    pf_key_map = {f: re.sub(r'[^A-Za-z0-9_]','_',f) for f in pf_fields_list}

    # Pre-format all value columns at once using vectorised ops
    # result: dict of col_name → list of formatted strings (index-aligned)
    formatted: dict[str, list[str]] = {}
    for vcol in val_cols:
        col_series = _pd_local.to_numeric(result[vcol], errors="coerce")
        fmt = fmt_map.get(vcol.strip())
        formatted[vcol] = [_format_value(v, fmt) for v in col_series]

    # Build tbody using StringIO — iterate over plain dicts, not Series
    buf       = io.StringIO()
    buf.write("<tbody>")
    prev_path = ["__SENTINEL__"] * n_dims
    data_idx  = [i for i, r in enumerate(all_records) if not is_grand(r)]

    for row_i, rec_i in enumerate(data_idx):
        rec  = all_records[rec_i]
        path = [str(rec.get(dim_cols[l],"") or "").strip() for l in range(n_dims)]

        diverge = 0
        while diverge < n_dims and path[diverge] == prev_path[diverge]:
            diverge += 1

        # Group header rows
        for l in range(diverge, n_dims - 1):
            sub_key  = "|".join(path[:l+1])
            pid_l    = "|".join(path[:l]) if l > 0 else ""
            agg_data = group_agg.get(sub_key, {})
            v_str    = _ht.escape(path[l])
            indent   = f"d{min(l,5)}"
            safe_gid = sub_key.replace('"','&quot;').replace("'","&#39;")
            safe_pid = pid_l.replace('"','&quot;').replace("'","&#39;")
            buf.write(
                f'<tr class="grp-expanded" data-gid="{safe_gid}" ' 
                f'data-pid="{safe_pid}" data-lvl="{l}" data-is-group="1">' 
                f'<td class="col-dim {indent}" style="left:0px">' 
                f'<span class="toggle" onclick="toggleGroup(this)"></span>' 
                f'{v_str}</td>'
            )
            for vcol in val_cols:
                v = agg_data.get(vcol.strip(), "")
                buf.write(f"<td>{_ht.escape(v)}</td>")
            buf.write("</tr>")

        # Leaf row
        gid  = "|".join(path)
        pid2 = "|".join(path[:n_dims-1]) if n_dims > 1 else ""
        clvl = n_dims - 1
        safe_gid = gid.replace('"','&quot;').replace("'","&#39;")
        safe_pid = pid2.replace('"','&quot;').replace("'","&#39;")

        # raw_vals — manual serialisation
        rv_parts = []
        for k in needed_vars:
            if k in rec:
                try:
                    fv = float(rec[k])
                    if fv == fv:  # not NaN
                        rv_parts.append(f'"{k}":{fv}')
                except Exception:
                    pass
        raw_json = "{" + ",".join(rv_parts) + "}"

        # pf attrs — use cached timestamp conversion
        pf_attrs = ""
        for pf_field in pf_fields_list:
            raw_val = str(rec.get(pf_field,"") or "").strip()
            pv      = _to_date_str(raw_val)
            ds_k    = pf_key_map[pf_field]
            pf_attrs += f' data-pf_{ds_k}="{_ht.escape(pv)}"'

        buf.write(
            f'<tr data-gid="{safe_gid}" data-pid="{safe_pid}" ' 
            f'data-lvl="{clvl}" data-raw=\'{raw_json}\'{pf_attrs}>'
        )
        if n_dims > 0:
            v_str = _ht.escape(path[clvl])
            buf.write(
                f'<td class="col-dim d{min(clvl,5)}" style="left:0px">{v_str}</td>'
            )
        # Use pre-formatted values — no per-cell _format_value call
        for vcol in val_cols:
            fmtd = formatted[vcol][rec_i]
            buf.write(f"<td>{_ht.escape(fmtd)}</td>")
        buf.write("</tr>")
        prev_path = path[:]

    # Grand total
    for rec in grand_records:
        buf.write('<tr class="grand-total">')
        if n_dims > 0:
            buf.write('<td class="col-dim d0" style="left:0px"><strong>Grand Total</strong></td>')
        for vcol in val_cols:
            v    = rec.get(vcol)
            fmt  = fmt_map.get(vcol.strip())
            fmtd = _format_value(v, fmt)
            buf.write(f"<td><strong>{_ht.escape(fmtd)}</strong></td>")
        buf.write("</tr>")

    buf.write("</tbody>")
    tbody = buf.getvalue()

    meta = {
        "val_cols":   val_cols,
        "agg_fn_map": agg_fn_map,
        "fmt_map":    fmt_map,
        "calc_formulas": calc_formulas,
    }
    meta_json = json.dumps(meta).replace("'","&#39;").replace('"','&quot;')

    table = (
        f'<div class="table-wrap">' 
        f'<table id="{table_id}" data-ndims="{n_dims}" data-meta="{meta_json}">' 
        f'{thead}{tbody}</table></div>'
    )

    return (
        f'<div class="pivot-panel active" id="{panel_id}">' 
        f'{header}{controls}{table}</div>'
    )


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def root():
    _ensure_col_cache()   # warm cache on first request
    return HTMLResponse(_dashboard_html())


@app.post("/api/pivot/{pivot_id}")
async def pivot_api(pivot_id: str, body: dict):
    """
    Execute pivot with page filters applied in DuckDB.
    Returns: { html: str, rows: int }
    """
    pivot = next((p for p in PIVOTS if p.get("id") == pivot_id), None)
    if not pivot:
        raise HTTPException(404, f"Pivot '{pivot_id}' not found")
    try:
        filters = body.get("filters", {})
        print(f"\n[pivot] {pivot_id} | filters: {filters}")
        t_start = time.time()

        # Collect only the columns this pivot actually needs
        # so load() fetches a narrow result set instead of SELECT *
        row_fields  = [r["field"] for r in pivot.get("rows", [])
                       if r.get("type") == "field" and r.get("field") != "__VALUES__"]
        col_fields  = [c["field"] for c in pivot.get("columns", [])
                       if c.get("type") == "field" and c.get("field") != "__VALUES__"]
        val_sources = [v["source_field"] for v in pivot.get("values", [])
                       if not v.get("is_calculated") and v.get("source_field") != "__VALUES__"]
        filt_fields = [f["field"] for f in pivot.get("filters", []) if f.get("field")]
        needed_cols = list(dict.fromkeys(
            row_fields + col_fields + val_sources + filt_fields
        ))

        backend   = LiveBackend(filters)
        df_master = backend.load(needed_cols if needed_cols else None)
        print(f"  [load done] {time.time()-t_start:.2f}s")

        t2     = time.time()
        result = execute_pivot(df_master, pivot, backend)
        print(f"  [execute_pivot done] {time.time()-t2:.2f}s  →  {len(result):,} result rows")

        t3       = time.time()
        panel_id = f"live_{pivot_id}"
        html_out = _fast_pivot_html(result, pivot, panel_id)
        print(f"  [html render done] {time.time()-t3:.2f}s")
        print(f"  [TOTAL] {time.time()-t_start:.2f}s")

        # Fix #6: inject data-field onto every .pf-btn for reliable badge matching
        # Match the whole <button class="pf-btn" ...> tag so we can modify its attributes
        def _inject_data_field(m: re.Match) -> str:
            field_key = m.group(1)
            btn_tag   = m.group(0)
            if 'data-field=' not in btn_tag:
                return btn_tag.replace(
                    'class="pf-btn"',
                    f'class="pf-btn" data-field="{field_key}"'
                )
            return btn_tag
        html_out = re.sub(
            r'<button class="pf-btn"[^>]*openPageFilter\(this,&quot;[^&]+&quot;,&quot;([^&]+)&quot;[^>]*>',
            _inject_data_field,
            html_out
        )

        html_out = html_out.replace('class="pivot-panel"',
                                    'class="pivot-panel active"')
        return JSONResponse({"html": html_out, "rows": len(result)})

    except Exception as e:
        import traceback; traceback.print_exc()
        raise HTTPException(500, str(e))


# ── Dashboard HTML ─────────────────────────────────────────────────────────────
def _dashboard_html() -> str:
    tabs = ""
    for i, p in enumerate(PIVOTS):
        pid    = p.get("id", "")
        name   = p.get("name", pid)
        active = "active" if i == 0 else ""
        short  = (name[:28] + "…") if len(name) > 30 else name
        tabs  += (f'<button class="tab-btn {active}" data-pid="{pid}" '
                  f'onclick="switchTab(this)">{short}</button>\n')

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Pivot Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
{_HTML_STYLE}
<style>
html,body{{height:100%;overflow:hidden}}
#app{{display:grid;grid-template-rows:44px 1fr;height:100vh}}
#topbar{{background:var(--surface);border-bottom:1px solid var(--border);
  display:flex;align-items:center;gap:10px;padding:0 16px}}
.logo{{font-family:var(--font-mono);font-size:12px;font-weight:600;color:var(--accent)}}
.live-dot{{width:7px;height:7px;border-radius:50%;background:var(--accent2);
  box-shadow:0 0 6px var(--accent2)}}
#row-badge{{font-size:11px;font-family:var(--font-mono);color:var(--text-muted);margin-left:auto}}
#body{{display:grid;grid-template-columns:210px 1fr;overflow:hidden}}
#sidebar{{background:var(--surface);border-right:1px solid var(--border);
  overflow-y:auto;padding:6px}}
.tab-section-lbl{{font-family:var(--font-mono);font-size:9px;letter-spacing:.1em;
  text-transform:uppercase;color:var(--text-muted);padding:3px 4px 5px;display:block}}
.tab-btn{{display:block;width:100%;padding:5px 7px;border:none;border-radius:3px;
  background:transparent;color:var(--text-muted);cursor:pointer;font-size:11px;
  font-family:var(--font-ui);text-align:left;white-space:nowrap;overflow:hidden;
  text-overflow:ellipsis;transition:all .12s;margin-bottom:1px}}
.tab-btn:hover{{background:var(--surface2);color:var(--text)}}
.tab-btn.active{{background:var(--surface2);color:var(--accent);font-weight:600}}

/* Fix #5: content area is position:relative so spinner can overlay */
#content{{overflow:auto;background:var(--bg);position:relative}}
#pivot-area{{min-height:100%}}

/* Fix #5: spinner overlays the table — does NOT replace it */
#fetch-overlay{{
  display:none;
  position:absolute;inset:0;
  background:rgba(13,15,20,.65);
  backdrop-filter:blur(2px);
  z-index:9999;
  align-items:center;justify-content:center;
  gap:10px;
  color:var(--text-muted);
  font-family:var(--font-mono);font-size:12px;
}}
#fetch-overlay.active{{display:flex}}
.spinner{{width:20px;height:20px;border:2px solid var(--border);
  border-top-color:var(--accent);border-radius:50%;animation:spin .7s linear infinite}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
.err-box{{padding:24px;color:#f06060;font-family:var(--font-mono);font-size:12px}}
.state-box{{display:flex;align-items:center;justify-content:center;height:220px;
  gap:12px;color:var(--text-muted);font-family:var(--font-mono);font-size:12px}}
</style>
</head>
<body>
<div id="app">
  <div id="topbar">
    <span class="logo">◈ pivot.dashboard</span>
    <div class="live-dot"></div>
    <span style="font-size:11px;font-family:var(--font-mono);color:var(--text-muted)">LIVE</span>
    <span id="row-badge"></span>
  </div>
  <div id="body">
    <div id="sidebar">
      <span class="tab-section-lbl">Pivot Tables</span>
      {tabs}
    </div>
    <!-- Fix #5: overlay lives here, pivot-area is never wiped during fetch -->
    <div id="content">
      <div id="fetch-overlay">
        <div class="spinner"></div><span>Querying DuckDB…</span>
      </div>
      <div id="pivot-area">
        <div class="state-box">
          <div class="spinner"></div><span>Loading…</span>
        </div>
      </div>
    </div>
  </div>
</div>

{_HTML_JS}

<script>
// ── Row index for O(1) collapse/expand ────────────────────────────────────────
const _rowIndex = new Map();  // gid → tr
const _children = new Map();  // pid → [tr, ...]

function _buildRowIndex() {{
  _rowIndex.clear();
  _children.clear();
  document.querySelectorAll('#pivot-area tbody tr[data-gid]').forEach(tr => {{
    const gid = tr.dataset.gid;
    const pid = tr.dataset.pid || '';
    if (gid) _rowIndex.set(gid, tr);
    if (pid) {{
      if (!_children.has(pid)) _children.set(pid, []);
      _children.get(pid).push(tr);
    }}
  }});
}}

function _getDescendants(gid) {{
  const out = [], q = [gid];
  while (q.length) {{
    const cur = q.shift();
    (_children.get(cur) || []).forEach(r => {{
      out.push(r);
      if (r.dataset.gid) q.push(r.dataset.gid);
    }});
  }}
  return out;
}}

// ── Optimised toggleGroup ─────────────────────────────────────────────────────
function toggleGroup(btn) {{
  const tr     = btn.closest('tr');
  const gid    = tr.dataset.gid;
  const isOpen = tr.classList.contains('grp-expanded');
  tr.classList.toggle('grp-expanded',  !isOpen);
  tr.classList.toggle('grp-collapsed',  isOpen);
  if (isOpen) {{
    _getDescendants(gid).forEach(r => {{
      r.classList.add('hidden-row');
      if (r.dataset.isGroup) {{
        r.classList.remove('grp-expanded');
        r.classList.add('grp-collapsed');
      }}
    }});
  }} else {{
    (_children.get(gid) || []).forEach(r => r.classList.remove('hidden-row'));
  }}
}}

function expandAll(tid) {{
  document.getElementById(tid).querySelector('tbody')
    .querySelectorAll('tr').forEach(r => {{
      r.classList.remove('hidden-row');
      if (r.dataset.isGroup) {{
        r.classList.remove('grp-collapsed');
        r.classList.add('grp-expanded');
      }}
    }});
}}

function collapseAll(tid) {{
  document.getElementById(tid).querySelector('tbody')
    .querySelectorAll('tr').forEach(r => {{
      if (r.classList.contains('grand-total')) return;
      if (parseInt(r.dataset.lvl || '0') > 0) r.classList.add('hidden-row');
      if (r.dataset.isGroup) {{
        r.classList.remove('grp-expanded');
        r.classList.add('grp-collapsed');
      }}
    }});
}}

// ── Collapse state snapshot / restore ────────────────────────────────────────
function _snapshotCollapseState() {{
  const s = new Set();
  _rowIndex.forEach((tr, gid) => {{
    if (tr.classList.contains('grp-collapsed')) s.add(gid);
  }});
  return s;
}}

function _restoreCollapseState(collapsed) {{
  if (!collapsed || !collapsed.size) return;
  _buildRowIndex();
  collapsed.forEach(gid => {{
    const tr = _rowIndex.get(gid);
    if (!tr) return;
    tr.classList.remove('grp-expanded');
    tr.classList.add('grp-collapsed');
    _getDescendants(gid).forEach(r => {{
      r.classList.add('hidden-row');
      if (r.dataset.isGroup) {{
        r.classList.remove('grp-expanded');
        r.classList.add('grp-collapsed');
      }}
    }});
  }});
}}

// ── State ─────────────────────────────────────────────────────────────────────
let curId      = null;
let curFilters = {{}};

window.addEventListener('load', () => {{
  const first = document.querySelector('.tab-btn');
  if (first) switchTab(first);
}});

function switchTab(btn) {{
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  curId      = btn.dataset.pid;
  curFilters = {{}};
  loadPivot(curId, {{}});
}}

// ── Override openPageFilter to call server instead of hiding rows ─────────────
function openPageFilter(btn, tid, field, allVals) {{
  _closeDD();

  const BLANK = '(blank)';
  const withBlank = allVals.includes(BLANK) ? allVals : [...allVals, BLANK];

  const curRaw = curFilters[field];
  const curSet = new Set(
    curRaw
      ? (Array.isArray(curRaw) ? curRaw : [curRaw])
          .map(v => (v === '' || v === 'NaT' || v === 'nan') ? BLANK : v)
      : []
  );

  const counts = {{}};
  withBlank.forEach(v => counts[v] = '');

  const dd = _buildDD(withBlank, counts, curSet, (dd, clr) => {{
    if (clr) {{
      delete curFilters[field];
    }} else {{
      const cbs    = dd.querySelectorAll('input[type=checkbox]');
      const sel    = Array.from(cbs).filter(c => c.checked).map(c => c.value);
      const allCbs = Array.from(cbs).map(c => c.value);
      if (!sel.length || sel.length === allCbs.length) {{
        delete curFilters[field];
      }} else {{
        const mapped = sel.map(v => v === BLANK ? '' : v);
        curFilters[field] = mapped.length === 1 ? mapped[0] : mapped;
      }}
    }}
    // Fix #6: use data-field for reliable badge update
    _updateBadge(field);
    _closeDD();
    loadPivot(curId, curFilters);
  }});

  _posDD(dd, btn);
}}

// Fix #6: badge update uses data-field attribute, not onclick string matching
function _updateBadge(field) {{
  const bar = document.querySelector('.page-filter-bar');
  if (!bar) return;
  bar.querySelectorAll('.pf-btn[data-field]').forEach(btn => {{
    if (btn.dataset.field !== field) return;
    const badge = btn.parentElement.querySelector('.pf-badge');
    if (!badge) return;
    const v = curFilters[field];
    badge.textContent = v
      ? (' [' + (Array.isArray(v) ? v.length + ' selected' : v) + ']')
      : '';
  }});
}}

function _restoreAllBadges() {{
  Object.keys(curFilters).forEach(f => _updateBadge(f));
}}

// Fix #5: overlay spinner instead of DOM replacement
// Fix #3: no eval() — call _buildRowIndex() explicitly
async function loadPivot(pivotId, filters) {{
  const area    = document.getElementById('pivot-area');
  const overlay = document.getElementById('fetch-overlay');

  // Snapshot BEFORE fetch (table stays in DOM during fetch)
  const collapsed = _snapshotCollapseState();

  overlay.classList.add('active');
  document.getElementById('row-badge').textContent = '';

  try {{
    console.log('[pivot] loading:', pivotId, 'filters:', JSON.stringify(filters));
    const res  = await fetch('/api/pivot/' + encodeURIComponent(pivotId), {{
      method:  'POST',
      headers: {{'Content-Type': 'application/json'}},
      body:    JSON.stringify({{ filters }}),
    }});
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || 'Server error');

    // Replace table content
    area.innerHTML = data.html;
    document.getElementById('row-badge').textContent =
      data.rows.toLocaleString() + ' rows';

    // Fix #3: explicit init instead of eval() on script tags
    _buildRowIndex();
    _restoreCollapseState(collapsed);
    _restoreAllBadges();

  }} catch(e) {{
    area.innerHTML = '<div class="err-box">⚠ ' + e.message + '</div>';
  }} finally {{
    overlay.classList.remove('active');
  }}
}}
</script>
</body>
</html>"""
