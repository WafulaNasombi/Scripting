# pivot_query_engine.py — Full Reference

## Overview

`pivot_query_engine.py` reads a **pivot-extractor JSON file** (produced by `pivot_extractor.py`) and an **Excel master data workbook**, executes each pivot definition as a real GROUP BY query, and renders the results as **self-contained interactive HTML dashboards** — no server, no external libraries, just open the file in a browser.

The script is the bridge between:

```
pivot_extractor.py  →  pivots.json  +  master_sheet.xlsx
                                              ↓
                              pivot_query_engine.py
                                              ↓
                              dashboard.html  (or one .html per pivot)
```

---

## Quick Start

```bash
pip install pandas openpyxl numpy

# See what pivots are in the JSON
python pivot_query_engine.py data.xlsx --json pivots.json --list-pivots

# Run one pivot → one HTML
python pivot_query_engine.py data.xlsx --json pivots.json --pivot-id PT_PRESALES_CHANNEL_1

# Run all pivots → one HTML per pivot
python pivot_query_engine.py data.xlsx --json pivots.json

# Run all pivots → single combined HTML with tab navigation
python pivot_query_engine.py data.xlsx --json pivots.json --combined

# Override sheet / header row detected from JSON
python pivot_query_engine.py data.xlsx --json pivots.json --sheet "Sales Data Dump" --header-row 2
```

---

## CLI Arguments

| Argument | Type | Default | Description |
|---|---|---|---|
| `xls_file` | positional | — | Path to the Excel workbook containing the master data sheet |
| `--json` | string | auto-detect alongside xls | Path to the pivot extractor JSON |
| `--sheet` | string | from JSON `meta.master_sheet` | Master worksheet name to load |
| `--header-row` | integer | from JSON `meta.header_row` | 1-based row number containing column headers |
| `--pivot-id` | string | run all | Run only the named pivot ID |
| `--output` | string | `<pivot_id>.html` or `dashboard.html` | Output file path |
| `--combined` | flag | false | Write all pivots into one HTML file with tab navigation |
| `--list-pivots` | flag | false | Print all available pivot IDs and exit |
| `--backend` | `excel` \| `duckdb` | `excel` | Data backend (see Backend section) |

Sheet name and header row are resolved in priority order: **CLI args → `master_sheet_info` block → `meta` block**.

---

## Architecture

```
pivot_query_engine.py
│
├── DataBackend (abstract)
│   ├── ExcelBackend        ← current: pandas + openpyxl
│   └── DuckDBBackend       ← future: drop-in replacement (Step 2)
│
├── execute_pivot()         ← main query engine, 9-step pipeline
│   ├── apply_page_filters()
│   ├── apply_hidden_items()
│   ├── groupby_agg()       ← delegated to backend (overridable for SQL push-down)
│   ├── apply_show_data_as()
│   ├── _eval_formula()     ← calculated fields
│   └── apply_pivot_filters()
│
└── HTML renderer
    ├── _build_pivot_html() ← one panel per pivot
    └── build_html_dashboard() ← assembles tabs + shared JS/CSS
```

---

## Python Pipeline — Step by Step

### 1. JSON Loading

The JSON file is read in full. If the file is truncated (contains the `"............."` placeholder produced when `pivot_extractor.py` runs on a partial workbook), the script recovers gracefully:

```
[loader] WARNING: JSON is truncated at char 265573 – only partial pivots available
```

It closes the JSON array/object at the truncation point and continues. Any pivot entry that contains an `"error"` key is skipped silently.

### 2. ExcelBackend — Loading the Master Sheet

```python
backend = ExcelBackend(xls_path, sheet_name, header_row)
df_master = backend.load()
```

`pd.read_excel` is called once with `engine="openpyxl"`. The `header_row` is converted from 1-based (JSON convention) to 0-based (pandas `header=` argument). The entire sheet is loaded into memory as a DataFrame — all subsequent pivots run in-memory against this single copy.

### 3. execute_pivot() — The 9-Step Pipeline

Each pivot definition is passed through this pipeline:

#### Step 1 — Page Filters (`apply_page_filters`)

Applies `pivot["filters"]` — these are the axisPage (slicer) fields shown above the pivot in Excel.

**Filter resolution waterfall** (tries each in order, stops at first match):

| Priority | Condition | Action |
|---|---|---|
| 0 | `show_all = true` | Skip — no filtering |
| 0 | `selected_item = null` | Skip |
| 0 | `selected_item = 0` on a text/object column | Skip — **zero sentinel** (extractor bug: stored index, not value) |
| 1 | Column dtype is `datetime64` | Strip timezone if tz-aware, compare `col.dt.normalize() == ts.normalize()`. Falls back to month-level match if no exact match. |
| 2 | Field metadata says `data_type: date` AND column is numeric | Treat column as **Excel serial integers** (e.g. `45839` = 2025-07-01). Converts target ISO string to serial. Falls back to month-range scan. |
| 3 | `selected_item` parses as a number | Numeric equality on coerced column |
| 4 | Fallback | String compare: `col.astype(str).strip() == str(selected).strip()` |

**The zero-sentinel rule** exists because some extractor versions store `selected_index = 0` (the first item in the dropdown, meaning "All") as `selected_item = 0`. If the actual column has no rows equal to zero, or is text-typed, this sentinel is ignored.

**The serial-number rule** exists because Excel stores dates as integers internally. When `pd.read_excel` reads a column with a custom number format (e.g. `mm-dd-yy`) but no explicit date type, pandas leaves the values as integers (e.g. `45839`). A naive `pd.to_datetime(45839)` interprets this as nanoseconds since Unix epoch — completely wrong. The fix converts the ISO `selected_item` string to its Excel serial equivalent and compares integers directly.

Diagnostic output for every filter attempt:
```
[filter] 'Month' = 2025-07-01  (serial=45839)  kept 150/300
[filter] DEBUG  'Month' dtype=datetime64[us] sample=['2025-07-01', ...] target=2025-07-01
[filter] SKIP  'OUTLET SYSTEM ACTIVE STATUS' = 0 – treated as (All) sentinel
[filter] WARN  'SomeField' = 'XYZ' – no matching rows (0/7,496)
```

#### Step 2 — Hidden Item Exclusions (`apply_hidden_items`)

Each `fields[name].hidden_items[]` list contains raw cache values that were unchecked in the pivot field list. The script excludes all rows whose dimension column value appears in the hidden list.

Both string and numeric comparisons are performed to handle type coercion (e.g. `"1.0"` matches `1`).

```
[hidden] 'region'  excluded 73 rows (1 hidden values)
```

#### Step 3 — Determine GROUP BY Dimensions

Row fields (`pivot["rows"]` where `type = "field"` and `field != "__VALUES__"`) become the GROUP BY keys. Column fields (`pivot["columns"]`) are added if they are real fields (not the `__VALUES__` sentinel). Any dimension column missing from the DataFrame is reported as a warning and skipped.

#### Step 4 — Build Aggregation Specs

Non-calculated value fields (`is_calculated = false`) are mapped to `(source_field, aggregation_function)` pairs. The aggregation key from the JSON is resolved via `_AGG_MAP`:

| JSON `aggregation` | pandas/numpy function |
|---|---|
| `sum` | `"sum"` |
| `count` / `counta` | `"count"` |
| `average` | `"mean"` |
| `min` / `max` | `"min"` / `"max"` |
| `product` | `np.prod` |
| `stddev` | `"std"` |
| `stddevp` | `lambda x: x.std(ddof=0)` |
| `var` | `"var"` |
| `varp` | `lambda x: x.var(ddof=0)` |

Multiple display names can map to the same source column (e.g. "RGB Sales" and "RGB Count" both from `rgbDeliveredQty`) — each gets a unique safe key internally.

Source columns are coerced to numeric before aggregation (`pd.to_numeric(errors="coerce")`).

#### Step 5 — Execute GROUP BY

If dimensions exist: `backend.groupby_agg(df, group_cols, named_aggs)` → `df.groupby(..., dropna=False).agg(**named_aggs).reset_index()`.

If no dimensions (summary pivot): a single-row DataFrame is built by calling the agg function directly on each source Series.

The `groupby_agg` method is the **DuckDB hook** — the future `DuckDBBackend` overrides it to emit `SELECT ... GROUP BY` SQL instead.

#### Step 6 — `show_data_as` Transforms

Applied per value field if `show_data_as != "normal"`:

| `show_data_as` | Transform |
|---|---|
| `normal` | No change |
| `percentoftotal` | `value / grand_total` |
| `percentofrow` | `value / row_sum` |
| `runtotal` | `cumsum()` |
| `percent` | `value / base_item_value` |
| `difference` | `value - base_item_value` |
| `percentdiff` | `(value - base) / base` |
| `rankascending` / `rankdescending` | `Series.rank()` |

#### Step 7 — Calculated Fields (`_eval_formula`)

Applied to value fields where `is_calculated = true`. Formula lookup cascade:
1. `calculated_fields[source_field].formula`
2. `fields[source_field].formula`
3. `values[n].formula` (inline)
4. `calculated_fields[display_name].formula`

The formula evaluator handles:
- `[Field Name]` bracket syntax
- `'Field Name'` single-quoted syntax
- Bare identifiers (`fieldName`)
- Operators `+ - * /`
- Excel functions: `IF`, `IFERROR`, `ABS`, `ROUND`, `INT`, `SQRT`
- Division by zero → `NaN`

Tokens are substituted with their numeric values from the aggregated result row before `eval()`.

```
CALC: 'OTIF %' = totalDeliveredQty/totalOrderQty
CALC: 'RGB  % CONTR' = rgbDeliveredQty/totalDeliveredQty
```

#### Step 8 — Grand Total Row

If `pivot["row_grand_total"] = true` and GROUP BY dimensions exist, a `"Grand Total"` row is appended. Each measure is re-aggregated across the entire result (using `sum`, `mean`, `min`, `max` as appropriate). Calculated fields are re-evaluated on the grand total row using the component column sums.

#### Step 9 — In-Pivot Filters (`apply_pivot_filters`)

Applies `pivot["pivot_filters"]` — these operate on the result DataFrame after aggregation:
- `top10` — keeps top N (or bottom N, or top N%) rows ranked by the first value column
- `valueList` — keeps rows whose field value matches a whitelist

---

## HTML Renderer

### Panel Structure

Each pivot produces one HTML panel `<div class="pivot-panel" id="panel_N">`:

```
pivot-panel
├── pivot-header
│   ├── pivot-title          (pivot name)
│   ├── pivot-meta           (pivot ID, host sheet, row count, measure count)
│   └── page-filter-bar      (interactive dropdowns for axisPage filters)
├── controls bar             (Expand All, Collapse All, search box, row count)
└── table-wrap
    └── <table id="tbl_panel_N" data-ndims="N">
        ├── <thead>
        │   ├── ctrl-row     (per-column ⊞ ⊟ expand/collapse buttons)
        │   └── header row   (column names + sort button + filter ▾ button)
        └── <tbody>
            ├── data rows    (with data-gid, data-pid, data-lvl, data-agg, data-vals, data-pf_*)
            └── grand total  (class="grand-total")
```

### Frozen Dimension Columns

Dimension (GROUP BY) columns are `position: sticky` with incrementing `left:` values (160px per level). They remain visible while scrolling horizontally through many measure columns.

Column headers for dimension columns carry `z-index: 15–20` so they stay above scrolling value cells.

### Collapse / Expand Hierarchy

#### Data attributes on each `<tr>`

| Attribute | Content | Example |
|---|---|---|
| `data-gid` | Full pipe-joined dim path | `"DAR\|Urban\|Block A\|Route1\|Store 5"` |
| `data-pid` | Parent path (all but last segment) | `"DAR\|Urban\|Block A\|Route1"` |
| `data-lvl` | Outermost dimension level that changed | `"0"` for a new region row |
| `data-agg` | JSON object of pre-computed subtotals | `{"OUTLETS":"58","TIER 1":"1,234,567"}` |
| `data-vals` | JSON object of this row's own leaf values | `{"OUTLETS":"3","TIER 1":"45,200"}` |
| `data-pf_<field>` | Prettified page-filter field value | `data-pf_Month="2025-07-01"` |

#### Group header detection

A row is a "group header" (gets a `▼/▶` toggle button) if its `data-gid` appears as a `data-pid` of any other row — i.e. it has at least one child. This is computed in Python before rendering.

#### Toggle behaviour (JS `toggleGroup`)

**Collapse:** Add `hidden-row` to all rows whose `data-pid === gid` or `data-pid.startsWith(gid + '|')`. Also recursively mark sub-group headers as `grp-collapsed`. Then call `_showSubtotals(tr)` to overwrite value cells with the `data-agg` JSON.

**Expand:** Call `_showLeaf(tr)` to restore the row's own `data-vals` into value cells. Then remove `hidden-row` from direct children only (`data-pid === gid`). Their own descendants stay collapsed if they were collapsed.

#### Per-column expand/collapse (ctrl-row buttons)

`colExpandAll(tableId, dimIdx)` / `colCollapseAll(tableId, dimIdx)` operate only on rows whose `data-lvl === dimIdx`. This mirrors Excel's column grouping buttons — collapsing the "region" column does not affect the "areaType" collapse state.

#### Subtotals — how they are computed

In Python, before rendering, a `groupby` is run for each dimension prefix depth from 0 to `n_dims - 2`. The aggregation function used per measure comes from `values[n].aggregation` in the JSON (same function as the main query). Results are stored in `group_agg[prefix_key]` keyed by the pipe-joined prefix path. Each row that is a group header receives its subtotal dict serialised as `data-agg='{"COL": "formatted_value", ...}'`. Calculated fields are **not** re-evaluated for subtotals (they receive the aggregated numerator/denominator values which may be from different functions — this is a known limitation).

### Page Filter Bar

Each `pivot["filters"]` entry (including `show_all = true` entries) becomes a clickable `▼ FieldName` button. Clicking opens the shared checkbox dropdown (`col-filter-dd`) populated from the field's `distinct_values` list in the JSON, merged with any values actually present in the data rows.

Each data row carries `data-pf_<sanitised_field_name>="<prettified_value>"` attributes. The page filter state is stored in `_state[tableId].pageState` as `{ fieldName → Set<string> }`. An empty Set means "all values selected."

Default selections from the JSON (`show_all = false, selected_item = ...`) are applied via the `_pivotInitQueue` mechanism (see below).

### Column Filter Dropdowns

Every column header (dim and value columns alike) carries a `▾` button. Clicking opens a dropdown that:
- Collects unique values from that column across all rows in `_state[tableId].allRows`
- For dim columns, reads text content excluding toggle/button child nodes
- Shows checkboxes sorted numerically (if parseable) or alphabetically
- Shows item counts in the right margin
- Includes a live search box and All/None shortcuts
- On Apply: stores `Set<string>` in `_state[tableId].filterState[colIdx]`; updates a `[N]` badge on the header
- On Clear: empties the set (all values shown)

### `_pivotInitQueue` — Deferred Initialisation

The `_HTML_JS` block is placed in `<head>` and declares `_pivotInitQueue = []` plus a `DOMContentLoaded` listener that flushes the queue. Each panel's inline `<script>` (in `<body>`) pushes a function that calls `_getState()` and sets the default page-filter state. This ensures `_getState` is always defined when the init functions run, regardless of document structure.

```
<head>   →  _HTML_JS parsed → _pivotInitQueue = [], _getState defined
<body>   →  panel HTML parsed → _pivotInitQueue.push(fn)
           (DOMContentLoaded fires) → queue flushed → fn() calls _getState()
```

### Client-Side State — `_state[tableId]`

Each table maintains a state object created lazily on first interaction:

```js
{
  allRows:     TR[]     // all non-grand-total rows (snapshotted once at init)
  filterState: {}       // colIdx → Set<string>  (column dropdown filters)
  pageState:   {}       // fieldName → Set<string>  (page filter dropdowns)
  searchQ:     string   // current search box value
  sortCol:     number   // -1 = unsorted
  sortAsc:     bool
}
```

`_applyAll(tableId)` is the single re-render function. It:
1. Filters `allRows` against `pageState` (matching `data-pf_*` attributes)
2. Filters against `filterState` (matching cell text content)
3. Filters against `searchQ` (full-row text contains)
4. Sorts the visible set
5. Hides all rows, then unhides visible rows
6. Updates the row count badge

Sort and filter do **not** interact with the collapse state — collapsed rows pass through `_applyAll` as normal hidden rows (they already have `hidden-row`). The search intentionally bypasses collapse and shows any matching row regardless of whether its parent is collapsed.

### Number Formatting

`_excel_format_to_python` maps Excel `num_format` strings to Python format hints:

| Excel `num_format` | Python hint | Example output |
|---|---|---|
| `#,##0` | `,d` | `1,234` |
| `#,##0.00` | `,.2f` | `1,234.56` |
| `0%` / `0.0%` | `pct` | `73.4%` |
| `0.00` | `.2f` | `73.45` |
| `General` / `@` | None | auto |

The `pct` hint auto-detects whether the value is stored as a fraction (`0.734`) or as percentage points (`73.4`) using the heuristic `abs(v) < 2`.

---

## Backend Abstraction — DuckDB Step 2

The `DataBackend` abstract class defines two methods:

```python
def load(self) -> pd.DataFrame          # load the full source into memory (or connect)
def groupby_agg(df, group_cols, named_aggs) -> pd.DataFrame  # GROUP BY
```

`ExcelBackend` implements both for pandas in-memory operation. `DuckDBBackend` (not yet implemented) would:

1. `load()` → connect to a DuckDB file or MySQL via the DuckDB MySQL extension; return a lightweight connector object (or a small metadata DataFrame)
2. `groupby_agg()` → emit `SELECT ... GROUP BY` SQL; push filtering down to the DB

No other code needs to change — all filter, formula, show_data_as, and HTML rendering logic operates on the returned DataFrame.

---

## JSON Schema — What the Engine Reads

The engine reads these keys from each pivot object. All others are ignored.

### Top level

| Key | Used by | Description |
|---|---|---|
| `id` | all | Pivot identifier, used for output file names and HTML panel IDs |
| `name` | HTML renderer | Human-readable pivot name shown as panel title |
| `host_sheet` | HTML renderer | Source sheet name (display only) |
| `filters[]` | `apply_page_filters` | axisPage filter definitions |
| `fields{}` | all | Per-field metadata: `data_type`, `hidden_items`, `distinct_values`, `placement` |
| `rows[]` | execute_pivot | Row dimension definitions |
| `columns[]` | execute_pivot | Column dimension definitions |
| `values[]` | execute_pivot | Value / measure definitions |
| `calculated_fields[]` | `_eval_formula` | Calculated field name + formula |
| `pivot_filters[]` | `apply_pivot_filters` | Post-aggregation label/value/top10 filters |
| `row_grand_total` | execute_pivot | Whether to append a grand total row |
| `master_column_links{}` | (unused) | Available for future DAX/DuckDB column resolution |

### `filters[]` entry

```json
{
  "field":         "Month",
  "show_all":      false,
  "selected_item": "2025-07-01T00:00:00"
}
```

`selected_item` can be an ISO date string, a number, or a plain string. `show_all: true` means no restriction on this field.

### `fields[name]` entry

```json
{
  "data_type":       "date",
  "placement":       "axisPage",
  "hidden_items":    ["NORTHERN", "EASTERN"],
  "distinct_values": [{"type": "date", "value": "2025-07-01T00:00:00"}, ...]
}
```

`data_type: "date"` triggers the serial-number filter path when the column is numeric. `hidden_items` are raw cache values excluded from results. `distinct_values` is used to populate the page-filter dropdown.

### `values[]` entry

```json
{
  "display_name":  "OUTLETS",
  "source_field":  "storeCode",
  "aggregation":   "count",
  "is_calculated": false,
  "show_data_as":  "normal",
  "num_format":    "#,##0",
  "formula":       null
}
```

For calculated fields: `is_calculated: true`, `formula: "totalDeliveredQty/totalOrderQty"`.

---

## Diagnostic Output Reference

The script prints progress to stdout throughout execution:

```
[config] sheet='Sales Data Dump'  header_row=2
[backend] Loading  'data.xlsx'
[backend] Loaded   7,496 rows × 246 columns
[loader] WARNING: JSON is truncated at char 265573 – only partial pivots available
[engine] Executing 17 pivot(s)

[pivot] ── PT_PRESALES_CHANNEL_1  ─────────────────────────────────────
  [filter] 'Month' = 2025-07-01  (serial=45839)  kept 7496/7496
  [filter] DEBUG  'Month' dtype=int64 sample=['45839', '45870'] target=2025-07-01
  [filter] SKIP  'OUTLET SYSTEM ACTIVE STATUS' = 0 – treated as (All) sentinel
        rows after page-filters     : 7,496
  [hidden] 'region'  excluded 1,204 rows (4 hidden values)
        rows after hidden exclusions : 6,292
        WARNING: value source 'missingCol' not in data (display='SOME MEASURE')
        CALC: 'OTIF %' = totalDeliveredQty/totalOrderQty
        result shape: (2,847, 32)

[output] ✓ Dashboard → dashboard.html  (17 pivot(s))
[engine] Done.
```

**Key warning patterns to watch for:**

| Warning | Meaning | Fix |
|---|---|---|
| `page filter produced 0 rows` | Filter matched nothing | Check field name spelling and date format; see DEBUG line for sample values |
| `dimension columns missing from data` | A field in `rows[]` is not a column in the Excel sheet | Check `rows[]` field names against actual column headers |
| `value source 'X' not in data` | `source_field` not found in DataFrame | Column renamed or missing from the master sheet |
| `no formula for calculated field` | Formula lookup failed | Check `calculated_fields[]` or `fields[name].formula` in the JSON |
| `JSON is truncated` | Only partial pivots loaded | Re-run `pivot_extractor.py` on the full workbook |

---

## Known Limitations

1. **Calculated field subtotals** — Pre-computed subtotals for collapsed rows use the raw aggregation function for each measure. Calculated fields (e.g. `OTIF% = delivered / ordered`) are not re-evaluated at subtotal level because the subtotal aggregation runs before formula evaluation. The subtotal cells for calculated fields will show blank when collapsed.

2. **`show_data_as` on subtotals** — Subtotals always show the raw aggregated value, not the transformed value (e.g. `percentOfTotal`).

3. **Column axis dimensions** — If a pivot has real column-axis fields (not just the `__VALUES__` sentinel), they are included in the GROUP BY but the result is rendered as a flat table rather than a proper cross-tab matrix.

4. **`countnums` aggregation** — Mapped to `count` (counts non-blank), not strictly counting only numeric values.

5. **`percentOfParent` and `index` show_data_as modes** — Not implemented; values pass through unchanged.

6. **DuckDB backend** — Not yet implemented. The `--backend duckdb` flag raises `NotImplementedError`.

---

## File Outputs

| Mode | Output |
|---|---|
| `--pivot-id X` | `X.html` |
| Default (multiple pivots) | `<pivot_id>.html` per pivot |
| `--combined` | `dashboard.html` |
| `--output path.html` | `path.html` |

All HTML files are fully self-contained — no external assets, no server required. CSS variables, fonts (Google Fonts CDN), and all JavaScript are embedded inline.
