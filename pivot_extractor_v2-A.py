#!/usr/bin/env python3
"""
pivot_extractor_v2.py
=====================
Extracts every pivot table from an Excel (.xlsx) workbook and produces:

  1. A structured JSON mapping of each pivot's full logic:
       - data source linkage (sheet + range -> master columns)
       - row / column / filter / value field assignments
       - calculated fields and their formulas (with DAX translation)
       - date/number groupings and bucket labels
       - showDataAs transformations (% of total, running total, diff, etc.)
       - active filters and selected items
       - distinct member values for column/row fields (e.g. "Alice, Bob, …")

  2. DAX EVALUATE blocks that exactly replicate each pivot's output,
     ready to paste into Power BI / DAX Studio.

Fixes vs v1
-----------
  * Column field members (e.g. "Alice", "Bob") are now fully resolved from the
    pivot cache even when the workbook uses the "pivotCache" folder name instead
    of "pivotCaches" — the v1 regex only matched the latter.
  * Cache-to-pivot linkage is resolved via the pivot table's own .rels file so
    cache_id mismatches no longer leave the field list empty.
  * Column fields are included in the SUMMARIZECOLUMNS GROUP-BY axis.
  * Calculated fields are detected from both cacheField/@formula (databaseField=0)
    and dataField/@formula, and their Excel→DAX translation is emitted.
  * showDataAs transformations (running total, % of total, index, etc.) produce
    correct DAX VAR blocks.
  * Grand-total block avoids double-comma when value list has one entry.
  * Verbose output reports both row AND column field names.

Usage
-----
    python pivot_extractor_v2.py  <workbook.xlsx>  <master_sheet>  [options]

    positional:
      workbook.xlsx     Excel file to analyse
      master_sheet      Worksheet name of the primary data table

    optional:
      --header-row N    1-based row number of the column headers in master_sheet
                        [default: 1]
      -o / --output     Output JSON path   [default: <stem>_pivots.json]
      --dax             Also write a .dax file with all generated queries
      -v / --verbose    Print detailed extraction progress

Examples
--------
    python pivot_extractor_v2.py sales.xlsx "Sheet1" --dax -v
    python pivot_extractor_v2.py report.xlsx "GL Entries" --header-row 3 -o out/pivots.json
    python pivot_extractor_v2.py data.xlsx "Raw Data" --header-row 5 --dax
"""

import re, sys, json, zipfile, argparse, traceback
import xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone
from collections import defaultdict

# ---------------------------------------------------------------------------
# XML namespace helpers
# ---------------------------------------------------------------------------

NSX = 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'
NSR = 'http://schemas.openxmlformats.org/officeDocument/2006/relationships'
NSP = 'http://schemas.openxmlformats.org/package/2006/relationships'

def tx(local):  return f'{{{NSX}}}{local}'
def tr(local):  return f'{{{NSR}}}{local}'

def xattr(el, name, default=None): return el.get(name, default)
def xint(el, name, default=None):
    v = el.get(name)
    if v is None: return default
    try: return int(v)
    except: return default
def xbool(el, name, default=False):
    v = el.get(name)
    if v is None: return default
    return v not in ('0', 'false', 'False', 'no')

def fone(root, *tags):
    el = root
    for t in tags:
        if el is None: return None
        el = el.find(tx(t))
    return el

def fall(root, *tags):
    el = fone(root, *tags[:-1]) if len(tags) > 1 else root
    return el.findall(tx(tags[-1])) if el is not None else []

def xml_read(zf, path):
    path = path.replace('//', '/')
    return ET.fromstring(zf.read(path))

def normalize_xl_path(raw: str) -> str:
    """
    Normalize a Target attribute from a .rels file to a canonical xl/... path.
    Handles both absolute (/xl/...) and relative (../pivotCache/...) forms.
    FIX: Accepts both 'pivotCache' and 'pivotCaches' folder names.
    """
    raw = raw.strip()
    if raw.startswith('/'):
        return raw.lstrip('/')
    # Already starts with xl/ or similar
    if raw.startswith('xl/'):
        return raw
    # Relative from xl/ sub-directory: strip leading ../
    stripped = re.sub(r'^(\.\./)+', '', raw)
    return 'xl/' + stripped

# ---------------------------------------------------------------------------
# Number format helpers
# ---------------------------------------------------------------------------

BUILTIN_FMT = {
    0:'General', 1:'0', 2:'0.00', 3:'#,##0', 4:'#,##0.00',
    9:'0%', 10:'0.00%', 11:'0.00E+00', 14:'mm-dd-yy', 15:'d-mmm-yy',
    16:'d-mmm', 17:'mmm-yy', 18:'h:mm AM/PM', 19:'h:mm:ss AM/PM',
    20:'h:mm', 21:'h:mm:ss', 22:'m/d/yy h:mm', 37:'#,##0 ;(#,##0)',
    38:'#,##0 ;[Red](#,##0)', 39:'#,##0.00;(#,##0.00)', 45:'mm:ss',
    46:'[h]:mm:ss', 47:'mmss.0', 48:'##0.0E+0', 49:'@',
}

def fmt_str(fid, custom):
    if fid is None: return None
    try: n = int(fid)
    except: return str(fid)
    return custom.get(n) or BUILTIN_FMT.get(n) or f'numFmtId={n}'

# ---------------------------------------------------------------------------
# Labels
# ---------------------------------------------------------------------------

AGG_LABEL = {
    'sum':'SUM','count':'COUNT','average':'AVERAGE','max':'MAX','min':'MIN',
    'product':'PRODUCT','countNums':'COUNT(numbers)','stdDev':'STDEV.S',
    'stdDevP':'STDEV.P','var':'VAR.S','varP':'VAR.P',
}
SHOW_LABEL = {
    'normal':'Raw value','difference':'Difference from','percent':'% of',
    'percentDiff':'% difference from','runTotal':'Running total in',
    'percentOfRow':'% of row total','percentOfCol':'% of column total',
    'percentOfTotal':'% of grand total','index':'Index',
    'percentOfParentRow':'% of parent row','percentOfParentCol':'% of parent col',
    'percentOfParent':'% of parent','rankAscending':'Rank (asc)',
    'rankDescending':'Rank (desc)',
}
DAX_AGG = {
    'sum':'SUM','count':'COUNTA','average':'AVERAGE','max':'MAX','min':'MIN',
    'countNums':'COUNT','stdDev':'STDEV.S','stdDevP':'STDEV.P',
    'var':'VAR.S','varP':'VAR.P','product':'PRODUCTX',
}

# ---------------------------------------------------------------------------
# WorkbookIndex
# ---------------------------------------------------------------------------

class WorkbookIndex:
    def __init__(self, zf):
        self.zf = zf
        self.sheets = {}           # rId -> {name, sheetId, path}
        self.pivot_caches = {}     # def_path -> {def_path, records_path, cache_id}
        self.sheet_pivots = {}     # sheet_path -> [pivot_path, ...]
        self.custom_numfmts = {}   # fmtId -> format string
        self._parse()

    def _parse(self):
        zf = self.zf

        # ---- workbook.xml ----
        root = xml_read(zf, 'xl/workbook.xml')
        for s in root.findall(f'.//{tx("sheet")}'):
            rid = s.get(f'{{{NSR}}}id')
            self.sheets[rid] = {
                'name': s.get('name'), 'sheetId': s.get('sheetId'), 'path': None
            }

        # ---- workbook.xml.rels ----
        try:
            rels = xml_read(zf, 'xl/_rels/workbook.xml.rels')
        except:
            rels = None
        if rels is not None:
            for rel in rels:
                rid   = rel.get('Id', '')
                tgt   = rel.get('Target', '')
                rtype = rel.get('Type', '').split('/')[-1]
                path  = normalize_xl_path(tgt)
                if rtype == 'worksheet' and rid in self.sheets:
                    self.sheets[rid]['path'] = path
                elif rtype == 'pivotCacheDefinition':
                    self.pivot_caches[path] = {
                        'def_path': path, 'records_path': None, 'cache_id': None
                    }

        # FIX: Scan for pivot cache files matching BOTH folder name variants:
        #   xl/pivotCaches/pivotCacheDefinitionN.xml  (standard)
        #   xl/pivotCache/pivotCacheDefinitionN.xml   (some generators, e.g. openpyxl / macOS Excel)
        for f in zf.namelist():
            if re.match(r'xl/pivotCach(?:es?)/pivotCacheDefinition\d+\.xml', f):
                if f not in self.pivot_caches:
                    self.pivot_caches[f] = {'def_path': f, 'records_path': None, 'cache_id': None}
            if re.match(r'xl/pivotCach(?:es?)/pivotCacheRecords(\d+)\.xml', f):
                m = re.match(r'xl/(pivotCach(?:es?))/pivotCacheRecords(\d+)\.xml', f)
                if m:
                    def_p = f'xl/{m.group(1)}/pivotCacheDefinition{m.group(2)}.xml'
                    if def_p in self.pivot_caches:
                        self.pivot_caches[def_p]['records_path'] = f

        # Resolve cache IDs from def files
        for path in list(self.pivot_caches):
            try:
                r = xml_read(zf, path)
                cid = r.get('cacheId')
                if cid:
                    self.pivot_caches[path]['cache_id'] = int(cid)
            except:
                pass

        # ---- sheet .rels -> pivot table paths ----
        for rid, sinfo in self.sheets.items():
            spath = sinfo.get('path')
            if not spath: continue
            parts    = spath.rsplit('/', 1)
            rel_path = parts[0] + '/_rels/' + parts[1] + '.rels'
            try:
                rroot = xml_read(zf, rel_path)
            except:
                continue
            for rel in rroot:
                rtype  = rel.get('Type', '').split('/')[-1]
                target = rel.get('Target', '')
                if rtype == 'pivotTable':
                    sheet_dir = parts[0]
                    if target.startswith('../'):
                        parent = '/'.join(sheet_dir.rstrip('/').split('/')[:-1])
                        ppath  = parent + '/' + target[3:]
                    else:
                        ppath = (sheet_dir + '/' + target).replace('//', '/')
                    ppath = ppath.replace('//', '/')
                    self.sheet_pivots.setdefault(spath, []).append(ppath)

        # ---- styles -> custom numfmts ----
        try:
            sroot = xml_read(zf, 'xl/styles.xml')
            for nf in sroot.findall(f'.//{tx("numFmt")}'):
                try:
                    self.custom_numfmts[int(nf.get('numFmtId', 0))] = nf.get('formatCode', '')
                except:
                    pass
        except:
            pass

    def name_to_path(self, name):
        for v in self.sheets.values():
            if v['name'] == name: return v['path']
        return None

    def path_to_name(self, path):
        for v in self.sheets.values():
            if v['path'] == path: return v['name']
        return path

    def cache_by_id(self, cid):
        for v in self.pivot_caches.values():
            if v['cache_id'] == cid: return v
        return None

    def resolve_cache_for_pivot(self, zf, pivot_path):
        """
        FIX: Resolve the cache definition path for a pivot table via its own
        .rels file. This is more reliable than matching by cacheId when cacheId
        parsing silently fails (e.g. wrong folder scan pattern).
        Returns (def_path, cache_id) or (None, None).
        """
        parts    = pivot_path.rsplit('/', 1)
        rel_path = parts[0] + '/_rels/' + parts[1] + '.rels'
        try:
            rroot = xml_read(zf, rel_path)
        except:
            return None, None
        for rel in rroot:
            rtype = rel.get('Type', '').split('/')[-1]
            if rtype == 'pivotCacheDefinition':
                tgt      = rel.get('Target', '')
                def_path = normalize_xl_path(tgt)
                # Look up cacheId from the definition file
                try:
                    droot = xml_read(zf, def_path)
                    cid   = droot.get('cacheId')
                    return def_path, (int(cid) if cid else None)
                except:
                    return def_path, None
        return None, None


# ---------------------------------------------------------------------------
# PivotCacheParser
# ---------------------------------------------------------------------------

class PivotCacheParser:
    def __init__(self, zf, def_path, custom_fmts):
        self.zf     = zf
        self.path   = def_path
        self.fmts   = custom_fmts
        self.fields = []
        self.source = {}

    def parse(self):
        root = xml_read(self.zf, self.path)

        # Source
        cs = fone(root, 'cacheSource')
        if cs is not None:
            wss = fone(cs, 'worksheetSource')
            if wss is not None:
                self.source = {
                    'type':  'worksheet',
                    'sheet': wss.get('sheet'),
                    'range': wss.get('ref'),
                    'name':  wss.get('name'),
                }
            else:
                self.source = {'type': cs.get('type', 'unknown')}

        # Fields
        cfe = fone(root, 'cacheFields')
        if cfe is None: return
        for idx, cf in enumerate(cfe.findall(tx('cacheField'))):
            name     = cf.get('name', f'Field{idx}')
            db_fld   = xbool(cf, 'databaseField', True)
            formula  = cf.get('formula')
            fid      = xint(cf, 'numFmtId')

            field = {
                'index':             idx,
                'name':              name,
                'num_format':        fmt_str(fid, self.fmts),
                'is_database_field': db_fld,
                # Calculated = formula present OR flagged as non-database field
                'is_calculated':     (not db_fld) or (formula is not None),
                'formula':           formula,
                'data_type':         None,
                'distinct_values':   [],
                'grouping':          None,
            }

            si = fone(cf, 'sharedItems')
            if si is not None:
                if xbool(si, 'containsDate'):
                    field['data_type'] = 'date'
                elif xbool(si, 'containsNumber') and not xbool(si, 'containsString'):
                    field['data_type'] = 'number'
                elif xbool(si, 'containsString'):
                    field['data_type'] = 'text'
                else:
                    field['data_type'] = 'mixed'

                vals = []
                for ch in si:
                    loc = ch.tag.split('}')[-1]
                    v   = ch.get('v')
                    if   loc == 's': vals.append({'type': 'string', 'value': v})
                    elif loc == 'n': vals.append({'type': 'number', 'value': float(v) if v else None})
                    elif loc == 'd': vals.append({'type': 'date',   'value': v})
                    elif loc == 'b': vals.append({'type': 'bool',   'value': v == '1'})
                    elif loc == 'm': vals.append({'type': 'blank',  'value': None})
                    elif loc == 'e': vals.append({'type': 'error',  'value': v})
                    if len(vals) >= 300: break
                field['distinct_values'] = vals

            fg = fone(cf, 'fieldGroup')
            if fg is not None:
                field['grouping'] = self._group(fg)
                if not field['data_type']: field['data_type'] = 'grouped'

            self.fields.append(field)

    def _group(self, fg):
        rp = fone(fg, 'rangePr')
        if rp is not None:
            buckets = [x.get('v') for x in fall(fg, 'groupItems', 's') if x.get('v')]
            return {
                'type':        'range',
                'group_by':    rp.get('groupBy'),
                'auto_start':  xbool(rp, 'autoStart', True),
                'auto_end':    xbool(rp, 'autoEnd', True),
                'start_value': rp.get('startDate') or rp.get('startNum'),
                'end_value':   rp.get('endDate')   or rp.get('endNum'),
                'interval':    rp.get('groupInterval'),
                'base_field':  xint(fg, 'base'),
                'buckets':     buckets,
            }
        dp = fone(fg, 'discretePr')
        if dp is not None:
            mappings = [xint(x, 'v') for x in dp.findall(tx('x'))]
            buckets  = [x.get('v') for x in fall(fg, 'groupItems', 's') if x.get('v')]
            return {'type': 'discrete', 'buckets': buckets, 'mappings': mappings}
        return None


# ---------------------------------------------------------------------------
# PivotTableParser
# ---------------------------------------------------------------------------

class PivotTableParser:
    def __init__(self, zf, ppath, cache_fields, host_sheet, custom_fmts):
        self.zf     = zf
        self.path   = ppath
        self.fields = cache_fields   # list from PivotCacheParser
        self.sheet  = host_sheet
        self.fmts   = custom_fmts

    def parse(self):
        root = xml_read(self.zf, self.path)
        r = {
            'id':               None,
            'name':             root.get('name', 'PivotTable'),
            'host_sheet':       self.sheet,
            'cache_id':         xint(root, 'cacheId'),
            'location':         None,
            'row_grand_total':  xbool(root, 'rowGrandTotals', True),
            'col_grand_total':  xbool(root, 'colGrandTotals', True),
            'data_caption':     root.get('dataCaption', 'Values'),
            'compact_mode':     xbool(root, 'compact', True),
            'outline_mode':     xbool(root, 'outline', False),
            'style':            None,
            'fields':           {},
            'rows':             [],
            'columns':          [],
            'filters':          [],
            'values':           [],
            'pivot_filters':    [],
            'calculated_fields': [],
            'warnings':         [],
        }

        loc = fone(root, 'location')
        if loc is not None:
            r['location'] = {
                'ref':              loc.get('ref'),
                'first_header_row': xint(loc, 'firstHeaderRow', 1),
                'first_data_row':   xint(loc, 'firstDataRow', 2),
                'first_data_col':   xint(loc, 'firstDataCol', 1),
            }

        psi = fone(root, 'pivotTableStyleInfo')
        if psi is not None:
            r['style'] = psi.get('name')

        # Pivot fields metadata (one entry per cache field, same index order)
        pivot_fields = []
        pfc = fone(root, 'pivotFields')
        if pfc is not None:
            for idx, pf in enumerate(pfc.findall(tx('pivotField'))):
                if idx >= len(self.fields): break
                pfd = {
                    'index':         idx,
                    'cache_name':    self.fields[idx]['name'],
                    'axis':          pf.get('axis'),
                    'is_data_field': xbool(pf, 'dataField'),
                    'subtotals':     self._subtotals(pf),
                    'hidden_items':  self._hidden(pf, self.fields[idx]),
                    'show_all':      xbool(pf, 'showAll', True),
                    'sort_type':     pf.get('sortType', 'manual'),
                }
                pivot_fields.append(pfd)

        # Row fields
        for f in fall(root, 'rowFields', 'field'):
            idx = xint(f, 'x', -1)
            if idx == -2:
                r['rows'].append({'type': 'values_header', 'field': '__VALUES__'})
            elif 0 <= idx < len(self.fields):
                r['rows'].append({
                    'type':  'field',
                    'field': self.fields[idx]['name'],
                    'index': idx,
                })

        # Column fields — FIX: now resolves member values from cache
        for f in fall(root, 'colFields', 'field'):
            idx = xint(f, 'x', -1)
            if idx == -2:
                r['columns'].append({'type': 'values_header', 'field': '__VALUES__'})
            elif 0 <= idx < len(self.fields):
                cf      = self.fields[idx]
                members = [v['value'] for v in cf.get('distinct_values', [])
                           if v.get('value') is not None]
                r['columns'].append({
                    'type':    'field',
                    'field':   cf['name'],
                    'index':   idx,
                    'members': members,   # e.g. ["Alice", "Bob", "Carol", ...]
                })

        # Page/filter fields
        for pf in fall(root, 'pageFields', 'pageField'):
            fi = xint(pf, 'fld', -1)
            ii = xint(pf, 'item')
            if 0 <= fi < len(self.fields):
                cf  = self.fields[fi]
                sel = None
                if ii is not None and ii < len(cf.get('distinct_values', [])):
                    sel = cf['distinct_values'][ii].get('value')
                r['filters'].append({
                    'field':          cf['name'],
                    'field_index':    fi,
                    'selected_item':  sel,
                    'selected_index': ii,
                    'show_all':       ii is None,
                })

        # Data fields (values)
        for df in fall(root, 'dataFields', 'dataField'):
            fi      = xint(df, 'fld', -1)
            dname   = df.get('name')
            agg     = df.get('subtotal', 'sum')
            show    = df.get('showDataAs', 'normal')
            bfld    = xint(df, 'baseField')
            bitm    = xint(df, 'baseItem')
            fid     = xint(df, 'numFmtId')
            formula = df.get('formula')

            if fi == -2:
                fname, is_calc, calc_f = '__VALUES__', False, None
            elif 0 <= fi < len(self.fields):
                cf      = self.fields[fi]
                fname   = cf['name']
                is_calc = cf['is_calculated']
                calc_f  = cf.get('formula') or formula
            else:
                fname, is_calc, calc_f = f'Field[{fi}]', False, formula

            bfname, bfval = None, None
            if bfld is not None and 0 <= bfld < len(self.fields):
                bfname = self.fields[bfld]['name']
                bvals  = self.fields[bfld].get('distinct_values', [])
                if bitm is not None and bitm < len(bvals):
                    bfval = bvals[bitm].get('value')

            vf = {
                'display_name':       dname,
                'source_field':       fname,
                'field_index':        fi,
                'aggregation':        agg,
                'aggregation_label':  AGG_LABEL.get(agg, agg.upper()),
                'show_data_as':       show,
                'show_data_as_label': SHOW_LABEL.get(show, show),
                'base_field':         bfname,
                'base_item':          bfval,
                'num_format':         fmt_str(fid, self.fmts),
                'is_calculated':      is_calc,
                'formula':            calc_f,
            }
            r['values'].append(vf)

            if is_calc and calc_f:
                r['calculated_fields'].append({
                    'name':    dname or fname,
                    'formula': calc_f,
                    'context': 'value_field',
                })

        # Pivot label/value/top10 filters
        for flt in fall(root, 'filters', 'filter'):
            fi   = xint(flt, 'fld', -1)
            fn   = self.fields[fi]['name'] if 0 <= fi < len(self.fields) else f'Field[{fi}]'
            crit = self._filter_criteria(flt)
            r['pivot_filters'].append({
                'field':       fn,
                'field_index': fi,
                'filter_type': flt.get('type', 'unknown'),
                'eval_order':  xint(flt, 'evalOrder'),
                'criteria':    crit,
            })

        # Build consolidated fields dict
        for cf in self.fields:
            pf_info = pivot_fields[cf['index']] if cf['index'] < len(pivot_fields) else {}
            ax = pf_info.get('axis') or ('values' if pf_info.get('is_data_field') else 'unused')
            r['fields'][cf['name']] = {
                'index':             cf['index'],
                'data_type':         cf['data_type'],
                'num_format':        cf['num_format'],
                'is_calculated':     cf['is_calculated'],
                'formula':           cf.get('formula'),
                'grouping':          cf.get('grouping'),
                'is_database_field': cf['is_database_field'],
                'placement':         ax,
                'subtotals':         pf_info.get('subtotals', []),
                'hidden_items':      pf_info.get('hidden_items', []),
                'distinct_values':   cf.get('distinct_values', []),
            }

        return r

    def _subtotals(self, pf):
        st = []
        for s in ('sum', 'count', 'average', 'max', 'min', 'product',
                  'countA', 'stdDev', 'stdDevP', 'var', 'varP'):
            if xbool(pf, s): st.append(s)
        if xbool(pf, 'defaultSubtotal') and 'sum' not in st:
            st.insert(0, 'sum')
        return st

    def _hidden(self, pf, cf):
        hidden = []
        iel    = fone(pf, 'items')
        if iel is None: return hidden
        dvals  = cf.get('distinct_values', [])
        for item in iel.findall(tx('item')):
            if item.get('t') == 'default': continue
            if xbool(item, 'h'):
                x = xint(item, 'x')
                if x is not None and x < len(dvals):
                    hidden.append(dvals[x].get('value'))
        return hidden

    def _filter_criteria(self, flt):
        res = {'type': flt.get('type', 'unknown'), 'conditions': []}
        af  = fone(flt, 'autoFilter')
        if af is None: return res
        for fc in af.findall(tx('filterColumn')):
            cid = xint(fc, 'colId', 0)
            fe  = fone(fc, 'filters')
            if fe is not None:
                vals = [x.get('val') for x in fe.findall(tx('filter'))]
                res['conditions'].append({
                    'kind': 'valueList', 'column': cid, 'values': vals,
                    'include_blanks': xbool(fe, 'blank'),
                })
            cfe = fone(fc, 'customFilters')
            if cfe is not None:
                conds = [{'operator': x.get('operator', 'equal'), 'value': x.get('val')}
                         for x in cfe.findall(tx('customFilter'))]
                res['conditions'].append({
                    'kind': 'custom', 'column': cid,
                    'operator': 'AND' if xbool(cfe, 'and') else 'OR',
                    'filters': conds,
                })
            t10 = fone(fc, 'top10')
            if t10 is not None:
                res['conditions'].append({
                    'kind': 'top10', 'column': cid,
                    'top': xbool(t10, 'top', True), 'percent': xbool(t10, 'percent'),
                    'val': t10.get('val'),
                })
            dyn = fone(fc, 'dynamicFilter')
            if dyn is not None:
                res['conditions'].append({
                    'kind': 'dynamic', 'column': cid,
                    'type': dyn.get('type'), 'val': dyn.get('val'),
                })
        return res


# ---------------------------------------------------------------------------
# Master sheet analyser
# ---------------------------------------------------------------------------

def analyse_master(zf, sheet_path, sheet_name, custom_fmts, header_row=1):
    """
    Parse the master data sheet and return column metadata.

    header_row : 1-based row number that contains the column headers.
                 Rows above it are treated as title/metadata and ignored.
                 The first row AFTER header_row is used for sample-value
                 type inference.
    """
    if not sheet_path or sheet_path not in zf.namelist():
        avail = [n for n in zf.namelist() if n.startswith('xl/worksheets/')]
        return {
            'sheet': sheet_name, 'columns': [], 'range': None, 'row_count': None,
            'header_row': header_row,
            'error': f'Sheet path {sheet_path!r} not found. Available: {avail}'
        }

    header_row = max(1, int(header_row))   # guard against 0 or negative
    sample_row_num = header_row + 1        # first data row, used for type inference

    root = xml_read(zf, sheet_path)
    dim  = fone(root, 'dimension')
    used_range = dim.get('ref') if dim is not None else None

    # Shared strings
    ss = []
    try:
        ssr = xml_read(zf, 'xl/sharedStrings.xml')
        for si in ssr.findall(tx('si')):
            ss.append(''.join(t.text or '' for t in si.findall(f'.//{tx("t")}')))
    except:
        pass

    def cell_val(c):
        t  = c.get('t', '')
        if t == 'inlineStr':
            is_el = fone(c, 'is')
            if is_el is not None:
                return ''.join(x.text or '' for x in is_el.findall(f'.//{tx("t")}'))
            return None
        ve = fone(c, 'v')
        v  = ve.text if ve is not None else None
        if v is None: return None
        if t == 's':
            try: return ss[int(v)]
            except: return v
        if t == 'str': return v
        if t == 'b': return v == '1'
        try: return float(v)
        except: return v

    headers, sample_row = [], []
    max_row = 0

    def col_idx(ref):
        m = re.match(r'([A-Za-z]+)', ref or '')
        if not m: return 0
        n = 0
        for ch in m.group(1).upper():
            n = n * 26 + (ord(ch) - ord('A') + 1)
        return n - 1

    def sparse_row_to_list(row_el):
        cells = row_el.findall(tx('c'))
        if not cells: return []
        max_col = max(col_idx(c.get('r', '')) for c in cells) + 1
        result  = [None] * max_col
        for c in cells:
            ci = col_idx(c.get('r', ''))
            if 0 <= ci < max_col:
                result[ci] = cell_val(c)
        return result

    for row_el in root.findall(f'.//{tx("row")}'):
        rn = xint(row_el, 'r', 0)
        max_row = max(max_row, rn)
        if rn == header_row:      headers    = sparse_row_to_list(row_el)
        elif rn == sample_row_num: sample_row = sparse_row_to_list(row_el)

    data_rows = max(0, max_row - header_row)   # rows below the header

    columns = []
    for i, h in enumerate(headers):
        if h is None: continue
        samp = sample_row[i] if i < len(sample_row) else None
        inferred = ('number'  if isinstance(samp, float)
                    else 'boolean' if isinstance(samp, bool)
                    else 'text'    if isinstance(samp, str) else 'unknown')
        columns.append({
            'index':          i,
            'name':           str(h),
            'sample_value':   str(samp) if samp is not None else None,
            'inferred_type':  inferred,
        })

    return {
        'sheet':      sheet_name,
        'path':       sheet_path,
        'range':      used_range,
        'header_row': header_row,
        'row_count':  data_rows,
        'col_count':  len(columns),
        'columns':    columns,
    }


# ---------------------------------------------------------------------------
# DAX Generator
# ---------------------------------------------------------------------------

class DaxGenerator:

    EXCEL_TO_DAX_FN = [
        (r'\bTEXT\(',    'FORMAT('),
        (r'\bISNA\(',    'ISBLANK('),
        (r'\bCOUNTIF\(', '/*COUNTIF→CALCULATE(COUNTA…)*/ COUNTIF('),
        (r'\bSUMIF\(',   '/*SUMIF→CALCULATE(SUM…)*/ SUMIF('),
        (r'\bVLOOKUP\(', '/*VLOOKUP→LOOKUPVALUE()*/ VLOOKUP('),
        (r'\bHLOOKUP\(', '/*HLOOKUP→LOOKUPVALUE()*/ HLOOKUP('),
    ]

    def __init__(self, table, master_info):
        self.table = table
        self.cols  = {c['name']: c for c in master_info.get('columns', [])}

    def ref(self, field):
        return f"'{self.table}'[{field}]"

    def agg_expr(self, agg, field):
        fn = DAX_AGG.get(agg, 'SUM')
        if fn == 'PRODUCTX':
            return f"PRODUCTX('{self.table}', {self.ref(field)})  -- verify PRODUCTX semantics"
        return f"{fn}({self.ref(field)})"

    def excel_to_dax(self, formula, extra_fields=None):
        """
        Translate an Excel calculated field formula to DAX.
        Handles:
          [FieldName]  -> 'Table'[FieldName]
          FieldName    -> 'Table'[FieldName]   (bare name, pivot calc field style)
        Also substitutes common Excel functions with DAX equivalents.
        """
        if not formula: return None
        # Pass 1: [Field] → 'Table'[Field]
        dax = re.sub(r'\[([^\]]+)\]', lambda m: f"'{self.table}'[{m.group(1)}]", formula)
        # Pass 2: bare field names
        known = set(self.cols.keys())
        if extra_fields:
            known.update(extra_fields)
        for fname in sorted(known, key=len, reverse=True):
            if not fname or not re.match(r'^[A-Za-z_]', fname): continue
            escaped = re.escape(fname)
            pattern = r"(?<!\[)'?" + escaped + r"'?(?!\])"
            dax = re.sub(pattern, f"'{self.table}'[{fname}]", dax)
        # Pass 3: function translations
        for pat, rep in self.EXCEL_TO_DAX_FN:
            dax = re.sub(pat, rep, dax, flags=re.IGNORECASE)
        return dax

    def filter_conditions(self, page_filters, pivot_filters):
        conds = []
        for f in page_filters:
            if f['show_all'] or f['selected_item'] is None: continue
            val = f['selected_item']
            ref = self.ref(f['field'])
            conds.append(f'{ref} = "{val}"' if isinstance(val, str) else f'{ref} = {val}')

        for f in pivot_filters:
            ref = self.ref(f['field'])
            for c in f.get('criteria', {}).get('conditions', []):
                kind = c.get('kind')
                if kind == 'valueList':
                    vals = c.get('values', [])
                    if len(vals) == 1:
                        conds.append(f'{ref} = "{vals[0]}"')
                    elif vals:
                        conds.append(f'{ref} IN {{{", ".join(repr(v) for v in vals)}}}')
                elif kind == 'custom':
                    opmap = {'equal': '=', 'notEqual': '<>', 'greaterThan': '>',
                             'greaterThanOrEqual': '>=', 'lessThan': '<', 'lessThanOrEqual': '<='}
                    parts = []
                    for ci in c.get('filters', []):
                        op = opmap.get(ci['operator'])
                        if op: parts.append(f'{ref} {op} "{ci["value"]}"')
                        else:  parts.append(f'-- TODO: {ci["operator"]} on {ref} val={ci["value"]}')
                    j = ' && ' if c.get('operator') == 'AND' else ' || '
                    if parts: conds.append('(' + j.join(parts) + ')')
                elif kind == 'top10':
                    n = c.get('val', 10)
                    t = 'DESC' if c.get('top', True) else 'ASC'
                    conds.append(
                        f'-- TODO: Top {n} filter on {ref}\n    '
                        f'-- Use TOPN({n}, VALUES({ref}), [Measure], {t})'
                    )
                elif kind == 'dynamic':
                    conds.append(f'-- TODO: Dynamic filter ({c.get("type")}) on {ref}')
        return conds

    def value_var(self, vf, varname, pg_filters, pv_filters):
        """Generate a VAR block for one value field, honouring showDataAs."""
        lines = []
        field = vf['source_field']
        agg   = vf['aggregation']
        show  = vf['show_data_as']

        if vf['is_calculated'] and vf['formula']:
            lines.append(f"    -- Calculated field original Excel formula: {vf['formula']}")
            base_expr = self.excel_to_dax(vf['formula']) or self.agg_expr(agg, field)
        else:
            base_expr = self.agg_expr(agg, field)

        conds = self.filter_conditions(pg_filters, pv_filters)
        if conds:
            core = (f"CALCULATE(\n        {base_expr},\n        " +
                    ',\n        '.join(conds) + "\n    )")
        else:
            core = base_expr

        if show == 'normal':
            lines.append(f"    VAR {varname} = {core}")

        elif show == 'percentOfTotal':
            lines.append(f"    -- showDataAs: % of grand total")
            lines.append(f"    VAR {varname}_Total = CALCULATE({base_expr}, ALL('{self.table}'))")
            lines.append(f"    VAR {varname} = DIVIDE({core}, {varname}_Total)")

        elif show == 'percentOfRow':
            lines.append(f"    -- showDataAs: % of row total")
            lines.append(f"    VAR {varname}_Row = CALCULATE({base_expr}, ALLSELECTED())")
            lines.append(f"    VAR {varname} = DIVIDE({core}, {varname}_Row)")

        elif show == 'percentOfCol':
            lines.append(f"    -- showDataAs: % of column total")
            lines.append(f"    VAR {varname}_Col = CALCULATE({base_expr}, ALLSELECTED())")
            lines.append(f"    VAR {varname} = DIVIDE({core}, {varname}_Col)")

        elif show in ('percent', 'difference', 'percentDiff'):
            bf = vf.get('base_field')
            bi = vf.get('base_item')
            lines.append(f"    -- showDataAs: {SHOW_LABEL.get(show, show)}")
            if bf and bi is not None:
                bf_cond = f'{self.ref(bf)} = "{bi}"'
                bvar    = f"{varname}_Base"
                lines.append(f"    VAR {bvar} = CALCULATE({base_expr}, {bf_cond})")
                if   show == 'difference':  lines.append(f"    VAR {varname} = {core} - {bvar}")
                elif show == 'percent':     lines.append(f"    VAR {varname} = DIVIDE({core}, {bvar})")
                elif show == 'percentDiff': lines.append(f"    VAR {varname} = DIVIDE({core} - {bvar}, {bvar})")
            else:
                lines.append(f"    -- TODO: base field/item unresolved")
                lines.append(f"    VAR {varname} = {core}")

        elif show == 'runTotal':
            bf = vf.get('base_field')
            lines.append(f"    -- showDataAs: running total in {bf}")
            if bf:
                lines.append(f"    VAR {varname} = CALCULATE({base_expr},")
                lines.append(f"        FILTER(ALLSELECTED('{self.table}'[{bf}]),")
                lines.append(f"            '{self.table}'[{bf}] <= MAX('{self.table}'[{bf}])))")
            else:
                lines.append(f"    VAR {varname} = {core}  -- TODO: running total base field")

        elif show == 'index':
            lines.append(f"    -- showDataAs: Index")
            lines.append(f"    VAR {varname}_C = {core}")
            lines.append(f"    VAR {varname}_G = CALCULATE({base_expr}, ALL('{self.table}'))")
            lines.append(f"    VAR {varname}_R = CALCULATE({base_expr}, ALLSELECTED())")
            lines.append(f"    VAR {varname} = DIVIDE({varname}_C * {varname}_G, "
                         f"{varname}_R * {varname}_R)")

        else:
            lines.append(f"    -- showDataAs: {show}  (TODO: verify translation)")
            lines.append(f"    VAR {varname} = {core}")

        return '\n'.join(lines)

    def generate(self, pivot, pid):
        out = []
        w   = out.append

        def safe(s): return re.sub(r'[^A-Za-z0-9_]', '_', str(s or ''))

        row_fields = [x['field'] for x in pivot.get('rows', [])    if x.get('field') != '__VALUES__']
        col_fields = [x['field'] for x in pivot.get('columns', []) if x.get('field') != '__VALUES__']
        all_gb     = row_fields + col_fields
        values     = pivot.get('values', [])
        pg_filters = pivot.get('filters', [])
        pv_filters = pivot.get('pivot_filters', [])
        src        = pivot.get('cache_source', {})

        # Column member values for comment (e.g. Alice, Bob, Carol)
        col_members = {}
        for cx in pivot.get('columns', []):
            if cx.get('field') != '__VALUES__' and cx.get('members'):
                col_members[cx['field']] = cx['members']

        w(f"-- {'='*72}")
        w(f"-- PIVOT  : {pivot['name']}  [{pid}]")
        w(f"-- Sheet  : {pivot['host_sheet']}   Location: {pivot.get('location', {}).get('ref', '?')}")
        w(f"-- Source : '{src.get('sheet', '?')}' ! {src.get('range', '?')}")
        w(f"-- Rows   : {row_fields}")
        w(f"-- Columns: {col_fields}")
        if col_members:
            for fn, members in col_members.items():
                preview = ', '.join(str(m) for m in members[:10])
                if len(members) > 10: preview += f', … ({len(members)} total)'
                w(f"--   Column '{fn}' members: {preview}")
        w(f"-- Filters: {[f['field'] + ('=' + str(f['selected_item']) if not f['show_all'] else '(all)') for f in pg_filters]}")
        w(f"-- Values : {[v['display_name'] for v in values]}")
        if pivot.get('calculated_fields'):
            w(f"--")
            w(f"-- Calculated Fields (Excel formula → DAX translation):")
            for cf in pivot['calculated_fields']:
                w(f"--   Excel : {cf['name']} = {cf['formula']}")
                dax_trans = self.excel_to_dax(cf['formula'])
                w(f"--   DAX   : {dax_trans or '(could not translate)'}")
        w(f"-- {'='*72}")
        w('')
        w("DEFINE")
        w('')

        var_names = []
        for i, vf in enumerate(values):
            dn      = vf.get('display_name') or vf.get('source_field', f'Value{i}')
            varname = f"_V{i+1}_{safe(dn)}"
            var_names.append((varname, dn))
            w(f"    -- ── Value field {i+1}: {dn}")
            w(f"    --    Aggregation : {vf['aggregation_label']}")
            w(f"    --    ShowDataAs  : {vf['show_data_as_label']}")
            if vf.get('formula'):      w(f"    --    Formula     : {vf['formula']}")
            if vf.get('num_format'):   w(f"    --    Format      : {vf['num_format']}")
            w(f"    --    Source field: {vf['source_field']}")
            w(self.value_var(vf, varname, pg_filters, pv_filters))
            w('')

        w("EVALUATE")
        w('')

        if not all_gb and not var_names:
            w('ROW("Note", "Pivot has no row/column fields — shows single aggregate")')
        else:
            w("SUMMARIZECOLUMNS(")

            for f in all_gb:
                fi     = pivot['fields'].get(f, {})
                g      = fi.get('grouping')
                hidden = fi.get('hidden_items', [])
                if hidden:
                    w(f"    -- Note: items hidden in this field: {hidden}")
                    w(f"    -- Add KEEPFILTERS({self.ref(f)} <> ...) to exclude them")
                if g and g.get('group_by'):
                    gb_by = g['group_by']
                    w(f"    -- '{f}' is grouped by {gb_by} in Excel.")
                    w(f"    -- In Power BI use a dedicated Date table with date columns.")
                    if gb_by == 'years':
                        w(f"    YEAR({self.ref(f)}),")
                    elif gb_by in ('months', 'quarters'):
                        fn = 'MONTH' if gb_by == 'months' else 'QUARTER'
                        w(f"    {fn}({self.ref(f)}),")
                    else:
                        w(f"    {self.ref(f)},  -- grouping: {gb_by}")
                else:
                    w(f"    {self.ref(f)},")

            for vn, dn in var_names:
                w(f'    "{dn}", {vn},')

            # Remove trailing comma on last non-empty line
            for idx in range(len(out) - 1, -1, -1):
                if out[idx].strip().endswith(','):
                    out[idx] = out[idx].rstrip().rstrip(',')
                    break

            w(")")

        # Grand total block — FIX: avoids double-comma issue
        if (pivot.get('row_grand_total') or pivot.get('col_grand_total')) and var_names:
            w('')
            w("-- Grand total row equivalent:")
            w("EVALUATE ROW(")
            gt_lines = []
            for vn, dn in var_names:
                gt_lines.append(f'    "{dn} (Grand Total)",')
                gt_lines.append(f"    CALCULATE({vn}, ALL('{self.table}'))")
            # Join with commas, last entry has no trailing comma
            for li, line in enumerate(gt_lines):
                is_last = (li == len(gt_lines) - 1)
                if is_last:
                    w(line.rstrip(','))
                else:
                    w(line if line.endswith(',') else line + ',')
            w(")")

        w('')
        w('')
        return '\n'.join(out)


# ---------------------------------------------------------------------------
# Master extract function
# ---------------------------------------------------------------------------

def extract(workbook_path, master_sheet, header_row=1, verbose=False):
    log = print if verbose else (lambda *a, **k: None)

    with zipfile.ZipFile(workbook_path) as zf:
        log(f"Opened: {workbook_path}")
        wb = WorkbookIndex(zf)
        sheet_names = [s['name'] for s in wb.sheets.values()]
        log(f"Sheets: {sheet_names}")
        log(f"Pivot caches found: {len(wb.pivot_caches)}")

        # Master sheet
        master_path = wb.name_to_path(master_sheet)
        if not master_path:
            print(f"  WARNING: master sheet '{master_sheet}' not found. "
                  f"Available: {sheet_names}")
        master_info = analyse_master(zf, master_path, master_sheet, wb.custom_numfmts, header_row)
        log(f"Master '{master_sheet}': {master_info.get('col_count', 0)} cols, "
            f"{master_info.get('row_count', 0)} data rows (headers on row {header_row})")

        # Parse all caches (keyed by def_path)
        cache_map_by_path = {}
        cache_map_by_id   = {}
        for def_path, cinfo in wb.pivot_caches.items():
            try:
                p = PivotCacheParser(zf, def_path, wb.custom_numfmts)
                p.parse()
                cid = cinfo.get('cache_id')
                entry = {'source': p.source, 'fields': p.fields, 'def_path': def_path}
                cache_map_by_path[def_path] = entry
                if cid is not None:
                    cache_map_by_id[cid] = entry
                log(f"  Cache (id={cid}) path={def_path}: "
                    f"{len(p.fields)} fields | source={p.source}")
            except Exception as e:
                log(f"  Cache error ({def_path}): {e}")

        # Discover all pivot tables
        pivot_paths = set()
        for sp in wb.sheet_pivots.values():
            pivot_paths.update(sp)
        for f in zf.namelist():
            if re.match(r'xl/pivotTables/pivotTable\d+\.xml', f):
                pivot_paths.add(f)
        log(f"Pivot tables: {len(pivot_paths)}")

        pivot_to_sheet = {}
        for spath, pvs in wb.sheet_pivots.items():
            sn = wb.path_to_name(spath)
            for p in pvs: pivot_to_sheet[p] = sn

        dax_gen = DaxGenerator(master_sheet, master_info)
        pivots  = []

        for i, ppath in enumerate(sorted(pivot_paths)):
            host = pivot_to_sheet.get(ppath, 'Unknown')
            pid  = f"PT_{re.sub(r'[^A-Za-z0-9]', '_', host)}_{i+1}"
            try:
                root_el = xml_read(zf, ppath)
                cid     = xint(root_el, 'cacheId')

                # FIX: Resolve cache via pivot's own .rels first (most reliable),
                #      fall back to cacheId match from workbook index.
                def_path_via_rels, _ = wb.resolve_cache_for_pivot(zf, ppath)
                cache = (cache_map_by_path.get(def_path_via_rels)
                         or cache_map_by_id.get(cid)
                         or {})
                fields = cache.get('fields', [])

                if not fields:
                    log(f"  WARNING: no cache fields resolved for {ppath} "
                        f"(cacheId={cid}, rels_def={def_path_via_rels})")

                parser = PivotTableParser(zf, ppath, fields, host, wb.custom_numfmts)
                piv    = parser.parse()
                piv['id']           = pid
                piv['pivot_file']   = ppath
                piv['cache_source'] = cache.get('source', {})

                # If Excel used a generic auto-name, replace with the host sheet name
                if re.match(r'^PivotTable\d+$', piv.get('name', ''), re.IGNORECASE):
                 piv['name'] = host

                # Link to master columns
                links = {}
                master_col_names = {c['name'].strip().lower(): c['name']
                                    for c in master_info.get('columns', [])}
                for fn, fi in piv['fields'].items():
                    matched = master_col_names.get(fn.strip().lower())
                    links[fn] = {
                        'matched_master_column': matched,
                        'is_calculated':         fi.get('is_calculated', False),
                        'linked':                matched is not None or fi.get('is_calculated', False),
                    }
                piv['master_column_links'] = links

                piv['dax'] = dax_gen.generate(piv, pid)
                pivots.append(piv)
                log(f"  OK {pid}: '{piv['name']}' | "
                    f"{len(piv['values'])} values | "
                    f"{len(piv['rows'])} row fields | "
                    f"{len(piv['columns'])} col fields")

            except Exception as e:
                pivots.append({
                    'id': pid, 'host_sheet': host, 'pivot_file': ppath,
                    'error': str(e), 'trace': traceback.format_exc(),
                })
                log(f"  ERROR {ppath}: {e}")

    return {
        'meta': {
            'source_file':  Path(workbook_path).name,
            'source_path':  str(workbook_path),
            'master_sheet': master_sheet,
            'header_row':   header_row,
            'extracted_at': datetime.now(timezone.utc).isoformat(),
            'pivot_count':  len(pivots),
            'successful':   sum(1 for p in pivots if 'error' not in p),
            'failed':       sum(1 for p in pivots if 'error' in p),
        },
        'master_sheet_info': master_info,
        'pivots': pivots,
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(
        description='Extract pivot table logic from Excel and generate DAX.',
        epilog=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument('workbook',      help='Path to .xlsx file')
    ap.add_argument('master_sheet',  help='Name of the master data worksheet')
    ap.add_argument('--header-row',  type=int, default=1, metavar='N',
                    help='1-based row number of the column headers in master_sheet (default: 1)')
    ap.add_argument('-o', '--output', help='Output JSON [default: <stem>_pivots.json]')
    ap.add_argument('--dax',          action='store_true', help='Write separate .dax file')
    ap.add_argument('-v', '--verbose', action='store_true')
    args = ap.parse_args()

    wb_path = Path(args.workbook)
    if not wb_path.exists():
        sys.exit(f"File not found: {args.workbook}")
    if not str(wb_path).lower().endswith('.xlsx'):
        sys.exit("Only .xlsx files supported")

    out_json = (Path(args.output) if args.output
                else wb_path.with_name(wb_path.stem + '_pivots.json'))
    out_dax  = wb_path.with_name(wb_path.stem + '_pivots.dax')

    print(f"File         : {wb_path}")
    print(f"Master sheet : '{args.master_sheet}'  (headers on row {args.header_row})")

    result = extract(str(wb_path), args.master_sheet,
                     header_row=args.header_row, verbose=args.verbose)
    meta   = result['meta']

    json_result = json.loads(json.dumps(result, default=str))
    out_json.write_text(json.dumps(json_result, indent=2), encoding='utf-8')
    print(f"\nJSON  → {out_json}  ({out_json.stat().st_size // 1024} KB)")

    print(f"\nSummary")
    print(f"  Total pivots  : {meta['pivot_count']}")
    print(f"  Parsed OK     : {meta['successful']}")
    print(f"  Errors        : {meta['failed']}")

    if meta['pivot_count'] == 0:
        print("\n  No pivot tables found in this workbook.")
        print("  Make sure the file contains actual Excel PivotTables (Insert → PivotTable).")
        print("  Formula-based summary tables are not pivot tables and won't appear here.")

    for p in result['pivots']:
        if 'error' in p:
            print(f"\n  [ERROR] {p['id']}: {p['error']}")
            if args.verbose:
                print(p.get('trace', ''))
        else:
            rows   = [r['field'] for r in p.get('rows', [])    if r.get('field') != '__VALUES__']
            cols   = [c['field'] for c in p.get('columns', []) if c.get('field') != '__VALUES__']
            vals   = [v['display_name'] for v in p.get('values', [])]
            calcs  = [c['name'] for c in p.get('calculated_fields', [])]
            lnk    = sum(1 for v in p.get('master_column_links', {}).values() if v['linked'])
            tot    = len(p.get('master_column_links', {}))
            src    = p.get('cache_source', {})
            print(f"\n  [{p['id']}] \"{p['name']}\" on '{p['host_sheet']}'")
            print(f"    Location    : {p.get('location', {}).get('ref', '?')}")
            print(f"    Source      : '{src.get('sheet', '?')}' ! {src.get('range', '?')}")
            print(f"    Row fields  : {rows}")
            print(f"    Col fields  : {cols}")
            # Show distinct members per column field
            for cx in p.get('columns', []):
                if cx.get('field') != '__VALUES__' and cx.get('members'):
                    members  = cx['members']
                    preview  = ', '.join(str(m) for m in members[:8])
                    if len(members) > 8: preview += f' … ({len(members)} total)'
                    print(f"      '{cx['field']}' members: [{preview}]")
            print(f"    Values      : {vals}")
            if calcs:
                print(f"    Calc fields : {calcs}")
            print(f"    Master links: {lnk}/{tot} fields resolved to '{args.master_sheet}'")

    dax_blocks = [p['dax'] for p in result['pivots'] if 'dax' in p]
    if dax_blocks:
        out_dax.write_text('\n'.join(dax_blocks), encoding='utf-8')
        print(f"\nDAX   → {out_dax}  ({out_dax.stat().st_size // 1024} KB)")
    else:
        print("\nNo DAX generated (no pivots found).")

    print("\nDone.")


if __name__ == '__main__':
    main()
