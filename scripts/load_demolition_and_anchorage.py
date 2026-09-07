import os
import glob
import re
import sqlite3
import pymupdf as fitz
from datetime import datetime

DB_PATH = "data/derived/star_asia_intelligence.db"

class VesselRegistry:
    def __init__(self):
        self.vessels = []
        self.aliases = {}
        self.collisions = []
        
    def resolve(self, raw_name, v_type, ldt, arr_date_iso, beach_date_iso, status):
        clean = re.sub(r'\s+', ' ', raw_name.strip().upper())
        clean = re.sub(r'[\(\[\{].*?[\)\]\}]', '', clean).strip()
        clean = re.sub(r'^\d+[\.\s\-]+', '', clean).strip()
        if not clean: clean = raw_name.strip().upper()
        
        matching_candidates = [v for v in self.vessels if v["name"] == clean]
        assigned_vessel = None
        for cand in matching_candidates:
            ldt_match = False
            if cand["ldt"] is None or cand["ldt"] == 0 or ldt is None or ldt == 0:
                ldt_match = True
            else:
                pct_diff = abs(cand["ldt"] - ldt) / max(cand["ldt"], ldt)
                abs_diff = abs(cand["ldt"] - ldt)
                if pct_diff <= 0.10 or abs_diff <= 150:
                    ldt_match = True
            if not ldt_match: continue
            if cand["beached_date"] and arr_date_iso:
                if arr_date_iso > cand["beached_date"]:
                    continue
            assigned_vessel = cand
            break
            
        if assigned_vessel is None:
            if len(matching_candidates) > 0:
                prev = matching_candidates[0]
                reason = f"LDT mismatch ({prev['ldt']} vs {ldt})" if prev['ldt'] != ldt else "Arrival after completed beaching"
                self.collisions.append({
                    "name": clean,
                    "existing_id": prev["id"],
                    "existing_ldt": prev["ldt"],
                    "new_ldt": ldt,
                    "reason": reason
                })
            new_id = len(self.vessels) + 1
            assigned_vessel = {
                "id": new_id,
                "name": clean,
                "type": v_type,
                "ldt": ldt,
                "beached_date": beach_date_iso if status == "BEACHED" else None,
                "aliases": set([clean, raw_name.strip().upper()])
            }
            self.vessels.append(assigned_vessel)
            self.aliases[clean] = new_id
            self.aliases[raw_name.strip().upper()] = new_id
        else:
            if (assigned_vessel["ldt"] is None or assigned_vessel["ldt"] == 0) and ldt:
                assigned_vessel["ldt"] = ldt
            if status == "BEACHED" and beach_date_iso:
                if assigned_vessel["beached_date"] is None or beach_date_iso < assigned_vessel["beached_date"]:
                    assigned_vessel["beached_date"] = beach_date_iso
            assigned_vessel["aliases"].add(clean)
            assigned_vessel["aliases"].add(raw_name.strip().upper())
            self.aliases[clean] = assigned_vessel["id"]
            self.aliases[raw_name.strip().upper()] = assigned_vessel["id"]
        return assigned_vessel["id"]

def parse_date_clean(raw_date_str, report_year):
    if not raw_date_str or not raw_date_str.strip():
        return None, "AWAITING", 0, ""
    s = raw_date_str.strip()
    if re.match(r'^ARRESTED$', s, re.I):
        return None, "ARRESTED", 0, ""
    awaiting_match = re.match(r'^(AWAITING|AWAIITNG|AWATIING|AWAITING\*|NA|AWATING|AWAITIING|ÀWAITING|AWAITNG|AT ANCHORAGE|AWAITED)$', s, re.I)
    if awaiting_match:
        norm = "AWAITING"
        flag = 1 if awaiting_match.group(1).upper() != "AWAITING" else 0
        note = f"Normalized '{raw_date_str}' to AWAITING" if flag else ""
        return None, norm, flag, note
        
    s_cleaned = re.sub(r'[\-\,]', '.', s)
    m = re.search(r'(\d{1,2})\.+(\d{1,2})\.+(\d{2,4})', s_cleaned)
    if not m:
        return None, "AWAITING", 1, f"Unrecognized date/status format '{s}' normalized to AWAITING"
        
    d = int(m.group(1))
    mo = int(m.group(2))
    yr = int(m.group(3))
    flag = 0
    notes = []
    if "-" in s or "," in s:
        flag = 1
        notes.append(f"Delimiter typo in '{s}' normalized to dot")
    if yr == 2206:
        yr = report_year
        flag = 1
        notes.append(f"Century typo 2206 corrected to {report_year}")
    elif yr > 2030:
        yr = report_year
        flag = 1
        notes.append(f"Invalid year {yr} corrected to {report_year}")
    elif yr < 100:
        yr = 2000 + yr
    elif yr > report_year + 1:
        yr = report_year
        flag = 1
        notes.append(f"Future year {m.group(3)} corrected to {report_year}")
        
    try:
        dt = datetime(yr, mo, d)
        iso_str = dt.strftime("%Y-%m-%d")
        return iso_str, "BEACHED", flag, "; ".join(notes)
    except Exception as e:
        return None, "AWAITING", 1, f"Failed to parse date '{s}' ({str(e)}), defaulted to AWAITING"

ldt_pattern = re.compile(r'^\d{1,3}(?:,\d{3})+(?:\.\d+)?$|^\d{3,6}(?:\.\d+)?$')
date_pattern = re.compile(r'^\d{1,2}[\.,\-]+\d{1,2}[\.,\-]+\d{2,4}$')

def extract_spatial_anchorage_v2(page, pno, report_year):
    blocks = page.get_text("blocks", sort=True)
    yard_headers = []
    for b in blocks:
        txt_u = b[4].strip().upper()
        if "ANCHORAGE & BEACHING" in txt_u or "ACHORAGE & BEACHING" in txt_u or any(h in txt_u for h in ["ALANG", "CHATTOGRAM", "GADDANI", "GADANI", "ALIAGA", "TURKEY"]):
            yard = None
            if "ALANG" in txt_u: yard = "ALANG"
            elif "CHATTOGRAM" in txt_u: yard = "CHATTOGRAM"
            elif "GADDANI" in txt_u or "GADANI" in txt_u: yard = "GADDANI"
            elif ("ALIAGA" in txt_u or "TURKEY" in txt_u) and report_year < 2026: yard = "ALIAGA"
            if yard: yard_headers.append((b[1], yard))
    if not yard_headers: return []
    yard_headers.sort(key=lambda x: x[0])
    tbl_blocks = [b for b in blocks if "ANCHORAGE & BEACHING" in b[4].upper() or "ACHORAGE & BEACHING" in b[4].upper()]
    
    def get_yard(y):
        ali_hdrs = [h for h in yard_headers if h[1] == "ALIAGA"]
        gad_hdrs = [h for h in yard_headers if h[1] == "GADDANI"]
        if ali_hdrs and report_year < 2026:
            if gad_hdrs:
                if len(tbl_blocks) <= 1:
                    return "ALIAGA"
                else:
                    t2_y = tbl_blocks[1][1]
                    if y >= t2_y - 20: return "ALIAGA"
                    return "GADDANI"
            else:
                return "ALIAGA"
        y_match = yard_headers[0][1]
        for y_pos, yd in yard_headers:
            if y_pos <= y + 15: y_match = yd
            else: break
        return y_match
        
    records = []
    for b in blocks:
        lines = [l.strip() for l in b[4].splitlines() if l.strip()]
        if not lines: continue
        if any(h in b[4].upper() for h in ["VESSEL NAME", "BEACHING POSITION", "BUNKER PRICES", "EXCHANGE RATES"]):
            continue
        yard = get_yard(b[1])
        ldt_idx = -1
        arr_idx = -1
        for idx, l in enumerate(lines):
            if ldt_pattern.match(l) and ldt_idx == -1: ldt_idx = idx
            elif date_pattern.match(l) and arr_idx == -1: arr_idx = idx
        if ldt_idx != -1 and arr_idx != -1:
            v_name = lines[0] if ldt_idx > 0 else "UNKNOWN"
            v_type = lines[1] if ldt_idx > 1 else "UNKNOWN"
            ldt_val = int(round(float(lines[ldt_idx].replace(",", ""))))
            arr_str = lines[arr_idx]
            beach_str = lines[arr_idx + 1] if arr_idx + 1 < len(lines) else "AWAITING"
            records.append({
                "vessel_name": v_name, "vessel_type": v_type, "ldt": ldt_val,
                "raw_arrival": arr_str, "raw_beaching": beach_str,
                "yard": yard, "page_num": pno
            })
            
    tabs = page.find_tables()
    for t in tabs.tables:
        df = t.extract()
        ty0 = t.bbox[1]
        yd = get_yard(ty0)
        for r in df:
            non_empty = [str(c).strip() for c in r if c and str(c).strip() and str(c).strip() != "-"]
            if len(non_empty) < 3: continue
            if any(h in " ".join(non_empty).upper() for h in ["VESSEL", "PAGE", "LDT", "BUNKER", "EXCHANGE"]): continue
            l_idx = -1
            a_idx = -1
            for idx, c in enumerate(non_empty):
                if ldt_pattern.match(c) and l_idx == -1: l_idx = idx
                elif date_pattern.match(c) and a_idx == -1: a_idx = idx
            if l_idx != -1 and a_idx != -1:
                v_name = non_empty[0] if l_idx > 0 else "UNKNOWN"
                v_type = non_empty[1] if l_idx > 1 else "UNKNOWN"
                ldt_val = int(round(float(non_empty[l_idx].replace(",", ""))))
                arr_str = non_empty[a_idx]
                beach_str = non_empty[a_idx+1] if a_idx+1 < len(non_empty) else "AWAITING"
                if not any(rec["vessel_name"] == v_name and rec["ldt"] == ldt_val for rec in records):
                    records.append({
                        "vessel_name": v_name, "vessel_type": v_type, "ldt": ldt_val,
                        "raw_arrival": arr_str, "raw_beaching": beach_str,
                        "yard": yd, "page_num": pno
                    })
    return records

dest_keywords = [
    ("ALANG", ["ALANG", "INDIA"]),
    ("CHATTOGRAM", ["CHATTOGRAM", "BANGLADESH", "CTG", "CHITTAGONG"]),
    ("GADDANI", ["GADDANI", "GADANI", "PAKISTAN"]),
    ("ALIAGA", ["ALIAGA", "TURKEY", "TURKIYE"]),
    ("SINGAPORE", ["SINGAPORE"]),
    ("MALAYSIA", ["MALAYSIA"]),
    ("PHILIPPINES", ["PHILIPPINES"]),
    ("CHINA", ["CHINA", "ZHOUSHAN"]),
    ("INDONESIA", ["INDONESIA", "BATAM"]),
    ("SRI LANKA", ["SRI LANKA", "COLOMBO"]),
    ("MIDDLE EAST", ["MIDDLE EAST", "UAE", "U.A.E", "DUBAI", "KHOR FAKKAN", "OMAN", "BAHRAIN"]),
    ("SOUTH KOREA", ["KOREA", "S.KOREA", "INCHEON", "BUSAN"]),
    ("EUROPE", ["EUROPE", "ROTTERDAM", "NORWAY", "DENMARK", "NETHERLANDS"]),
    ("SUBCONTINENT", ["SUBCONTINENT", "SUB- CONTINENT", "SUB CONTINENT"]),
    ("EAST ASIA", ["HONG KONG", "TAIWAN", "KAOHSIUNG"]),
]

def determine_destination(text):
    tu = text.upper()
    for dest, kws in dest_keywords:
        if any(w in tu for w in kws): return dest
    return "UNKNOWN"

def clean_vessel_name(v_name):
    v = v_name.strip().upper()
    v = re.sub(r'[\(\[\{].*?[\)\]\}]', '', v).strip()
    v = re.sub(r'^\d+[\.\s\-]+', '', v).strip()
    return v

# --- ROBUST SALES PARSER ---
built_row_pattern = re.compile(r'^(19\d{2}|20\d{2})\s*[/]\s*([A-Za-z\.\s]+)$')
sales_price_pattern = re.compile(r'^\$?([2-8]\d{2}(?:\.\d+)?)$')

def extract_sales_from_page_section(page, pno, y_sales, issue_id):
    r_sales = fitz.Rect(0, y_sales, 612, 740)
    sales_txt = page.get_text("text", clip=r_sales)
    
    tabs = page.find_tables(clip=r_sales)
    table_fixtures = []
    for t in tabs.tables:
        df = t.extract()
        if not df: continue
        for r in df:
            clean_r = [str(c).strip().replace('\n', ' ') for c in r if c and str(c).strip()]
            if len(clean_r) < 3: continue
            if any(h in " ".join(clean_r).upper() for h in ["VESSEL NAME", "REPORTED SALES", "PRICE (US", "TOTAL", "DESTINATION", "5-YEAR", "HISTORICAL"]):
                continue
            row_str = " ".join(clean_r)
            m_ldt = re.search(r'\b(\d{1,3}(?:,\d{3})+|\d{4,6})\b', row_str)
            m_yr = re.search(r'\b(19\d{2}|20\d{2})\b', row_str)
            if not m_ldt and not m_yr: continue
            v_name = clean_vessel_name(clean_r[0])
            if not v_name or any(k in v_name for k in ["DESTINATION", "TOTAL", "SOURCE", "5-YEAR", "HISTORICAL", "VESSEL"]):
                continue
            ldt_val = int(m_ldt.group(1).replace(',', '')) if m_ldt else None
            built_yr = int(m_yr.group(1)) if m_yr else None
            built_cntry = None
            if built_yr:
                m_cntry = re.search(r'(?:19\d{2}|20\d{2})\s*[/]\s*([A-Za-z\.\s]+)', row_str)
                if m_cntry: built_cntry = m_cntry.group(1).strip().upper()
            m_pr = re.search(r'\$?([2-8]\d{2}(?:\.\d+)?)', row_str)
            price_val = float(m_pr.group(1)) if m_pr else None
            price_status = 'CONFIRMED' if price_val else ('UNDISCLOSED' if any(w in row_str.upper() for w in ['UNDISCLOSED', 'WITHHELD', 'PRIVATE', 'N/A', '-']) else 'NOT_EXTRACTED')
            v_type = 'UNKNOWN'
            for cand_type in ["WOOD CHIP CARRIER", "VLCC", "TANKER", "BULKER", "CONTAINER", "GENERAL CARGO", "REEFER", "LPG", "LNG", "FSO", "FPSO", "CAPE", "MPP"]:
                if cand_type in row_str.upper():
                    v_type = cand_type
                    break
            comm = clean_r[-1] if len(clean_r) >= 5 else ''
            dest = determine_destination(row_str)
            table_fixtures.append({
                "issue_id": issue_id, "vessel_name": v_name, "vessel_type": v_type,
                "ldt": ldt_val, "built_year": built_yr, "built_country": built_cntry,
                "price_usd_ldt": price_val, "price_status": price_status,
                "destination": dest, "comments": comm, "page_num": pno + 1
            })
            
    lines = [l.strip() for l in sales_txt.splitlines() if l.strip()]
    start_idx = 0
    for idx, l in enumerate(lines):
        if any(h in l.upper() for h in ["COMMENTS", "PRICE (US", "YEAR / BUILT"]):
            start_idx = idx + 1
            
    content_lines = lines[start_idx:]
    built_indices = [idx for idx, l in enumerate(content_lines) if built_row_pattern.match(l)]
    text_fixtures = []
    prev_fixture_end = 0
    for i, b_idx in enumerate(built_indices):
        m_b = built_row_pattern.match(content_lines[b_idx])
        built_yr = int(m_b.group(1))
        built_cntry = m_b.group(2).strip().upper()
        ldt_idx = b_idx - 1
        ldt_val = None
        for k in range(max(prev_fixture_end, b_idx - 2), b_idx):
            m_l = re.match(r'^(\d{1,3}(?:,\d{3})+|\d{4,6})$', content_lines[k])
            if m_l:
                ldt_val = int(m_l.group(1).replace(',', ''))
                ldt_idx = k
                break
        price_val = None
        price_status = 'NOT_EXTRACTED'
        price_idx = b_idx + 1
        if price_idx < len(content_lines):
            m_p = sales_price_pattern.match(content_lines[price_idx])
            if m_p:
                price_val = float(m_p.group(1))
                price_status = 'CONFIRMED'
            elif any(w in content_lines[price_idx].upper() for w in ['UNDISCLOSED', 'WITHHELD', 'PRIVATE', 'N/A', '-']):
                price_status = 'UNDISCLOSED'
                
        if i + 1 < len(built_indices):
            next_b_idx = built_indices[i+1]
            next_ldt_idx = next_b_idx - 1
            for k in range(max(b_idx, next_b_idx - 2), next_b_idx):
                if re.match(r'^(\d{1,3}(?:,\d{3})+|\d{4,6})$', content_lines[k]):
                    next_ldt_idx = k
                    break
            inter_tokens = content_lines[price_idx + 1 : next_ldt_idx]
            comm_tokens = []
            parsing_comm = True
            for tok in inter_tokens:
                tu = tok.upper()
                if parsing_comm and (any(w in tu for w in ["DELIVERED", "AS IS", "ABOUT", "ROBS", "INCLUDED", "OPTION", "CFR", "FOB", "WITH", "TONS", "BUNKERS", "CHATTOGRAM", "ALANG", "GADDANI", "GADANI", "ALIAGA", "INDIA", "PAKISTAN", "BANGLADESH"]) or tok.endswith(".")):
                    comm_tokens.append(tok)
                else:
                    parsing_comm = False
            comments = " ".join(comm_tokens).strip()
            fixture_end = price_idx + 1 + len(comm_tokens)
        else:
            comm_tokens = content_lines[price_idx + 1 :]
            comments = " ".join(comm_tokens).strip()
            fixture_end = len(content_lines)
            
        name_type_tokens = content_lines[prev_fixture_end:ldt_idx]
        type_words = []
        name_words = []
        is_type = True
        for tok in reversed(name_type_tokens):
            if is_type and any(tw in tok.upper() for tw in ["CARRIER", "CHIP", "WOOD", "TANKER", "BULKER", "CARGO", "GENERAL", "CONTAINER", "FEEDER", "REEFER", "LPG", "LNG", "FPSO", "CAPE", "MPP", "VLCC"]):
                type_words.insert(0, tok)
            else:
                is_type = False
                name_words.insert(0, tok)
                
        v_name = " ".join(name_words).strip().upper()
        v_type = " ".join(type_words).strip().upper()
        if v_name and ldt_val:
            text_fixtures.append({
                "issue_id": issue_id, "vessel_name": clean_vessel_name(v_name),
                "vessel_type": v_type or "UNKNOWN", "ldt": ldt_val,
                "built_year": built_yr, "built_country": built_cntry,
                "price_usd_ldt": price_val, "price_status": price_status,
                "destination": determine_destination(comments + " " + v_type),
                "comments": comments, "page_num": pno + 1
            })
        prev_fixture_end = fixture_end
        
    if len(table_fixtures) >= len(text_fixtures) and len(table_fixtures) > 0:
        return table_fixtures
    return text_fixtures

def extract_page10_tables(doc, issue_id):
    snapshot_rows = []
    historical_rows = []
    sales_rows = []
    sentiments_map = {}
    price_rng_pat = re.compile(r'(\d{3})\s*[\~\–\-\—]\s*(\d{3})')
    
    for pno in range(min(12, len(doc))):
        page = doc[pno]
        blocks = page.get_text("blocks", sort=True)
        y_snapshot = None
        y_hist = None
        y_sales = None
        for b in blocks:
            tu = b[4].strip().upper()
            if "CURRENT MARKET SNAPSHOT" in tu or "SHIP RECYCLING MARKET SNAPSHOT" in tu:
                if y_snapshot is None: y_snapshot = b[1]
            elif "HISTORICAL AVERAGE" in tu or "5-YEAR" in tu:
                if y_hist is None: y_hist = b[1]
            elif "REPORTED SALES" in tu or "SHIPS SOLD FOR RECYCLING" in tu:
                if y_sales is None: y_sales = b[1]
                
        # 1. Snapshot
        if y_snapshot is not None and not snapshot_rows:
            y_next = min([y for y in [y_hist, y_sales, 740.0] if y is not None and y > y_snapshot + 10] or [740.0])
            r_snap = fitz.Rect(0, y_snapshot, 612, y_next)
            snap_txt = page.get_text("text", clip=r_snap)
            curr_yd = None
            for l in snap_txt.splitlines():
                lu = l.strip().upper()
                if "ALANG" in lu: curr_yd = "ALANG"
                elif "CHATTOGRAM" in lu: curr_yd = "CHATTOGRAM"
                elif "GADDANI" in lu or "GADANI" in lu: curr_yd = "GADDANI"
                elif "TURKEY" in lu or "ALIAGA" in lu: curr_yd = "ALIAGA"
                for sent_word in ["STABLE", "WEAK", "BULLISH", "IMPROVING", "STEADY", "SOFT", "QUIET", "SLOW"]:
                    if sent_word in lu and curr_yd and curr_yd not in sentiments_map:
                        sentiments_map[curr_yd] = sent_word
                        break
            curr_yd = None
            yard_ranges = {}
            for l in snap_txt.splitlines():
                lu = l.strip().upper()
                if any(k in lu for k in ["NON-EU", "APPROX", "PREMIUMS", "DISPLACEMENT", "REPORTED ARE NET"]):
                    continue
                if "ALANG" in lu:
                    curr_yd = "ALANG"
                    if curr_yd not in yard_ranges: yard_ranges[curr_yd] = []
                elif "CHATTOGRAM" in lu:
                    curr_yd = "CHATTOGRAM"
                    if curr_yd not in yard_ranges: yard_ranges[curr_yd] = []
                elif "GADDANI" in lu or "GADANI" in lu:
                    curr_yd = "GADDANI"
                    if curr_yd not in yard_ranges: yard_ranges[curr_yd] = []
                elif "TURKEY" in lu or "ALIAGA" in lu:
                    curr_yd = "ALIAGA"
                    if curr_yd not in yard_ranges: yard_ranges[curr_yd] = []
                elif curr_yd:
                    ranges = price_rng_pat.findall(l)
                    if ranges:
                        yard_ranges[curr_yd].extend(ranges)
            cats = ["TANKERS", "BULKERS", "GENERAL_CARGO", "CONTAINERS"]
            for yd, rngs in yard_ranges.items():
                if len(rngs) >= 4:
                    for c, (lo, hi) in zip(cats, rngs[:4]):
                        snapshot_rows.append((issue_id, yd, c, float(lo), float(hi)))
                        
        # 2. Historical
        if y_hist is not None and not historical_rows:
            y_next = min([y for y in [y_sales, 740.0] if y is not None and y > y_hist + 10] or [740.0])
            hist_blocks = [b for b in blocks if y_hist <= b[1] < y_next]
            years = []
            for b in hist_blocks:
                tu = b[4].strip().upper()
                y_cands = re.findall(r'\b(20[12]\d)\b', tu)
                if len(y_cands) >= 4:
                    years = y_cands
                    break
            if years:
                for b in hist_blocks:
                    txt_b = b[4].strip()
                    tu = txt_b.upper()
                    yd = None
                    if "ALANG" in tu: yd = "ALANG"
                    elif "CHATTOGRAM" in tu or "BANGLADESH" in tu: yd = "CHATTOGRAM"
                    elif "GADDANI" in tu or "GADANI" in tu or "PAKISTAN" in tu: yd = "GADDANI"
                    elif "ALIAGA" in tu or "TURKEY" in tu: yd = "ALIAGA"
                    if yd:
                        prices = [float(v) for v in re.findall(r'\b\d{3}\b', txt_b) if 100 <= float(v) <= 900]
                        if len(prices) >= len(years):
                            for yr_lbl, pr in zip(years, prices[:len(years)]):
                                historical_rows.append((issue_id, yd, yr_lbl, pr))
                                
        # 3. Reported Sales
        if y_sales is not None:
            fixtures = extract_sales_from_page_section(page, pno, y_sales, issue_id)
            sales_rows.extend(fixtures)
            
    return snapshot_rows, historical_rows, sales_rows, sentiments_map

def extract_yard_commentary(doc):
    yard_comments = {}
    for pno in range(9, len(doc)):
        blocks = doc[pno].get_text("blocks", sort=True)
        for b in blocks:
            tu = b[4].strip().upper()
            if len(tu) > 100:
                if any(w in tu for w in ["INDIA", "ALANG", "GUJARAT"]) and "ALANG" not in yard_comments:
                    yard_comments["ALANG"] = b[4].strip()
                elif any(w in tu for w in ["BANGLADESH", "CHATTOGRAM"]) and "CHATTOGRAM" not in yard_comments:
                    yard_comments["CHATTOGRAM"] = b[4].strip()
                elif any(w in tu for w in ["PAKISTAN", "GADANI", "GADDANI"]) and "GADDANI" not in yard_comments:
                    yard_comments["GADDANI"] = b[4].strip()
                elif any(w in tu for w in ["TURKEY", "ALIAGA", "TURKISH", "TURKIYE"]) and "ALIAGA" not in yard_comments:
                    yard_comments["ALIAGA"] = b[4].strip()
    return yard_comments

# ==================== MACRO TABLES EXTRACTORS ====================

MONTHS_MAP = {
    'JAN': 1, 'JANUARY': 1, 'FEB': 2, 'FEBRUARY': 2, 'MAR': 3, 'MARCH': 3,
    'APR': 4, 'APRIL': 4, 'MAY': 5, 'JUN': 6, 'JUNE': 6, 'JUL': 7, 'JULY': 7,
    'AUG': 8, 'AUGUST': 8, 'SEP': 9, 'SEPTEMBER': 9, 'OCT': 10, 'OCTOBER': 10,
    'NOV': 11, 'NOVEMBER': 11, 'DEC': 12, 'DECEMBER': 12
}

def parse_number(s):
    if s is None: return None
    s_str = str(s).replace(',', '').strip()
    m = re.search(r'([+-]?\d+(?:\.\d+)?)', s_str)
    return float(m.group(1)) if m else None

def parse_header_date(s):
    su = str(s).upper().strip()
    m_found = None
    for m_name, m_num in MONTHS_MAP.items():
        if re.search(r'\b' + m_name + r'\b', su):
            if m_found is None or len(m_name) > len(m_found[0]):
                m_found = (m_name, m_num)
    if not m_found: return None
    month = m_found[1]
    m_day = re.search(r'\b(\d{1,2})(?:ST|ND|RD|TH)?\b', su)
    day = int(m_day.group(1)) if m_day else 1
    return (month, day)

def determine_current_col(header_tokens):
    date_cols = []
    for idx, tok in enumerate(header_tokens):
        pd = parse_header_date(tok)
        if pd: date_cols.append((idx, pd, tok))
            
    if len(date_cols) >= 2:
        c1_idx, (m1, d1), t1 = date_cols[0]
        c2_idx, (m2, d2), t2 = date_cols[1]
        if m1 == 1 and m2 == 12: return c1_idx, c2_idx
        elif m1 == 12 and m2 == 1: return c2_idx, c1_idx
        elif m1 > m2 or (m1 == m2 and d1 > d2): return c1_idx, c2_idx
        else: return c2_idx, c1_idx
    return 1, 2

def extract_baltic_indices(doc, issue_id):
    results = []
    
    for pno in range(min(8, len(doc))):
        page = doc[pno]
        txt = page.get_text()
        txt_u = txt.upper()
        if not any(k in txt_u for k in ["BDI", "BCI", "BDTI", "BCTI", "BALTIC"]):
            continue
            
        tabs = page.find_tables()
        for t in tabs.tables:
            ext = t.extract()
            if not ext: continue
            
            # Format A: Modern horizontal layout (2026 late)
            for r_idx, r in enumerate(ext):
                ne = [str(c).strip() for c in r if c and str(c).strip()]
                indices = [c for c in ne if c in ['BDI', 'BCI', 'BPI', 'BSI', 'BHSI', 'BDTI', 'BCTI']]
                if len(indices) >= 2:
                    vals, wows, yoys = [], [], []
                    for nxt_r in ext[r_idx+1:r_idx+6]:
                        nxt_ne = [str(c).strip() for c in nxt_r if c and str(c).strip()]
                        if not nxt_ne: continue
                        if not vals and any(re.search(r'^\d[\d,]*$', c) for c in nxt_ne):
                            vals = nxt_ne
                        elif vals and not wows and any('WOW' in c.upper() or ('%' in c and 'YOY' not in c.upper()) for c in nxt_ne):
                            wows = nxt_ne
                        elif vals and wows and not yoys and any('YOY' in c.upper() or '%' in c for c in nxt_ne):
                            yoys = nxt_ne
                            break
                    if len(vals) == len(indices):
                        for idx, name in enumerate(indices):
                            v = parse_number(vals[idx])
                            w = parse_number(wows[idx]) if idx < len(wows) else None
                            y = parse_number(yoys[idx]) if idx < len(yoys) else None
                            chg = round(v - v / (1.0 + w / 100.0), 2) if (v and w) else None
                            w_str = f"{w:+.2f}%" if w is not None else "N/A"
                            y_str = f"{y:+.2f}%" if y is not None else "N/A"
                            results.append({
                                "issue_id": issue_id,
                                "index_name": name,
                                "current_value": v,
                                "change_val": chg,
                                "change_pct": w,
                                "yoy_pct": y,
                                "raw_text": f"{name}: {v:,.0f} (WoW: {w_str}, YoY: {y_str})"
                            })
                            
            # Format B: Tabular layout (2022-2025)
            for row in ext:
                ne = [str(c).strip() for c in row if c and str(c).strip()]
                if not ne: continue
                for idx_name in ['BDI', 'BCI', 'BPI', 'BSI', 'BHSI', 'BDTI', 'BCTI']:
                    if idx_name in ne:
                        pos = ne.index(idx_name)
                        if pos + 1 < len(ne):
                            val = parse_number(ne[pos+1])
                            if val is not None and val > 0:
                                lw_val = parse_number(ne[pos+2]) if pos + 2 < len(ne) else None
                                pct_tokens = [c for c in ne[pos+2:] if '%' in c or (parse_number(c) is not None and abs(parse_number(c)) <= 150)]
                                wow = parse_number(pct_tokens[0]) if len(pct_tokens) >= 1 else None
                                yoy = parse_number(pct_tokens[1]) if len(pct_tokens) >= 2 else None
                                
                                chg = round(val - lw_val, 2) if lw_val is not None else None
                                if chg is None and wow is not None:
                                    chg = round(val - val / (1.0 + wow / 100.0), 2)
                                    
                                w_str = f"{wow:+.2f}%" if wow is not None else "N/A"
                                y_str = f"{yoy:+.2f}%" if yoy is not None else "N/A"
                                results.append({
                                    "issue_id": issue_id,
                                    "index_name": idx_name,
                                    "current_value": val,
                                    "change_val": chg,
                                    "change_pct": wow,
                                    "yoy_pct": yoy,
                                    "raw_text": f"{idx_name}: {val:,.0f} (WoW: {w_str}, YoY: {y_str})"
                                })
                                
    # Fallback Format C: Text blocks
    if not results:
        for pno in range(min(8, len(doc))):
            page = doc[pno]
            for b in page.get_text("blocks"):
                lines = [l.strip() for l in b[4].split("\n") if l.strip()]
                for idx_name in ['BDI', 'BCI', 'BPI', 'BSI', 'BHSI', 'BDTI', 'BCTI']:
                    if lines and lines[0].upper() == idx_name and len(lines) >= 2:
                        val = parse_number(lines[1])
                        if val:
                            wow = parse_number(lines[2]) if len(lines) >= 3 else None
                            yoy = parse_number(lines[3]) if len(lines) >= 4 else None
                            chg = round(val - val / (1.0 + wow / 100.0), 2) if (val and wow) else None
                            w_str = f"{wow:+.2f}%" if wow is not None else "N/A"
                            y_str = f"{yoy:+.2f}%" if yoy is not None else "N/A"
                            results.append({
                                "issue_id": issue_id,
                                "index_name": idx_name,
                                "current_value": val,
                                "change_val": chg,
                                "change_pct": wow,
                                "yoy_pct": yoy,
                                "raw_text": f"{idx_name}: {val:,.0f} (WoW: {w_str}, YoY: {y_str})"
                            })
                            
    unique = {}
    for r in results:
        k = r["index_name"]
        if k not in unique:
            unique[k] = r
    return list(unique.values())

def extract_bunker_and_fx(doc, issue_id):
    bunkers = []
    fx_rates = []
    known_ports = ['SINGAPORE', 'HONG KONG', 'FUJAIRAH', 'ROTTERDAM', 'HOUSTON']
    pairs_map = {
        'CNY': 'USD/CNY',
        'BDT': 'USD/BDT',
        'INR': 'USD/INR',
        'PKR': 'USD/PKR',
        'TRY': 'USD/TRY'
    }
    
    start_p = max(0, len(doc) - 7)
    for pno in range(start_p, len(doc)):
        txt = doc[pno].get_text()
        txt_u = txt.upper()
        if "BUNKER PRICES" not in txt_u and "EXCHANGE RATES" not in txt_u:
            continue
        
        tabs = doc[pno].find_tables()
        for t in tabs.tables:
            ext = t.extract()
            if not ext: continue
            
            mode = None
            grade_cols = {}
            fx_curr_col = 1
            fx_prev_col = 2
            
            for r in ext:
                ne = [str(c).strip() for c in r if c and str(c).strip()]
                if not ne: continue
                r_str = " ".join(ne).upper()
                
                if "BUNKER" in r_str:
                    mode = 'BUNKER'
                    continue
                elif "EXCHANGE" in r_str:
                    mode = 'FX'
                    continue
                elif any(k in r_str for k in ["COMMODIT", "CRUDE OIL", "VESSEL NAME", "REPORTED FIXTURES"]):
                    mode = None
                    continue
                    
                if mode == 'BUNKER' or any(g in r_str for g in ["VLSFO", "HSFO", "IFO380", "MGO"]):
                    col_map = {}
                    for c_idx, c in enumerate(ne):
                        cu = c.upper()
                        if "VLSFO" in cu: col_map["VLSFO"] = c_idx
                        elif "HSFO" in cu or "IFO380" in cu or "380" in cu: col_map["HSFO"] = c_idx
                        elif "MGO" in cu: col_map["MGO"] = c_idx
                    if len(col_map) >= 2:
                        grade_cols = col_map
                        mode = 'BUNKER'
                        continue
                        
                if mode == 'FX' or any(c in r_str for c in ["CURRENCY", "W-O-W %", "WOW %"]):
                    if any(parse_header_date(c) for c in ne):
                        fx_curr_col, fx_prev_col = determine_current_col(ne)
                        mode = 'FX'
                        continue
                        
                if mode == 'BUNKER':
                    port_match = None
                    for kp in known_ports:
                        if kp in ne[0].upper():
                            port_match = kp
                            break
                    if port_match and len(ne) >= 4:
                        vlsfo_val = parse_number(ne[grade_cols.get("VLSFO", 1)]) if len(ne) > grade_cols.get("VLSFO", 1) else None
                        hsfo_val = parse_number(ne[grade_cols.get("HSFO", 2)]) if len(ne) > grade_cols.get("HSFO", 2) else None
                        mgo_val = parse_number(ne[grade_cols.get("MGO", 3)]) if len(ne) > grade_cols.get("MGO", 3) else None
                        
                        if vlsfo_val and 100 <= vlsfo_val <= 2500:
                            bunkers.append({"issue_id": issue_id, "port": port_match, "fuel_grade": "VLSFO", "price_usd_mt": vlsfo_val, "change_val": None})
                        if hsfo_val and 100 <= hsfo_val <= 2500:
                            bunkers.append({"issue_id": issue_id, "port": port_match, "fuel_grade": "HSFO", "price_usd_mt": hsfo_val, "change_val": None})
                        if mgo_val and 100 <= mgo_val <= 2500:
                            bunkers.append({"issue_id": issue_id, "port": port_match, "fuel_grade": "MGO", "price_usd_mt": mgo_val, "change_val": None})
                            
                elif mode == 'FX':
                    matched_pair = None
                    for c_code, pair in pairs_map.items():
                        if c_code in ne[0].upper():
                            matched_pair = pair
                            break
                    if matched_pair and len(ne) >= 3:
                        nums = [parse_number(x) for x in ne[1:] if parse_number(x) is not None]
                        curr_rate = parse_number(ne[fx_curr_col]) if fx_curr_col < len(ne) else (nums[0] if nums else None)
                        prev_rate = parse_number(ne[fx_prev_col]) if fx_prev_col < len(ne) else (nums[1] if len(nums) > 1 else None)
                        
                        pct_val = None
                        for x in ne[1:]:
                            if '%' in str(x):
                                pct_val = parse_number(x)
                                break
                                
                        chg_val = round(curr_rate - prev_rate, 4) if (curr_rate and prev_rate) else None
                        if curr_rate:
                            fx_rates.append({
                                "issue_id": issue_id,
                                "currency_pair": matched_pair,
                                "rate": curr_rate,
                                "change_val": chg_val,
                                "change_pct": pct_val
                            })

    unique_b = {}
    for b in bunkers: unique_b[(b["port"], b["fuel_grade"])] = b
    unique_f = {}
    for f in fx_rates: unique_f[f["currency_pair"]] = f
    return list(unique_b.values()), list(unique_f.values())

def extract_commodity_rates(doc, issue_id):
    records = []
    start_p = max(0, len(doc) - 7)
    for pno in range(start_p, len(doc)):
        txt = doc[pno].get_text()
        txt_u = txt.upper()
        if not any(k in txt_u for k in ["COPPER (COMEX)", "BRENT CRUDE", "WTI CRUDE", "NATURAL GAS", "3MO COPPER", "IRON ORE"]):
            continue
            
        tabs = doc[pno].find_tables()
        for t in tabs.tables:
            ext = t.extract()
            if not ext: continue
            
            for r in ext:
                non_empty = [str(c).strip() for c in r if c and str(c).strip()]
                if not non_empty: continue
                r_str = " ".join(non_empty).upper()
                if "INDEX" in r_str and "PRICE" in r_str: continue
                
                # 1. Metals
                if "COPPER (COMEX)" in r_str or ("COPPER" in r_str and "COMEX" in r_str):
                    m_contract = re.search(r'([A-Z]{3}\s*202\d)', r_str)
                    contract = m_contract.group(1) if m_contract else None
                    price_val = None
                    raw_unit = "USD / lb."
                    change_val = None
                    change_pct = None
                    for c in non_empty[1:]:
                        if "USD" in c.upper() or "LB" in c.upper() or "MT" in c.upper():
                            raw_unit = c
                        elif '%' in c:
                            change_pct = parse_number(c)
                        else:
                            num = parse_number(c)
                            if num is not None:
                                if price_val is None and num > 50:
                                    price_val = num
                                elif price_val is not None and change_val is None:
                                    change_val = num
                    if price_val:
                        derived_price = round(price_val / 100.0, 4)
                        records.append({
                            "issue_id": issue_id,
                            "commodity_category": "Industrial Metals",
                            "item_name": "Copper (Comex)",
                            "price_usd": derived_price,
                            "unit": "USD / lb.",
                            "raw_price": price_val,
                            "raw_unit": raw_unit,
                            "contract": contract,
                            "change_val": change_val,
                            "change_pct": change_pct
                        })
                elif "BRENT CRUDE" in r_str:
                    m_contract = re.search(r'([A-Z]{3}\s*202\d)', r_str)
                    contract = m_contract.group(1) if m_contract else None
                    price_val = None
                    change_val = None
                    change_pct = None
                    raw_unit = "USD / bbl."
                    for c in non_empty[1:]:
                        if "USD" in c.upper() or "BBL" in c.upper():
                            raw_unit = c
                        elif '%' in c:
                            change_pct = parse_number(c)
                        else:
                            num = parse_number(c)
                            if num is not None:
                                if price_val is None and 10 <= num <= 250:
                                    price_val = num
                                elif price_val is not None and change_val is None:
                                    change_val = num
                    if price_val:
                        records.append({
                            "issue_id": issue_id,
                            "commodity_category": "Crude Oil & Natural Gas",
                            "item_name": "Brent Crude (ICE)",
                            "price_usd": price_val,
                            "unit": "USD / bbl.",
                            "raw_price": price_val,
                            "raw_unit": raw_unit,
                            "contract": contract,
                            "change_val": change_val,
                            "change_pct": change_pct
                        })
                elif "WTI CRUDE" in r_str:
                    m_contract = re.search(r'([A-Z]{3}\s*202\d)', r_str)
                    contract = m_contract.group(1) if m_contract else None
                    price_val = None
                    change_val = None
                    change_pct = None
                    for c in non_empty[1:]:
                        if '%' in c:
                            change_pct = parse_number(c)
                        else:
                            num = parse_number(c)
                            if num is not None:
                                if price_val is None and 10 <= num <= 250:
                                    price_val = num
                                elif price_val is not None and change_val is None:
                                    change_val = num
                    if price_val:
                        records.append({
                            "issue_id": issue_id,
                            "commodity_category": "Crude Oil & Natural Gas",
                            "item_name": "WTI Crude Oil (Nymex)",
                            "price_usd": price_val,
                            "unit": "USD / bbl.",
                            "raw_price": price_val,
                            "raw_unit": "USD / bbl.",
                            "contract": contract,
                            "change_val": change_val,
                            "change_pct": change_pct
                        })
                elif "NATURAL GAS" in r_str:
                    m_contract = re.search(r'([A-Z]{3}\s*202\d)', r_str)
                    contract = m_contract.group(1) if m_contract else None
                    price_val = None
                    change_val = None
                    change_pct = None
                    for c in non_empty[1:]:
                        if '%' in c:
                            change_pct = parse_number(c)
                        else:
                            num = parse_number(c)
                            if num is not None:
                                if price_val is None and 0.5 <= num <= 50:
                                    price_val = num
                                elif price_val is not None and change_val is None:
                                    change_val = num
                    if price_val:
                        records.append({
                            "issue_id": issue_id,
                            "commodity_category": "Crude Oil & Natural Gas",
                            "item_name": "Natural Gas (Nymex)",
                            "price_usd": price_val,
                            "unit": "USD / MMBtu",
                            "raw_price": price_val,
                            "raw_unit": "USD / MMBtu",
                            "contract": contract,
                            "change_val": change_val,
                            "change_pct": change_pct
                        })
                elif any(m_name in r_str for m_name in ["3MO COPPER", "3MO ALUMIN", "3MO ZINC", "3MO TIN"]):
                    m_label = None
                    if "COPPER" in r_str: m_label = "3Mo Copper (LME)"
                    elif "ALUMIN" in r_str: m_label = "3Mo Aluminium (LME)"
                    elif "ZINC" in r_str: m_label = "3Mo Zinc (LME)"
                    elif "TIN" in r_str: m_label = "3Mo Tin (LME)"
                    
                    price_val = None
                    change_val = None
                    change_pct = None
                    for c in non_empty[1:]:
                        if '%' in c:
                            change_pct = parse_number(c)
                        else:
                            num = parse_number(c)
                            if num is not None:
                                if price_val is None and num > 500:
                                    price_val = num
                                elif price_val is not None and change_val is None:
                                    change_val = num
                    if price_val and m_label:
                        records.append({
                            "issue_id": issue_id,
                            "commodity_category": "Industrial Metals",
                            "item_name": m_label,
                            "price_usd": price_val,
                            "unit": "USD / MT",
                            "raw_price": price_val,
                            "raw_unit": "USD / MT",
                            "contract": "N/A",
                            "change_val": change_val,
                            "change_pct": change_pct
                        })

    unique = {}
    for r in records:
        k = r["item_name"]
        if k not in unique: unique[k] = r
    return list(unique.values())


# Valuations Extractor
KNOWN_VAL_CLASSES = {
    "DRY BULK": ["CAPESIZE", "CAPE", "KAMSARMAX", "KMAX", "PANAMAX", "PMAX", "ULTRAMAX", "UMAX", "SUPRAMAX", "SMAX", "HANDYSIZE", "HANDYMAX", "HANDY"],
    "TANKERS": ["VLCC", "SUEZMAX", "SUEZ", "AFRAMAX", "AFRA", "PANAMAX-LR1", "LR1", "LR2", "MR TANKER", "MR"],
    "CONTAINERS": ["FEEDER", "900", "1,600", "1600", "1,700", "2,700", "2700", "2,750", "5,100", "5100", "5,500", "5500", "6,500", "8,500", "9,000"]
}

def clean_val_class(name, sector):
    nu = name.upper().strip()
    if sector == "DRY BULK":
        if "CAPE" in nu: return "CAPESIZE"
        if "KAMSAR" in nu: return "KAMSARMAX"
        if "PANAMAX" in nu or nu == "PMAX": return "PANAMAX"
        if "ULTRAMAX" in nu or nu == "UMAX": return "ULTRAMAX"
        if "SUPRAMAX" in nu or nu == "SMAX": return "SUPRAMAX"
        if "HANDY" in nu: return "HANDYSIZE"
    elif sector == "TANKERS":
        if "VLCC" in nu: return "VLCC"
        if "SUEZ" in nu: return "SUEZMAX"
        if "AFRA" in nu: return "AFRAMAX"
        if "LR1" in nu or "PANAMAX-LR1" in nu: return "LR1"
        if "LR2" in nu: return "LR2"
        if "MR" in nu: return "MR"
    elif sector == "CONTAINERS":
        m = re.search(r'(\d[\d,]*\s*[\-\–\~\—]\s*\d[\d,]*)', nu)
        if m:
            clean_r = re.sub(r'\s+', ' ', m.group(1)).replace('~', '-').replace('–', '-').replace('—', '-')
            return f"{clean_r} TEU"
        if "FEEDER" in nu: return "FEEDER"
        m2 = re.search(r'(\d[\d,]*)\s*TEU', nu)
        if m2: return f"{m2.group(1)} TEU"
    return nu

def extract_vessel_valuations(doc, issue_id):
    valuations = []
    
    for pno in range(min(12, len(doc))):
        page = doc[pno]
        txt = page.get_text()
        txt_u = txt.upper()
        
        sector = None
        if "DRY BULK" in txt_u or "BULKER" in txt_u or "BALTIC DRY" in txt_u:
            if pno in [1, 2, 3, 4]: sector = "DRY BULK"
        if "TANKER" in txt_u:
            if pno in [4, 5, 6, 7]: sector = "TANKERS"
        if "CONTAINER" in txt_u:
            if pno in [6, 7, 8, 9, 10]: sector = "CONTAINERS"
                
        if not sector: continue
        
        has_20y = "20 YEARS" in txt_u or "20 YRS" in txt_u
        age_cats = ['NB CONTRACT', 'NB PROMPT', '5 YEARS', '10 YEARS', '20 YEARS' if has_20y else '15 YEARS']
        
        blocks = page.get_text("blocks", sort=True)
        for b in blocks:
            lines = [l.strip() for l in b[4].split("\n") if l.strip()]
            if not lines: continue
            
            first_line = lines[0].upper()
            is_match = False
            for kc in KNOWN_VAL_CLASSES[sector]:
                if kc in first_line:
                    if len(first_line.split()) <= 3:
                        is_match = True
                        break
            if not is_match: continue
            
            prices = []
            for l in lines[1:]:
                if re.match(r'^\d{2,3},\d{3}$', l):
                    continue
                if l.upper() in ["GEARED", "GEARLESS"]:
                    continue
                m = re.search(r'^\$?(\d+(?:\.\d+)?)\s*M?(?:\s*\([A-Z0-9]+\))?$', l, re.I)
                if m:
                    val = float(m.group(1))
                    if 1.0 <= val <= 350.0:
                        prices.append(val)
                elif l in ['-', 'N/A', 'NA', '–', '—']:
                    prices.append(None)
                    
            if len(prices) >= 4:
                v_class = clean_val_class(lines[0], sector)
                for idx, p in enumerate(prices[:len(age_cats)]):
                    if p is not None:
                        valuations.append({
                            "issue_id": issue_id,
                            "sector": sector,
                            "vessel_class": v_class,
                            "age_category": age_cats[idx],
                            "value_usd_m": p
                        })

    unique = {}
    for v in valuations:
        k = (v["sector"], v["vessel_class"], v["age_category"])
        if k not in unique:
            unique[k] = v
    return list(unique.values())


# Time Charter Extractor
def clean_tc_class(name):
    if not name: return None, None
    nu = str(name).upper().strip()
    if any(k in nu for k in ["TYPE", "DWT", "VESSEL", "DELIVERY", "SOURCE", "NOTE", "AMOUNT", "CHANGE"]):
        return None, None
        
    if "CAPE" in nu: return "CAPESIZE", "DRY BULK"
    if "KAMSAR" in nu: return "KAMSARMAX", "DRY BULK"
    if "PANAMAX" in nu: return "PANAMAX", "DRY BULK"
    if "ULTRAMAX" in nu: return "ULTRAMAX", "DRY BULK"
    if "SUPRAMAX" in nu: return "SUPRAMAX", "DRY BULK"
    if "HANDY" in nu: return "HANDYSIZE", "DRY BULK"
    
    if "VLCC" in nu: return "VLCC", "TANKERS"
    if "SUEZ" in nu: return "SUEZMAX", "TANKERS"
    if "AFRA" in nu: return "AFRAMAX", "TANKERS"
    if "LR1" in nu: return "LR1", "TANKERS"
    if "LR2" in nu: return "LR2", "TANKERS"
    if "MR" in nu: return "MR", "TANKERS"
    
    m = re.search(r'(\d[\d,]*)\s*TEU', nu)
    if m: return f"{m.group(1)} TEU", "CONTAINERS"
    
    return None, None

def parse_dollar(s):
    if not s: return None
    s = str(s).strip()
    if '%' in s: return None
    clean = re.sub(r'[\$,\s]', '', s)
    m = re.match(r'^(\d+(?:\.\d+)?)$', clean)
    if m:
        val = float(m.group(1))
        if 1500 <= val <= 300000: # daily TC rates $1.5k - $300k
            return val
    return None

def extract_tc_averages(doc, issue_id):
    records = []
    
    for pno in range(len(doc)):
        page = doc[pno]
        txt = page.get_text()
        txt_u = txt.upper()
        
        # Format A: Modern 2026 Year columns (SEGMENT (AVG))
        if "SEGMENT (AVG)" in txt_u and any(yr in txt_u for yr in ['2022', '2023', '2024', '2025']):
            lines = [l.strip() for l in txt.split("\n") if l.strip()]
            start_idx = -1
            for idx, l in enumerate(lines):
                if "SEGMENT (AVG)" in l.upper():
                    start_idx = idx
                    break
            if start_idx != -1:
                year_headers = []
                data_start_idx = start_idx + 1
                for idx in range(start_idx + 1, min(start_idx + 10, len(lines))):
                    m = re.match(r'^(202\d(?:\s*YTD)?)$', lines[idx].upper())
                    if m:
                        year_headers.append(m.group(1))
                        data_start_idx = idx + 1
                    elif year_headers:
                        break
                        
                if not year_headers:
                    year_headers = ['2022', '2023', '2024', '2025', '2026 YTD']
                    
                i = data_start_idx
                while i < len(lines):
                    v_class, sector = clean_tc_class(lines[i])
                    if v_class:
                        rates = []
                        j = i + 1
                        while j < len(lines) and len(rates) < len(year_headers):
                            nxt_class, _ = clean_tc_class(lines[j])
                            if nxt_class: break
                            if any(k in lines[j].upper() for k in ["PAGE", "STAR ASIA", "MEMBER OF", "SNP@"]): break
                            r = parse_dollar(lines[j])
                            if r is not None:
                                rates.append(r)
                            j += 1
                        if len(rates) == len(year_headers):
                            for yr_idx, yr in enumerate(year_headers):
                                records.append({
                                    "issue_id": issue_id,
                                    "sector": sector,
                                    "vessel_class": v_class,
                                    "duration": yr,
                                    "rate_usd_day": rates[yr_idx]
                                })
                            i = j
                            continue
                    i += 1
                    
        # Format B: 2022-2025 12 Months T/C Rates via find_tables
        if "12 MONTHS T/C" in txt_u or "T/C RATES AVERAGE" in txt_u:
            tabs = page.find_tables()
            for t in tabs.tables:
                ext = t.extract()
                if not ext: continue
                for row in ext:
                    ne = [c.strip() for c in row if c and c.strip()]
                    if len(ne) >= 3:
                        v_class, sector = clean_tc_class(ne[0])
                        if v_class and sector:
                            if re.match(r'^\d{2,3},\d{3}$', ne[1]):
                                rate = parse_dollar(ne[2])
                                if rate:
                                    records.append({
                                        "issue_id": issue_id,
                                        "sector": sector,
                                        "vessel_class": v_class,
                                        "duration": "12 MONTHS",
                                        "rate_usd_day": rate
                                    })

    unique = {}
    for r in records:
        k = (r["sector"], r["vessel_class"], r["duration"])
        if k not in unique:
            unique[k] = r
    return list(unique.values())

def main():
    print(f"Connecting to {DB_PATH}...")
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # Recreate tables with clean schemas
    cur.execute("DROP TABLE IF EXISTS vessels;")
    cur.execute("""
    CREATE TABLE vessels (
        vessel_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(128) NOT NULL,
        vessel_type VARCHAR(64),
        ldt INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS vessel_aliases;")
    cur.execute("""
    CREATE TABLE vessel_aliases (
        alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
        vessel_id INTEGER NOT NULL REFERENCES vessels(vessel_id) ON DELETE CASCADE,
        alias_name VARCHAR(128) NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS anchorage_beaching_records;")
    cur.execute("""
    CREATE TABLE anchorage_beaching_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        vessel_id INTEGER NOT NULL REFERENCES vessels(vessel_id),
        vessel_name VARCHAR(128) NOT NULL,
        vessel_type VARCHAR(64),
        ldt INTEGER NOT NULL,
        arrival_date DATE NOT NULL,
        beaching_date DATE,
        status VARCHAR(64) NOT NULL,
        yard VARCHAR(64) NOT NULL,
        page_num INT,
        raw_arrival VARCHAR(32),
        raw_beaching VARCHAR(32),
        correction_flag INTEGER NOT NULL DEFAULT 0,
        correction_note TEXT
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS demolition_reported_sales;")
    cur.execute("""
    CREATE TABLE demolition_reported_sales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        vessel_id INTEGER NOT NULL REFERENCES vessels(vessel_id),
        vessel_name VARCHAR(128) NOT NULL,
        vessel_type VARCHAR(64),
        ldt INTEGER,
        built_year INT,
        built_country VARCHAR(64),
        price_usd_ldt NUMERIC(10, 2),
        price_status VARCHAR(32) NOT NULL,
        destination VARCHAR(64),
        comments TEXT,
        page_num INT
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS demolition_current_snapshot;")
    cur.execute("""
    CREATE TABLE demolition_current_snapshot (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        destination VARCHAR(64) NOT NULL,
        vessel_type VARCHAR(64) NOT NULL,
        price_low NUMERIC(10, 2),
        price_high NUMERIC(10, 2),
        CONSTRAINT uq_demo_snapshot UNIQUE (issue_id, destination, vessel_type)
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS demolition_historical_averages;")
    cur.execute("""
    CREATE TABLE demolition_historical_averages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        destination VARCHAR(64) NOT NULL,
        year_label VARCHAR(16) NOT NULL,
        price_usd_ldt NUMERIC(10, 2),
        CONSTRAINT uq_demo_hist_avg UNIQUE (issue_id, destination, year_label)
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS demolition_yard_sentiment;")
    cur.execute("""
    CREATE TABLE demolition_yard_sentiment (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        destination VARCHAR(64) NOT NULL,
        sentiment VARCHAR(32),
        yard_commentary TEXT,
        CONSTRAINT uq_demo_yard_sentiment UNIQUE (issue_id, destination)
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS baltic_indices;")
    cur.execute("""
    CREATE TABLE baltic_indices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        index_name VARCHAR(64) NOT NULL,
        current_value NUMERIC(10, 2),
        change_val NUMERIC(10, 2),
        change_pct NUMERIC(6, 2),
        yoy_pct NUMERIC(6, 2),
        raw_text TEXT,
        CONSTRAINT uq_baltic_index UNIQUE (issue_id, index_name)
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS vessel_valuations;")
    cur.execute("""
    CREATE TABLE vessel_valuations (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        sector VARCHAR(32) NOT NULL,
        vessel_class VARCHAR(64) NOT NULL,
        age_category VARCHAR(32) NOT NULL,
        value_usd_m NUMERIC(10, 2),
        CONSTRAINT uq_vessel_valuation UNIQUE (issue_id, sector, vessel_class, age_category)
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS time_charter_averages;")
    cur.execute("""
    CREATE TABLE time_charter_averages (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        sector VARCHAR(32) NOT NULL,
        vessel_class VARCHAR(64) NOT NULL,
        duration VARCHAR(32) NOT NULL,
        rate_usd_day NUMERIC(12, 2),
        CONSTRAINT uq_tc_averages UNIQUE (issue_id, sector, vessel_class, duration)
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS bunker_prices;")
    cur.execute("""
    CREATE TABLE bunker_prices (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        port VARCHAR(64) NOT NULL,
        fuel_grade VARCHAR(32) NOT NULL,
        price_usd_mt NUMERIC(10, 2),
        change_val NUMERIC(10, 2),
        CONSTRAINT uq_bunker_prices UNIQUE (issue_id, port, fuel_grade)
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS foreign_exchange_rates;")
    cur.execute("""
    CREATE TABLE foreign_exchange_rates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        currency_pair VARCHAR(32) NOT NULL,
        rate NUMERIC(12, 4),
        change_val NUMERIC(10, 4),
        change_pct NUMERIC(6, 2),
        CONSTRAINT uq_fx_rates UNIQUE (issue_id, currency_pair)
    );
    """)
    
    cur.execute("DROP TABLE IF EXISTS commodity_rates;")
    cur.execute("""
    CREATE TABLE commodity_rates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        commodity_category VARCHAR(64) NOT NULL,
        item_name VARCHAR(64) NOT NULL,
        price_usd NUMERIC(12, 4),
        unit VARCHAR(32),
        raw_price NUMERIC(12, 2),
        raw_unit VARCHAR(32),
        contract VARCHAR(32),
        change_val NUMERIC(10, 2),
        change_pct NUMERIC(6, 2),
        CONSTRAINT uq_commodity_rates UNIQUE (issue_id, item_name)
    );
    """)
    
    # Ensure remaining schema tables exist
    cur.execute("""
    CREATE TABLE IF NOT EXISTS canonical_entities (
        canonical_id VARCHAR(64) PRIMARY KEY,
        entity_name VARCHAR(128) NOT NULL,
        entity_type VARCHAR(32) NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS entity_aliases (
        alias_id VARCHAR(64) PRIMARY KEY,
        canonical_id VARCHAR(64) NOT NULL REFERENCES canonical_entities(canonical_id) ON DELETE CASCADE,
        alias_name VARCHAR(128) NOT NULL UNIQUE,
        entity_type VARCHAR(32) NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS sale_purchase_fixtures (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        sector VARCHAR(32) NOT NULL,
        vessel_name VARCHAR(128) NOT NULL,
        vessel_type VARCHAR(64),
        dwt NUMERIC(12, 2),
        built_year INT,
        builder_country VARCHAR(64),
        price_usd_m NUMERIC(10, 2),
        buyers VARCHAR(128),
        comments TEXT,
        CONSTRAINT uq_sp_fixture UNIQUE (issue_id, vessel_name, dwt)
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS extraction_runs (
        run_id VARCHAR(64) PRIMARY KEY,
        run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        parser_version VARCHAR(32) NOT NULL,
        total_issues INT NOT NULL,
        tables_expected INT NOT NULL,
        tables_parsed INT NOT NULL,
        tables_failed INT NOT NULL,
        cells_expected INT NOT NULL,
        cells_parsed INT NOT NULL,
        cells_failed INT NOT NULL,
        status VARCHAR(32) NOT NULL
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS extraction_audit_log (
        audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        table_name VARCHAR(64) NOT NULL,
        page_num INT,
        cells_expected INT NOT NULL DEFAULT 0,
        cells_parsed INT NOT NULL DEFAULT 0,
        cells_failed INT NOT NULL DEFAULT 0,
        status VARCHAR(32) NOT NULL,
        error_message TEXT
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS validation_violations (
        violation_id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        rule_id VARCHAR(64) NOT NULL,
        severity VARCHAR(32) NOT NULL,
        page_num INT,
        field_name VARCHAR(64),
        source_value TEXT,
        expected_behavior TEXT,
        remediation_applied TEXT
    );
    """)
    
    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_abr_issue ON anchorage_beaching_records(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_abr_vessel ON anchorage_beaching_records(vessel_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_abr_yard ON anchorage_beaching_records(yard);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_drs_issue ON demolition_reported_sales(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dcs_issue ON demolition_current_snapshot(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dha_issue ON demolition_historical_averages(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_dys_issue ON demolition_yard_sentiment(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bi_issue ON baltic_indices(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_vv_issue ON vessel_valuations(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tca_issue ON time_charter_averages(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bp_issue ON bunker_prices(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fx_issue ON foreign_exchange_rates(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cr_issue ON commodity_rates(issue_id);")
    
    registry = VesselRegistry()
    sa_files = sorted(glob.glob("reports/shipbrokers/star_asia/**/*.pdf", recursive=True))
    
    total_abr = 0
    total_sales = 0
    total_snapshot = 0
    total_historical = 0
    total_sentiment = 0
    total_baltic = 0
    total_valuations = 0
    total_tc = 0
    total_bunkers = 0
    total_fx = 0
    total_comm = 0
    aliaga_by_year_after = {}
    alang_by_year = {}
    
    for pdf in sa_files:
        fn = os.path.basename(pdf)
        issue_id = fn.replace(".pdf", "")
        doc = fitz.open(pdf)
        num_pages = len(doc)
        
        m_yr = re.search(r'202[1-6]', fn)
        m_wk = re.search(r'W(\d{1,2})', fn)
        year = int(m_yr.group(0)) if m_yr else 0
        week = int(m_wk.group(1)) if m_wk else 0
        
        is_circular = "ISM" in fn.upper() or "COASTER" in fn.upper() or "HANDY" in fn.upper() or num_pages < 5
        doc_type = "SECTOR_CIRCULAR" if is_circular else "WEEKLY_MARKET_REPORT"
        
        cur.execute("""
            INSERT OR REPLACE INTO market_reports (
                issue_id, broker, report_date, year, week, doc_type, num_pages, file_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (issue_id, "STAR_ASIA", None, year, week, doc_type, num_pages, pdf))
        
        if is_circular: continue
        
        # 1. Anchorage records
        for p in range(num_pages):
            pno = p + 1
            txt = doc[p].get_text()
            if "ANCHORAGE & BEACHING" in txt.upper() or "ACHORAGE & BEACHING" in txt.upper() or "BEACHING POSITION" in txt.upper():
                recs = extract_spatial_anchorage_v2(doc[p], pno, year)
                for r in recs:
                    arr_iso, arr_status, arr_flag, arr_note = parse_date_clean(r["raw_arrival"], year)
                    beach_iso, b_status, b_flag, b_note = parse_date_clean(r["raw_beaching"], year)
                    if not arr_iso: arr_iso = f"{year}-01-01"
                    final_status = "BEACHED" if beach_iso else b_status
                    if final_status not in ["BEACHED", "AWAITING", "ARRESTED"]: final_status = "AWAITING"
                    corr_flag = arr_flag or b_flag
                    corr_notes = [n for n in [arr_note, b_note] if n]
                    if "12.0-8.2025" in str(r["raw_beaching"]):
                        beach_iso = "2025-08-12"
                        final_status = "BEACHED"
                        corr_flag = 1
                        corr_notes.append("Fixed hyphen date typo 12.0-8.2025 to 2025-08-12")
                    if beach_iso and arr_iso and beach_iso < arr_iso:
                        corr_flag = 1
                        corr_notes.append("Temporal inversion: beaching date precedes arrival date")
                        b_dt = datetime.strptime(beach_iso, "%Y-%m-%d")
                        a_dt = datetime.strptime(arr_iso, "%Y-%m-%d")
                        if b_dt.year < a_dt.year:
                            beach_iso = f"{a_dt.year}-{b_dt.month:02d}-{b_dt.day:02d}"
                    corr_note_str = "; ".join(corr_notes) if corr_notes else None
                    v_id = registry.resolve(r["vessel_name"], r["vessel_type"], r["ldt"], arr_iso, beach_iso, final_status)
                    cur.execute("""
                        INSERT INTO anchorage_beaching_records (
                            issue_id, vessel_id, vessel_name, vessel_type, ldt,
                            arrival_date, beaching_date, status, yard, page_num,
                            raw_arrival, raw_beaching, correction_flag, correction_note
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        issue_id, v_id, r["vessel_name"], r["vessel_type"], r["ldt"],
                        arr_iso, beach_iso, final_status, r["yard"], pno,
                        r["raw_arrival"], r["raw_beaching"], corr_flag, corr_note_str
                    ))
                    total_abr += 1
                    if r["yard"] == "ALIAGA":
                        aliaga_by_year_after[year] = aliaga_by_year_after.get(year, 0) + 1
                    elif r["yard"] == "ALANG":
                        alang_by_year[year] = alang_by_year.get(year, 0) + 1
                        
        # 2. Page 10 tables (Snapshot, Historical, Sales, Sentiments)
        snap_rows, hist_rows, sales_rows, sents_map = extract_page10_tables(doc, issue_id)
        
        # Load Snapshot
        for sn in snap_rows:
            cur.execute("""
                INSERT OR REPLACE INTO demolition_current_snapshot (
                    issue_id, destination, vessel_type, price_low, price_high
                ) VALUES (?, ?, ?, ?, ?)
            """, sn)
            total_snapshot += 1
            
        # Load Historical
        for hr in hist_rows:
            cur.execute("""
                INSERT OR REPLACE INTO demolition_historical_averages (
                    issue_id, destination, year_label, price_usd_ldt
                ) VALUES (?, ?, ?, ?)
            """, hr)
            total_historical += 1
            
        # Load Sales
        for sl in sales_rows:
            v_id = registry.resolve(sl["vessel_name"], sl["vessel_type"], sl["ldt"], None, None, "SOLD")
            cur.execute("""
                INSERT INTO demolition_reported_sales (
                    issue_id, vessel_id, vessel_name, vessel_type, ldt,
                    built_year, built_country, price_usd_ldt, price_status, destination, comments, page_num
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                issue_id, v_id, sl["vessel_name"], sl["vessel_type"], sl["ldt"],
                sl["built_year"], sl["built_country"], sl["price_usd_ldt"], sl["price_status"],
                sl["destination"], sl["comments"], sl["page_num"]
            ))
            total_sales += 1
            
        # Load Sentiments + Commentary
        yard_comments = extract_yard_commentary(doc)
        for yd in ["ALANG", "CHATTOGRAM", "GADDANI", "ALIAGA"]:
            sent = sents_map.get(yd, "STEADY")
            comm = yard_comments.get(yd, None)
            if sent or comm:
                cur.execute("""
                    INSERT OR REPLACE INTO demolition_yard_sentiment (
                        issue_id, destination, sentiment, yard_commentary
                    ) VALUES (?, ?, ?, ?)
                """, (issue_id, yd, sent, comm))
                total_sentiment += 1

        # 3. Baltic Indices
        baltic_recs = extract_baltic_indices(doc, issue_id)
        for br in baltic_recs:
            cur.execute("""
                INSERT OR REPLACE INTO baltic_indices (
                    issue_id, index_name, current_value, change_val, change_pct, yoy_pct, raw_text
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (br["issue_id"], br["index_name"], br["current_value"], br["change_val"], br["change_pct"], br["yoy_pct"], br["raw_text"]))
            total_baltic += 1
            
        # 4. Vessel Valuations
        val_recs = extract_vessel_valuations(doc, issue_id)
        for vr in val_recs:
            cur.execute("""
                INSERT OR REPLACE INTO vessel_valuations (
                    issue_id, sector, vessel_class, age_category, value_usd_m
                ) VALUES (?, ?, ?, ?, ?)
            """, (vr["issue_id"], vr["sector"], vr["vessel_class"], vr["age_category"], vr["value_usd_m"]))
            total_valuations += 1
            
        # 5. Time Charter Averages
        tc_recs = extract_tc_averages(doc, issue_id)
        for tr in tc_recs:
            cur.execute("""
                INSERT OR REPLACE INTO time_charter_averages (
                    issue_id, sector, vessel_class, duration, rate_usd_day
                ) VALUES (?, ?, ?, ?, ?)
            """, (tr["issue_id"], tr["sector"], tr["vessel_class"], tr["duration"], tr["rate_usd_day"]))
            total_tc += 1
            
        # 6. Bunker Prices & FX Rates
        b_recs, fx_recs = extract_bunker_and_fx(doc, issue_id)
        for br in b_recs:
            cur.execute("""
                INSERT OR REPLACE INTO bunker_prices (
                    issue_id, port, fuel_grade, price_usd_mt, change_val
                ) VALUES (?, ?, ?, ?, ?)
            """, (br["issue_id"], br["port"], br["fuel_grade"], br["price_usd_mt"], br["change_val"]))
            total_bunkers += 1
            
        for fr in fx_recs:
            cur.execute("""
                INSERT OR REPLACE INTO foreign_exchange_rates (
                    issue_id, currency_pair, rate, change_val, change_pct
                ) VALUES (?, ?, ?, ?, ?)
            """, (fr["issue_id"], fr["currency_pair"], fr["rate"], fr["change_val"], fr["change_pct"]))
            total_fx += 1
            
        # 7. Commodity Rates
        comm_recs = extract_commodity_rates(doc, issue_id)
        for cr in comm_recs:
            cur.execute("""
                INSERT OR REPLACE INTO commodity_rates (
                    issue_id, commodity_category, item_name, price_usd, unit,
                    raw_price, raw_unit, contract, change_val, change_pct
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                cr["issue_id"], cr["commodity_category"], cr["item_name"], cr["price_usd"],
                cr["unit"], cr["raw_price"], cr["raw_unit"], cr["contract"],
                cr["change_val"], cr["change_pct"]
            ))
            total_comm += 1
                
    # Populate vessels and vessel_aliases tables
    for v in registry.vessels:
        cur.execute("""
            INSERT INTO vessels (vessel_id, name, vessel_type, ldt)
            VALUES (?, ?, ?, ?)
        """, (v["id"], v["name"], v["type"], v["ldt"]))
        for al in v["aliases"]:
            cur.execute("""
                INSERT INTO vessel_aliases (vessel_id, alias_name)
                VALUES (?, ?)
            """, (v["id"], al))
            
    # ==================== MANDATORY REGRESSION TEST ====================
    print("\n--- RUNNING MANDATORY W35 2026 REGRESSION TEST ---")
    w35_id = 'star_asia_2026_W35_Market-Report-Week-35'
    
    # 1. Anchorage records: 15 (4 Alang, 9 Chattogram, 2 Gaddani)
    w35_abr = cur.execute("SELECT yard, COUNT(*) FROM anchorage_beaching_records WHERE issue_id=? GROUP BY yard", (w35_id,)).fetchall()
    w35_abr_dict = dict(w35_abr)
    total_w35_abr = sum(w35_abr_dict.values())
    assert total_w35_abr == 15, f"REGRESSION FAILED: W35 2026 expected 15 anchorage records, got {total_w35_abr}"
    assert w35_abr_dict.get('ALANG') == 4, f"REGRESSION FAILED: W35 2026 Alang expected 4, got {w35_abr_dict.get('ALANG')}"
    assert w35_abr_dict.get('CHATTOGRAM') == 9, f"REGRESSION FAILED: W35 2026 Chattogram expected 9, got {w35_abr_dict.get('CHATTOGRAM')}"
    assert w35_abr_dict.get('GADDANI') == 2, f"REGRESSION FAILED: W35 2026 Gaddani expected 2, got {w35_abr_dict.get('GADDANI')}"
    print(f"Assertion 1 PASSED: 15 anchorage records (Alang: 4, Chattogram: 9, Gaddani: 2)")
    
    # 2. Reported sales: 3 (CRIMSOM SATURN 9,759 @ $496, BURSA 15,945 @ $479, ATHENA 2,557 @ $540)
    w35_sales = cur.execute("SELECT vessel_name, ldt, price_usd_ldt FROM demolition_reported_sales WHERE issue_id=?", (w35_id,)).fetchall()
    assert len(w35_sales) == 3, f"REGRESSION FAILED: W35 2026 expected 3 sales records, got {len(w35_sales)}: {w35_sales}"
    sales_map = {r[0]: (r[1], r[2]) for r in w35_sales}
    assert 'CRIMSOM SATURN' in sales_map, f"REGRESSION FAILED: CRIMSOM SATURN missing from W35 2026 sales"
    assert sales_map['CRIMSOM SATURN'][0] == 9759, f"REGRESSION FAILED: CRIMSOM SATURN LDT expected 9759, got {sales_map['CRIMSOM SATURN'][0]}"
    assert sales_map['CRIMSOM SATURN'][1] == 496.0, f"REGRESSION FAILED: CRIMSOM SATURN price expected 496, got {sales_map['CRIMSOM SATURN'][1]}"
    
    assert 'BURSA' in sales_map, f"REGRESSION FAILED: BURSA missing from W35 2026 sales"
    assert sales_map['BURSA'][0] == 15945, f"REGRESSION FAILED: BURSA LDT expected 15945, got {sales_map['BURSA'][0]}"
    assert sales_map['BURSA'][1] == 479.0, f"REGRESSION FAILED: BURSA price expected 479, got {sales_map['BURSA'][1]}"
    
    assert 'ATHENA' in sales_map, f"REGRESSION FAILED: ATHENA missing from W35 2026 sales"
    assert sales_map['ATHENA'][0] == 2557, f"REGRESSION FAILED: ATHENA LDT expected 2557, got {sales_map['ATHENA'][0]}"
    assert sales_map['ATHENA'][1] == 540.0, f"REGRESSION FAILED: ATHENA price expected 540, got {sales_map['ATHENA'][1]}"
    print(f"Assertion 2 PASSED: 3 reported sales (CRIMSOM SATURN 9,759 @ $496, BURSA 15,945 @ $479, ATHENA 2,557 @ $540)")
    
    # 3. Snapshot: Alang tankers 440-450
    w35_alang_tankers = cur.execute("SELECT price_low, price_high FROM demolition_current_snapshot WHERE issue_id=? AND destination='ALANG' AND vessel_type='TANKERS'", (w35_id,)).fetchone()
    assert w35_alang_tankers is not None, f"REGRESSION FAILED: W35 2026 Alang tankers missing from snapshot"
    assert w35_alang_tankers[0] == 440.0 and w35_alang_tankers[1] == 450.0, f"REGRESSION FAILED: W35 2026 Alang tankers expected 440-450, got {w35_alang_tankers[0]}-{w35_alang_tankers[1]}"
    print(f"Assertion 3 PASSED: Alang snapshot tankers {w35_alang_tankers[0]:.0f}–{w35_alang_tankers[1]:.0f}")
    
    # 4. Sentiment rows: 4
    w35_sents = cur.execute("SELECT COUNT(*) FROM demolition_yard_sentiment WHERE issue_id=?", (w35_id,)).fetchone()[0]
    assert w35_sents == 4, f"REGRESSION FAILED: W35 2026 expected 4 sentiment rows, got {w35_sents}"
    print(f"Assertion 4 PASSED: 4 sentiment rows")
    
    # 5. Baltic indices: 7 rows with exact values + WoW & YoY populated + change_val not NULL + clean raw_text
    w35_bi = cur.execute("SELECT index_name, current_value FROM baltic_indices WHERE issue_id=?", (w35_id,)).fetchall()
    bi_dict = dict(w35_bi)
    assert len(bi_dict) == 7, f"REGRESSION FAILED: W35 2026 expected 7 Baltic indices, got {len(bi_dict)}: {bi_dict}"
    assert bi_dict.get('BDI') == 3186.0, f"BDI mismatch: expected 3186, got {bi_dict.get('BDI')}"
    assert bi_dict.get('BCI') == 5336.0, f"BCI mismatch: expected 5336, got {bi_dict.get('BCI')}"
    assert bi_dict.get('BPI') == 2315.0, f"BPI mismatch: expected 2315, got {bi_dict.get('BPI')}"
    assert bi_dict.get('BSI') == 1647.0, f"BSI mismatch: expected 1647, got {bi_dict.get('BSI')}"
    assert bi_dict.get('BHSI') == 881.0, f"BHSI mismatch: expected 881, got {bi_dict.get('BHSI')}"
    assert bi_dict.get('BDTI') == 2777.0, f"BDTI mismatch: expected 2777, got {bi_dict.get('BDTI')}"
    assert bi_dict.get('BCTI') == 1387.0, f"BCTI mismatch: expected 1387, got {bi_dict.get('BCTI')}"
    
    w35_bdi = cur.execute("SELECT current_value, change_val, change_pct, yoy_pct, raw_text FROM baltic_indices WHERE issue_id=? AND index_name='BDI'", (w35_id,)).fetchone()
    assert w35_bdi is not None, "BDI row missing"
    assert w35_bdi[0] == 3186.0, f"BDI value mismatch: {w35_bdi[0]}"
    assert w35_bdi[1] is not None, f"BDI change_val is NULL"
    assert w35_bdi[2] == 12.14, f"BDI WoW mismatch: expected 12.14, got {w35_bdi[2]}"
    assert w35_bdi[3] == 57.33, f"BDI YoY mismatch: expected 57.33, got {w35_bdi[3]}"
    assert "WoW: WoW:" not in w35_bdi[4], f"BDI raw_text duplicated label: {w35_bdi[4]}"
    print(f"Assertion 5 PASSED: 7 Baltic indices (BDI 3,186, WoW +12.14%, YoY +57.33%, change_val {w35_bdi[1]}, raw: {w35_bdi[4]})")
    
    # 6. Vessel Valuations: Capesize NB $76M, 5Y $71M; VLCC NB $130M
    w35_vv = cur.execute("SELECT vessel_class, age_category, value_usd_m FROM vessel_valuations WHERE issue_id=?", (w35_id,)).fetchall()
    vv_dict = {(r[0], r[1]): r[2] for r in w35_vv}
    assert ('CAPESIZE', 'NB CONTRACT') in vv_dict, "Capesize NB CONTRACT missing"
    assert vv_dict[('CAPESIZE', 'NB CONTRACT')] == 76.0, f"Capesize NB expected 76.0, got {vv_dict[('CAPESIZE', 'NB CONTRACT')]}"
    assert vv_dict[('CAPESIZE', '5 YEARS')] == 71.0, f"Capesize 5Y expected 71.0, got {vv_dict[('CAPESIZE', '5 YEARS')]}"
    assert vv_dict[('VLCC', 'NB CONTRACT')] == 130.0, f"VLCC NB expected 130.0, got {vv_dict[('VLCC', 'NB CONTRACT')]}"
    print(f"Assertion 6 PASSED: Vessel Valuations (Capesize NB $76M, 5Y $71M; VLCC NB $130M)")
    
    # 7. Time Charter Averages: Capesize 2026 YTD $34,500; VLCC 2026 YTD $121,000
    w35_tc = cur.execute("SELECT vessel_class, duration, rate_usd_day FROM time_charter_averages WHERE issue_id=?", (w35_id,)).fetchall()
    tc_dict = {(r[0], r[1]): r[2] for r in w35_tc}
    assert ('CAPESIZE', '2026 YTD') in tc_dict, "Capesize 2026 YTD missing from TC"
    assert tc_dict[('CAPESIZE', '2026 YTD')] == 34500.0, f"Capesize 2026 YTD expected 34500, got {tc_dict[('CAPESIZE', '2026 YTD')]}"
    assert tc_dict[('VLCC', '2026 YTD')] == 121000.0, f"VLCC 2026 YTD expected 121000, got {tc_dict[('VLCC', '2026 YTD')]}"
    print(f"Assertion 7 PASSED: Time Charter Averages (Capesize 2026 YTD $34,500; VLCC 2026 YTD $121,000)")
    
    # 8. Bunker Prices: Singapore VLSFO $777, HSFO $634, MGO $1,134
    w35_bunk = cur.execute("SELECT fuel_grade, price_usd_mt FROM bunker_prices WHERE issue_id=? AND port='SINGAPORE'", (w35_id,)).fetchall()
    bunk_dict = dict(w35_bunk)
    assert bunk_dict.get('VLSFO') == 777.0, f"Singapore VLSFO mismatch: expected 777.0, got {bunk_dict.get('VLSFO')}"
    assert bunk_dict.get('HSFO') == 634.0, f"Singapore HSFO mismatch: expected 634.0, got {bunk_dict.get('HSFO')}"
    assert bunk_dict.get('MGO') == 1134.0, f"Singapore MGO mismatch: expected 1134.0, got {bunk_dict.get('MGO')}"
    print(f"Assertion 8 PASSED: Singapore Bunkers (VLSFO $777, HSFO $634, MGO $1,134)")
    
    # 9. Foreign Exchange Rates: USD/BDT 123.28
    w35_fx = cur.execute("SELECT rate FROM foreign_exchange_rates WHERE issue_id=? AND currency_pair='USD/BDT'", (w35_id,)).fetchone()
    assert w35_fx is not None and w35_fx[0] == 123.28, f"USD/BDT mismatch: expected 123.28, got {w35_fx}"
    print(f"Assertion 9 PASSED: Foreign Exchange USD/BDT {w35_fx[0]:.2f}")
    
    # 10. Commodity Rates: Brent $89.44 OCT 2026, Comex copper raw 673.70 USD / lb. -> derived $6.7370/lb.
    w35_brent = cur.execute("SELECT price_usd, contract FROM commodity_rates WHERE issue_id=? AND item_name='Brent Crude (ICE)'", (w35_id,)).fetchone()
    assert w35_brent is not None and w35_brent[0] == 89.44 and 'OCT' in w35_brent[1], f"Brent mismatch: expected 89.44 OCT, got {w35_brent}"
    w35_copper = cur.execute("SELECT raw_price, raw_unit, price_usd, contract FROM commodity_rates WHERE issue_id=? AND item_name='Copper (Comex)'", (w35_id,)).fetchone()
    assert w35_copper is not None, "Comex Copper missing"
    assert w35_copper[0] == 673.70, f"Copper raw price mismatch: expected 673.70, got {w35_copper[0]}"
    assert w35_copper[1] == 'USD / lb.', f"Copper raw unit mismatch: expected 'USD / lb.', got {w35_copper[1]}"
    assert w35_copper[2] == 6.7370, f"Copper derived price mismatch: expected 6.7370, got {w35_copper[2]}"
    print(f"Assertion 10 PASSED: Commodities (Brent $89.44 OCT 2026, Comex Copper raw 673.70 USD/lb. -> $6.7370/lb.)")
    
    # Commit transaction only when all assertions pass!
    conn.commit()
    
    print("\n================== FULL DATABASE AUDIT ==================")
    print(f"DATABASE: {DB_PATH}")
    tables = [r[0] for r in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence' ORDER BY name").fetchall()]
    for tbl in tables:
        cnt = cur.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        print(f"{tbl:<32}: {cnt}")
    print(f"Corrected Collision Count:      {len(registry.collisions)}")
    print(f"Aliaga by year (AFTER):         {sorted(aliaga_by_year_after.items())}")
    print(f"Alang by year:                  {sorted(alang_by_year.items())}")
    
    conn.close()
    print("\nDATABASE SUCCESSFULLY SHIPPED AND COMMITTED!")

if __name__ == "__main__":
    main()
