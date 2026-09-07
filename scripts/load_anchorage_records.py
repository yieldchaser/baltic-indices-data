import os
import glob
import re
import sqlite3
import pymupdf as fitz
from datetime import datetime

DB_PATH = "data/derived/star_asia_intelligence.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    # 1. Market reports table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS market_reports (
        issue_id VARCHAR(128) PRIMARY KEY,
        broker VARCHAR(64) NOT NULL DEFAULT 'STAR_ASIA',
        report_date DATE,
        year INT NOT NULL,
        week INT NOT NULL,
        doc_type VARCHAR(32) NOT NULL,
        num_pages INT NOT NULL,
        file_path TEXT NOT NULL,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # 2. Vessels table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vessels (
        vessel_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(128) NOT NULL UNIQUE,
        vessel_type VARCHAR(64),
        ldt NUMERIC(12, 2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # 3. Vessel aliases table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS vessel_aliases (
        alias_id INTEGER PRIMARY KEY AUTOINCREMENT,
        vessel_id INTEGER NOT NULL REFERENCES vessels(vessel_id) ON DELETE CASCADE,
        alias_name VARCHAR(128) NOT NULL UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    
    # 4. Anchorage & beaching records table
    cur.execute("DROP TABLE IF EXISTS anchorage_beaching_records;")
    cur.execute("""
    CREATE TABLE anchorage_beaching_records (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        issue_id VARCHAR(128) NOT NULL REFERENCES market_reports(issue_id),
        vessel_id INTEGER REFERENCES vessels(vessel_id),
        vessel VARCHAR(128) NOT NULL,
        vessel_name VARCHAR(128) NOT NULL,
        type VARCHAR(64),
        vessel_type VARCHAR(64),
        ldt NUMERIC(12, 2),
        arrival VARCHAR(32),
        arrival_date VARCHAR(32),
        beaching VARCHAR(32),
        beaching_date VARCHAR(32),
        yard VARCHAR(64) NOT NULL,
        location VARCHAR(64) NOT NULL,
        status VARCHAR(64),
        page_num INT
    );
    """)
    
    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_abr_issue ON anchorage_beaching_records(issue_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_abr_vessel ON anchorage_beaching_records(vessel_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_abr_yard ON anchorage_beaching_records(yard);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_va_name ON vessel_aliases(alias_name);")
    
    conn.commit()
    conn.close()

def get_or_create_vessel(cur, raw_name, v_type, ldt):
    clean = re.sub(r'\s+', ' ', raw_name.strip().upper())
    clean = re.sub(r'[\(\[\{].*?[\)\]\}]', '', clean).strip()
    if not clean:
        clean = raw_name.strip().upper()
        
    cur.execute("SELECT vessel_id FROM vessel_aliases WHERE alias_name = ?", (clean,))
    row = cur.fetchone()
    if row:
        v_id = row[0]
        if ldt and ldt > 0:
            cur.execute("UPDATE vessels SET ldt = ? WHERE vessel_id = ? AND (ldt IS NULL OR ldt = 0)", (ldt, v_id))
        if v_type and v_type != "UNKNOWN":
            cur.execute("UPDATE vessels SET vessel_type = ? WHERE vessel_id = ? AND (vessel_type IS NULL OR vessel_type = 'UNKNOWN')", (v_type, v_id))
        return v_id
        
    cur.execute("SELECT vessel_id FROM vessels WHERE name = ?", (clean,))
    row = cur.fetchone()
    if row:
        v_id = row[0]
    else:
        cur.execute("INSERT INTO vessels (name, vessel_type, ldt) VALUES (?, ?, ?)", (clean, v_type, ldt))
        v_id = cur.lastrowid
        
    cur.execute("INSERT OR IGNORE INTO vessel_aliases (vessel_id, alias_name) VALUES (?, ?)", (v_id, clean))
    if raw_name.strip().upper() != clean:
        cur.execute("INSERT OR IGNORE INTO vessel_aliases (vessel_id, alias_name) VALUES (?, ?)", (v_id, raw_name.strip().upper()))
        
    return v_id

def extract_anchorage_from_page(page_text, pno):
    if not ("ANCHORAGE & BEACHING" in page_text.upper() or "ACHORAGE & BEACHING" in page_text.upper()):
        return []
        
    records = []
    lines = [l.strip() for l in page_text.splitlines() if l.strip()]
    
    current_yard = None
    i = 0
    n = len(lines)
    
    ldt_pattern = re.compile(r'^\d{1,3}(?:,\d{3})+(?:\.\d+)?$|^\d{3,6}$')
    date_pattern = re.compile(r'^\d{1,2}[\.,]\d{2}[\.,]\d{2,4}$')
    status_pattern = re.compile(r'^(AWAITING|AWAIITNG|BEACHED|AT ANCHORAGE|AWAITED)$', re.I)
    
    while i < n:
        line = lines[i]
        
        # Yard header
        if "ANCHORAGE & BEACHING" in line.upper() or "ACHORAGE & BEACHING" in line.upper():
            line_upper = line.upper()
            if "ALANG" in line_upper: current_yard = "ALANG"
            elif "CHATTOGRAM" in line_upper: current_yard = "CHATTOGRAM"
            elif "GADDANI" in line_upper or "GADANI" in line_upper: current_yard = "GADDANI"
            elif "ALIAGA" in line_upper or "TURKEY" in line_upper: current_yard = "ALIAGA"
            else:
                for j in range(max(0, i-4), min(n, i+5)):
                    lj = lines[j].upper()
                    if "ALANG" in lj: current_yard = "ALANG"; break
                    elif "CHATTOGRAM" in lj: current_yard = "CHATTOGRAM"; break
                    elif "GADDANI" in lj or "GADANI" in lj: current_yard = "GADDANI"; break
                    elif "ALIAGA" in lj or "TURKEY" in lj: current_yard = "ALIAGA"; break
            i += 1
            continue
            
        if line.upper() in ["ALANG, INDIA", "ALANG", "CHATTOGRAM, BANGLADESH", "CHATTOGRAM", "GADDANI, PAKISTAN", "GADANI, PAKISTAN", "GADDANI", "GADANI", "ALIAGA, TURKEY", "ALIAGA"]:
            if "ALANG" in line.upper(): current_yard = "ALANG"
            elif "CHATTOGRAM" in line.upper(): current_yard = "CHATTOGRAM"
            elif "GADDANI" in line.upper() or "GADANI" in line.upper(): current_yard = "GADDANI"
            elif "ALIAGA" in line.upper(): current_yard = "ALIAGA"
            i += 1
            continue

        if line.upper() in ["VESSEL", "VESSEL NAME", "TYPE", "LDT", "ARRIVAL", "BEACHING", "STATUS"]:
            i += 1
            continue
            
        if any(stop in line.upper() for stop in ["COMMODITIES, BUNKERS", "SHIP RECYCLING", "EXCHANGE RATES", "BUNKER PRICES"]):
            i += 1
            continue

        # Look for LDT
        if ldt_pattern.match(line) and current_yard:
            ldt_val = float(line.replace(",", ""))
            
            vessel_name = ""
            vessel_type = ""
            if i >= 2:
                c_name = lines[i-2]
                c_type = lines[i-1]
                if not any(h in c_name.upper() for h in ["VESSEL", "BEACHING", "ARRIVAL", "LDT", "POSITION", "REPORT", "STAR ASIA"]):
                    vessel_name = c_name
                    vessel_type = c_type
                elif not any(h in c_type.upper() for h in ["VESSEL", "BEACHING", "ARRIVAL", "LDT", "POSITION", "REPORT", "STAR ASIA"]):
                    vessel_name = c_type
                    vessel_type = "UNKNOWN"
            elif i >= 1:
                vessel_name = lines[i-1]
                vessel_type = "UNKNOWN"
                
            arr_date = ""
            beach_date = ""
            
            if i + 1 < n and date_pattern.match(lines[i+1]):
                arr_date = lines[i+1]
                if i + 2 < n:
                    next2 = lines[i+2]
                    if date_pattern.match(next2) or status_pattern.match(next2):
                        beach_date = next2
                        i += 2
                    else:
                        beach_date = "AWAITING"
                        i += 1
                else:
                    beach_date = "AWAITING"
                    i += 1
                    
                if vessel_name and arr_date:
                    status = "BEACHED" if date_pattern.match(beach_date) else beach_date.upper()
                    records.append({
                        "vessel_name": vessel_name,
                        "vessel_type": vessel_type,
                        "ldt": ldt_val,
                        "arrival": arr_date,
                        "beaching": beach_date,
                        "status": status,
                        "yard": current_yard,
                        "page": pno
                    })
        i += 1
        
    return records

def main():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    sa_files = sorted(glob.glob("reports/shipbrokers/star_asia/**/*.pdf", recursive=True))
    
    total_loaded = 0
    w35_loaded = 0
    
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
        
        if is_circular:
            continue
            
        for p in range(num_pages):
            pno = p + 1
            recs = extract_anchorage_from_page(doc[p].get_text(), pno)
            for r in recs:
                v_id = get_or_create_vessel(cur, r["vessel_name"], r["vessel_type"], r["ldt"])
                
                cur.execute("""
                    INSERT INTO anchorage_beaching_records (
                        issue_id, vessel_id, vessel, vessel_name, type, vessel_type,
                        ldt, arrival, arrival_date, beaching, beaching_date,
                        yard, location, status, page_num
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    issue_id, v_id, r["vessel_name"], r["vessel_name"],
                    r["vessel_type"], r["vessel_type"], r["ldt"],
                    r["arrival"], r["arrival"], r["beaching"], r["beaching"],
                    r["yard"], r["yard"], r["status"], pno
                ))
                total_loaded += 1
                if issue_id == "star_asia_2026_W35_Market-Report-Week-35":
                    w35_loaded += 1
                    
    conn.commit()
    
    # Verification checks
    cur.execute("SELECT COUNT(*) FROM anchorage_beaching_records;")
    total_abr = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM vessels;")
    total_v = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM vessel_aliases;")
    total_va = cur.fetchone()[0]
    
    cur.execute("SELECT vessel, type, ldt, arrival, beaching, yard, page_num FROM anchorage_beaching_records WHERE issue_id='star_asia_2026_W35_Market-Report-Week-35';")
    w35_rows = cur.fetchall()
    
    conn.close()
    
    print(f"DATABASE COMMITTED: {DB_PATH}")
    print(f"TOTAL anchorage_beaching_records: {total_abr}")
    print(f"TOTAL vessels: {total_v}")
    print(f"TOTAL vessel_aliases: {total_va}")
    print(f"TOTAL W35 records: {len(w35_rows)}")
    print("\nW35 Records:")
    for r in w35_rows:
        print(f"  {r[0]:<20} | {r[1]:<15} | LDT: {r[2]:>8,.0f} | Arr: {r[3]:<12} | Beach: {r[4]:<12} | Yard: {r[5]:<10} | Page: {r[6]}")

if __name__ == "__main__":
    main()
