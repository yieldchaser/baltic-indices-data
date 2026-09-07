import os
import glob
import json
import pandas as pd

with open('index.html', 'r', encoding='utf-8', errors='ignore') as f:
    index_html = f.read()

# 1. Audit all CSV / Parquet datasets
data_files = (
    glob.glob('data/**/*.csv', recursive=True) +
    glob.glob('data/**/*.parquet', recursive=True) +
    glob.glob('data/**/*.json', recursive=True) +
    glob.glob('*.csv') +
    glob.glob('bunker_pipeline/**/*.csv', recursive=True)
)

print(f"Total data files scanned: {len(data_files)}")

audit_records = []
for p in sorted(data_files):
    rel = p.replace('\\', '/')
    base = os.path.basename(rel)
    
    # Check if mentioned in index.html
    in_index = (rel in index_html) or (base in index_html)
    
    size = os.path.getsize(p)
    lines = None
    cols = None
    date_start = None
    date_end = None
    
    if rel.endswith('.csv'):
        try:
            df = pd.read_csv(p, nrows=500000)
            lines = len(df)
            cols = list(df.columns)
            # Find date col
            for c in df.columns:
                if 'date' in c.lower() or 'time' in c.lower() or 'timestamp' in c.lower():
                    try:
                        s = pd.to_datetime(df[c].dropna(), errors='coerce').dropna()
                        if len(s):
                            date_start = str(s.min())[:10]
                            date_end = str(s.max())[:10]
                            break
                    except Exception:
                        pass
        except Exception as e:
            lines = -1

    audit_records.append({
        'path': rel,
        'size_bytes': size,
        'rows': lines,
        'columns': cols[:8] if cols else [],
        'date_start': date_start,
        'date_end': date_end,
        'in_index': in_index
    })

# 2. Audit Reports Directories
report_dirs = [
    'reports/fearnleys',
    'reports/seabrokers',
    'reports/drewry',
    'reports/poten',
    'reports/hellenic',
    'reports/broker_reports',
    'reports/panama_canal',
    'reports/breakwave',
    'reports/tankers',
    'reports/drybulk',
    'reports/baltic',
    'data/reports/fearnleys',
    'data/reports/seabrokers'
]

reports_summary = {}
for rd in report_dirs:
    if os.path.exists(rd):
        files = [f for f in os.listdir(rd) if os.path.isfile(os.path.join(rd, f))]
        reports_summary[rd] = {
            'count': len(files),
            'total_mb': sum(os.path.getsize(os.path.join(rd, f)) for f in files) / (1024*1024)
        }

# 3. PDF Books
pdf_books = [f for f in os.listdir('reports') if f.endswith('.pdf')]

output = {
    'datasets': audit_records,
    'reports_summary': reports_summary,
    'pdf_books': pdf_books
}

with open('scratch_full_data_audit.json', 'w', encoding='utf-8') as f:
    json.dump(output, f, indent=2)

print(f"Audit complete. Processed {len(audit_records)} files.")
