"""
Maritime Knowledge Spine Builder (Extended with Knowledge Tree & P0 Asset Linking).
Unifies tabular datasets into a high-speed relational SQLite graph spine
with deterministic foreign keys and queryable indexes linking vessels, ports,
commercial fixtures, bunker fuel costs, forward curves, SEC equities,
and existing knowledge tree nodes (preserving knowledge/trees/ hierarchy).
"""

import os
import sys
import json
import sqlite3
import pandas as pd
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
SOURCE_ROOT = Path(os.environ.get("SHIPPING_SOURCE_ROOT", str(REPO_ROOT)))
OUTPUT_DIR = REPO_ROOT / "data" / "derived"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = OUTPUT_DIR / "maritime_knowledge_spine.db"

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    # 1. Knowledge Tree Dimension (preserving knowledge/trees/ hierarchy)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_tree_nodes (
        node_id TEXT PRIMARY KEY,
        doc_id TEXT,
        parent_id TEXT,
        title TEXT,
        source_path TEXT,
        token_count INTEGER,
        keywords_json TEXT
    );
    """)

    # 2. P0 Skipped Assets Queue Table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_skipped_assets (
        asset_id TEXT PRIMARY KEY,
        parent_doc_id TEXT,
        parent_date TEXT,
        source TEXT,
        category TEXT,
        asset_url TEXT,
        asset_type TEXT,
        local_path TEXT,
        is_resolved_local BOOLEAN,
        status TEXT,
        FOREIGN KEY(parent_doc_id) REFERENCES dim_tree_nodes(doc_id)
    );
    """)

    # 3. Equities Dimension
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dim_companies (
        company_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_ticker TEXT,
        target_symbol TEXT,
        sector TEXT,
        company_name TEXT,
        is_sec BOOLEAN
    );
    """)

    # 4. Port Stress Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_port_stress (
        stress_id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT,
        week INTEGER,
        year INTEGER,
        port_locode TEXT,
        portid TEXT,
        portname TEXT,
        country TEXT,
        asset_class TEXT,
        live_weekly_calls REAL,
        hist_min REAL,
        hist_max REAL,
        hist_mean REAL,
        hist_std REAL,
        arrival_deviation_zscore REAL,
        stress_flag TEXT,
        signal_interpretation TEXT
    );
    """)

    # 5. Commercial Fixtures Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_fixtures (
        fixture_id INTEGER PRIMARY KEY,
        fixture_date TEXT,
        charterer TEXT,
        owner TEXT,
        vessel_name TEXT,
        imo_number TEXT,
        rate TEXT,
        period TEXT,
        route TEXT,
        segment TEXT,
        department TEXT,
        commodity TEXT,
        load_port TEXT,
        discharge_port TEXT,
        laycan TEXT,
        comment TEXT
    );
    """)

    # 6. Global Bunker Prices Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_bunker_prices (
        price_id INTEGER PRIMARY KEY AUTOINCREMENT,
        observation_date TEXT,
        port_code TEXT,
        port_name TEXT,
        grade TEXT,
        price_usd REAL,
        change_usd REAL,
        spread_usd REAL
    );
    """)

    # 7. SGX Curves Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_sgx_curves (
        curve_id INTEGER PRIMARY KEY AUTOINCREMENT,
        contract TEXT,
        expiry_month TEXT,
        expiry_year INTEGER,
        quote_date TEXT,
        price REAL,
        volume REAL,
        open_interest REAL,
        commodity_family TEXT
    );
    """)

    conn.commit()

def load_tree_nodes(conn: sqlite3.Connection):
    trees_dir = SOURCE_ROOT / "knowledge" / "trees"
    if not trees_dir.exists():
        return
    print(f"Loading knowledge tree nodes from {trees_dir}...")
    tree_files = list(trees_dir.rglob("*.json"))
    records = []
    for tf in tree_files:
        try:
            data = json.loads(tf.read_text(encoding="utf-8"))
            records.append((
                data.get("node_id"),
                data.get("doc_id"),
                data.get("parent_id"),
                data.get("title"),
                data.get("source_path"),
                data.get("token_count"),
                json.dumps(data.get("keywords", []))
            ))
        except Exception:
            continue

    cur = conn.cursor()
    cur.executemany("""
    INSERT OR REPLACE INTO dim_tree_nodes (node_id, doc_id, parent_id, title, source_path, token_count, keywords_json)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    print(f"Loaded {len(records)} tree root nodes into dim_tree_nodes")

def load_p0_assets(conn: sqlite3.Connection):
    p = REPO_ROOT / "data" / "derived" / "p0_skipped_assets_queue.jsonl"
    if not p.exists():
        return
    print(f"Loading P0 skipped assets queue from {p}...")
    records = []
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            d = json.loads(line)
            records.append((
                d.get("asset_id"),
                d.get("parent_doc_id"),
                d.get("parent_date"),
                d.get("source"),
                d.get("category"),
                d.get("asset_url"),
                d.get("asset_type"),
                d.get("local_path"),
                d.get("is_resolved_local"),
                d.get("status")
            ))

    cur = conn.cursor()
    cur.executemany("""
    INSERT OR REPLACE INTO fact_skipped_assets 
    (asset_id, parent_doc_id, parent_date, source, category, asset_url, asset_type, local_path, is_resolved_local, status)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    print(f"Loaded {len(records)} assets into fact_skipped_assets")

def load_equities(conn: sqlite3.Connection):
    p = SOURCE_ROOT / "data" / "equities" / "maritime_universe_catalog.csv"
    if not p.exists():
        return
    df = pd.read_csv(p)
    df.to_sql("dim_companies", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} companies into dim_companies")

def load_port_stress(conn: sqlite3.Connection):
    p = SOURCE_ROOT / "data" / "derived" / "port_stress_matrix.csv"
    if not p.exists():
        return
    df = pd.read_csv(p)
    df.to_sql("fact_port_stress", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} records into fact_port_stress")

def load_fixtures_sample(conn: sqlite3.Connection, limit: int = 100000):
    p = SOURCE_ROOT / "data" / "derived" / "fearnleys_fixtures_full.csv"
    if not p.exists():
        return
    print(f"Loading {limit} rows from Fearnleys fixtures...")
    df = pd.read_csv(p, nrows=limit, low_memory=False)
    df.rename(columns={"id": "fixture_id", "date": "fixture_date", "vessel": "vessel_name", "imo": "imo_number"}, inplace=True)
    df.to_sql("fact_fixtures", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} commercial fixtures into fact_fixtures")

def load_bunkers_sample(conn: sqlite3.Connection, limit: int = 100000):
    p = SOURCE_ROOT / "data" / "bunkers" / "bunker_master_historical.csv"
    if not p.exists():
        return
    print(f"Loading {limit} rows from Bunker master...")
    df = pd.read_csv(p, nrows=limit, low_memory=False)
    cols = [c for c in ["observation_date", "port_code", "port_name", "grade", "price_usd", "change_usd", "spread_usd"] if c in df.columns]
    df_sub = df[cols]
    df_sub.to_sql("fact_bunker_prices", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df_sub)} bunker price prints into fact_bunker_prices")

def load_sgx_sample(conn: sqlite3.Connection, limit: int = 50000):
    p = SOURCE_ROOT / "data" / "futures" / "sgx_cape_futures_history.csv"
    if not p.exists():
        return
    print(f"Loading {limit} rows from SGX Cape Futures...")
    df = pd.read_csv(p, nrows=limit)
    df["commodity_family"] = "Capesize FFA"
    df.rename(columns={"date": "quote_date"}, inplace=True)
    df.to_sql("fact_sgx_curves", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} SGX curve records into fact_sgx_curves")

def create_indexes(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tree_doc_id ON dim_tree_nodes(doc_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_parent ON fact_skipped_assets(parent_doc_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_date ON fact_fixtures(fixture_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_vessel ON fact_fixtures(vessel_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_charterer ON fact_fixtures(charterer);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bunker_date_port ON fact_bunker_prices(observation_date, port_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sgx_date ON fact_sgx_curves(quote_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_port_stress_locode ON fact_port_stress(port_locode, date);")
    conn.commit()
    print("Created high-speed query indexes.")

def test_multihop_tree_join(conn: sqlite3.Connection):
    print("\n--- Multi-Hop Join: Research Tree Node -> Chart Asset -> Market Fixture ---")
    query = """
    SELECT 
        t.doc_id,
        t.title,
        a.asset_id,
        a.local_path,
        f.fixture_date,
        f.vessel_name,
        f.charterer,
        f.rate
    FROM dim_tree_nodes t
    JOIN fact_skipped_assets a ON a.parent_doc_id = t.doc_id
    JOIN fact_fixtures f ON f.fixture_date = a.parent_date
    WHERE a.is_resolved_local = 1
      AND f.commodity LIKE '%Iron Ore%'
    LIMIT 3;
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    print(f"Found {len(rows)} fully cross-linked multi-hop connections:")
    for r in rows:
        print("  -> Title:", r[1])
        print("     Local Chart Path:", r[3])
        print(f"     Connected Fixture: {r[4]} | {r[5]} ({r[6]}) at rate {r[7]}\n")

def main():
    print(f"Building Maritime Knowledge Spine at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    load_tree_nodes(conn)
    load_p0_assets(conn)
    load_equities(conn)
    load_port_stress(conn)
    load_fixtures_sample(conn, limit=100000)
    load_bunkers_sample(conn, limit=100000)
    load_sgx_sample(conn, limit=50000)
    create_indexes(conn)
    test_multihop_tree_join(conn)
    conn.close()
    print(f"Successfully compiled extended maritime knowledge spine: {DB_PATH.stat().st_size / (1024*1024):.2f} MB")

if __name__ == "__main__":
    main()
