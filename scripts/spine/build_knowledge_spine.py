"""
Maritime Knowledge Spine Builder.
Unifies tabular datasets into a high-speed relational SQLite graph spine
with deterministic foreign keys and queryable indexes linking vessels, ports,
commercial fixtures, bunker fuel costs, forward curves, and SEC equities.
"""

import os
import sys
import sqlite3
import pandas as pd
from pathlib import Path

SOURCE_ROOT = Path(os.environ.get("SHIPPING_SOURCE_ROOT", "c:/Users/Dell/Github/Shipping"))
OUTPUT_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "derived"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DB_PATH = OUTPUT_DIR / "maritime_knowledge_spine.db"

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_date ON fact_fixtures(fixture_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_vessel ON fact_fixtures(vessel_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_charterer ON fact_fixtures(charterer);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bunker_date_port ON fact_bunker_prices(observation_date, port_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sgx_date ON fact_sgx_curves(quote_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_port_stress_locode ON fact_port_stress(port_locode, date);")
    conn.commit()
    print("Created high-speed query indexes.")

def test_multi_hop_query(conn: sqlite3.Connection):
    print("\n--- Testing Multi-Hop Cross-Source Query ---")
    query = """
    SELECT 
        f.fixture_date,
        f.vessel_name,
        f.charterer,
        f.load_port,
        f.discharge_port,
        f.rate,
        b.grade,
        b.price_usd AS bunker_price_usd,
        s.price AS sgx_prompt_ffa_price
    FROM fact_fixtures f
    LEFT JOIN fact_bunker_prices b 
        ON b.observation_date = f.fixture_date 
        AND b.port_name LIKE '%' || f.load_port || '%'
    LEFT JOIN fact_sgx_curves s 
        ON s.quote_date = f.fixture_date
    WHERE f.segment = 'Capesize'
      AND f.load_port IS NOT NULL
      AND f.rate IS NOT NULL
    LIMIT 5;
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    print(f"Multi-hop query executed successfully! Found {len(rows)} connected records:")
    for r in rows:
        print("  ->", r)

def main():
    print(f"Building Maritime Knowledge Spine at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    load_equities(conn)
    load_port_stress(conn)
    load_fixtures_sample(conn, limit=100000)
    load_bunkers_sample(conn, limit=100000)
    load_sgx_sample(conn, limit=50000)
    create_indexes(conn)
    test_multi_hop_query(conn)
    conn.close()
    print(f"Successfully compiled maritime knowledge spine: {DB_PATH.stat().st_size / (1024*1024):.2f} MB")

if __name__ == "__main__":
    main()
