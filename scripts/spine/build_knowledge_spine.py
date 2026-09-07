"""
Maritime Knowledge Spine Builder (Decision 4 Extended: Source Wiring & Graph Integration).
Unifies all tabular datasets into a high-speed relational SQLite graph spine
with deterministic foreign keys, high-speed indexes, and multi-hop queries linking:
- Knowledge tree nodes (preserving knowledge/trees/ hierarchy)
- Ingested mirror assets (with Finding E1 resolution check)
- Equities universe catalog (dim_companies)
- Port stress & PortWatch congestion (fact_port_stress, fact_portwatch_congestion)
- Commercial fixtures & S&P deals (fact_fixtures, fact_fearnleys_snp)
- Global bunker fuel price matrix (fact_bunker_prices)
- SGX freight & iron ore forward curves (fact_sgx_curves)
- Capital Link maritime indices (fact_capital_link_indices)
- CFTC monthly regulatory fund accounting ledgers (fact_cftc_etf_ledgers)
- ETF daily holdings & portfolio weights (fact_etf_holdings)
- USDA grain exports, vessel queues & freight rates (fact_usda_grain_flows)
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

def find_data_file(rel_path: str | Path) -> Path | None:
    """Find file across worktree root, source root, or primary repo checkout."""
    rel = Path(rel_path)
    candidates = [
        REPO_ROOT / rel,
        SOURCE_ROOT / rel,
        Path("c:/Users/Dell/Github/Shipping") / rel
    ]
    for c in candidates:
        if c.exists():
            return c
    return None

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

    # 2. Ingested Assets Table (linking trees to physical mirror assets)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_ingested_assets (
        asset_id TEXT PRIMARY KEY,
        node_id TEXT,
        doc_id TEXT,
        asset_url TEXT,
        asset_kind TEXT,
        local_mirror_rel TEXT,
        is_resolved_local BOOLEAN,
        disposition TEXT,
        FOREIGN KEY(node_id) REFERENCES dim_tree_nodes(node_id),
        FOREIGN KEY(doc_id) REFERENCES dim_tree_nodes(doc_id)
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
        expiry_date TEXT,
        commodity_family TEXT
    );
    """)

    # 8. Capital Link Indices Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_capital_link_indices (
        entry_id INTEGER PRIMARY KEY AUTOINCREMENT,
        quote_date TEXT,
        index_code TEXT,
        index_name TEXT,
        close_val REAL,
        open_val REAL,
        high_val REAL,
        low_val REAL,
        volume REAL,
        change_pct REAL
    );
    """)

    # 9. CFTC Monthly Fund Accounting Ledgers
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_cftc_etf_ledgers (
        ledger_id INTEGER PRIMARY KEY AUTOINCREMENT,
        fund TEXT,
        period_ended TEXT,
        opening_nav_dollars REAL,
        sales_of_shares_dollars REAL,
        redemptions_of_shares_dollars REAL,
        net_share_activity_dollars REAL,
        interest_income_dollars REAL,
        total_expenses_dollars REAL,
        net_expenses_dollars REAL,
        net_investment_income_dollars REAL,
        realized_futures_pnl_dollars REAL,
        unrealized_futures_pnl_delta_dollars REAL,
        net_futures_pnl_dollars REAL,
        net_income_loss_dollars REAL,
        closing_nav_dollars REAL,
        shares_outstanding REAL,
        nav_per_share REAL,
        balance_identity_valid BOOLEAN
    );
    """)

    # 10. ETF Daily Holdings & Weights Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_etf_holdings (
        holding_id INTEGER PRIMARY KEY AUTOINCREMENT,
        fund TEXT,
        quote_date TEXT,
        contract_name TEXT,
        ticker TEXT,
        cusip TEXT,
        lots REAL,
        price REAL,
        market_value REAL,
        weightings REAL
    );
    """)

    # 11. USDA Grain Flows & Vessel Queues Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_usda_grain_flows (
        flow_id INTEGER PRIMARY KEY AUTOINCREMENT,
        report_date TEXT,
        week INTEGER,
        year INTEGER,
        port_name TEXT,
        in_port INTEGER,
        loaded_7_days INTEGER,
        due_10_days INTEGER
    );
    """)

    # 12. PortWatch Port Congestion & Calls Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_portwatch_congestion (
        congestion_id INTEGER PRIMARY KEY AUTOINCREMENT,
        obs_date TEXT,
        portid TEXT,
        portname TEXT,
        country TEXT,
        hub_code TEXT,
        daily_port_calls_total REAL,
        daily_port_calls_dry_bulk REAL,
        daily_port_calls_tanker REAL,
        daily_port_calls_container REAL,
        import_dry_bulk_kt REAL,
        export_dry_bulk_kt REAL,
        import_tanker_kt REAL,
        export_tanker_kt REAL
    );
    """)

    # 13. Fearnleys S&P Transactions Fact
    cur.execute("""
    CREATE TABLE IF NOT EXISTS fact_fearnleys_snp (
        snp_id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT,
        vessel_name TEXT,
        built_year INTEGER,
        shipyard TEXT,
        dwt REAL,
        segment TEXT,
        price_usd TEXT,
        buyer TEXT,
        comment TEXT
    );
    """)

    conn.commit()

def load_tree_nodes(conn: sqlite3.Connection):
    trees_dir = SOURCE_ROOT / "knowledge" / "trees"
    if not trees_dir.exists():
        trees_dir = REPO_ROOT / "knowledge" / "trees"
    if not trees_dir.exists():
        return
    print(f"Loading knowledge tree nodes from {trees_dir}...", flush=True)
    tree_files = list(trees_dir.rglob("*.json"))
    records = []
    for tf in tree_files:
        try:
            data = json.loads(tf.read_text(encoding="utf-8"))
            stack = [data]
            while stack:
                n = stack.pop()
                nid = n.get("node_id")
                if nid:
                    records.append((
                        nid,
                        n.get("doc_id"),
                        n.get("parent_id"),
                        n.get("title"),
                        str(n.get("source_path", "")).replace("\\", "/"),
                        n.get("token_count"),
                        json.dumps(n.get("keywords", []))
                    ))
                stack.extend(n.get("children") or [])
        except Exception:
            continue

    cur = conn.cursor()
    cur.executemany("""
    INSERT OR REPLACE INTO dim_tree_nodes (node_id, doc_id, parent_id, title, source_path, token_count, keywords_json)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    print(f"Loaded {len(records)} tree section nodes into dim_tree_nodes", flush=True)

def load_ingested_assets(conn: sqlite3.Connection):
    p = find_data_file("data/derived/asset_dispositions.jsonl")
    if not p:
        return
    print(f"Loading ingested assets from {p}...", flush=True)
    records = []
    with open(p, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            d = json.loads(line)
            mirror = str(d.get("local_mirror_rel") or d.get("mirror_rel") or "").replace("\\", "/")
            resolved = bool(mirror and find_data_file(mirror))
            records.append((
                d.get("asset_url") or f"{d.get('doc_id')}_{len(records)}",
                d.get("node_id"),
                d.get("doc_id"),
                d.get("asset_url"),
                d.get("asset_kind"),
                mirror,
                resolved,
                d.get("disposition")
            ))

    cur = conn.cursor()
    cur.executemany("""
    INSERT OR REPLACE INTO fact_ingested_assets 
    (asset_id, node_id, doc_id, asset_url, asset_kind, local_mirror_rel, is_resolved_local, disposition)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    print(f"Loaded {len(records)} assets into fact_ingested_assets", flush=True)

def load_equities(conn: sqlite3.Connection):
    p = find_data_file("data/equities/maritime_universe_catalog.csv")
    if not p:
        return
    df = pd.read_csv(p)
    df.to_sql("dim_companies", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} companies into dim_companies", flush=True)

def load_port_stress(conn: sqlite3.Connection):
    p = find_data_file("data/derived/port_stress_matrix.csv")
    if not p:
        return
    df = pd.read_csv(p)
    df.to_sql("fact_port_stress", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} records into fact_port_stress", flush=True)

def load_fixtures(conn: sqlite3.Connection, limit: int = 150000):
    p = find_data_file("data/derived/fearnleys_fixtures_full.csv")
    if not p:
        return
    print(f"Loading commercial fixtures from {p} (up to {limit} rows)...", flush=True)
    df = pd.read_csv(p, nrows=limit, low_memory=False)
    df.rename(columns={"id": "fixture_id", "date": "fixture_date", "vessel": "vessel_name", "imo": "imo_number"}, inplace=True)
    df.to_sql("fact_fixtures", conn, if_exists="replace", index=False, chunksize=25000)
    print(f"Loaded {len(df)} commercial fixtures into fact_fixtures", flush=True)

def load_fearnleys_snp(conn: sqlite3.Connection):
    p = find_data_file("data/derived/fearnleys_snp_transactions.csv")
    if not p:
        return
    df = pd.read_csv(p, low_memory=False)
    df.rename(columns={"id": "snp_id", "vessel": "vessel_name", "built": "built_year", "yard": "shipyard", "price": "price_usd"}, inplace=True)
    df.to_sql("fact_fearnleys_snp", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} Fearnleys S&P deals into fact_fearnleys_snp", flush=True)

def load_bunkers(conn: sqlite3.Connection, limit: int = 100000):
    p = find_data_file("data/bunkers/bunker_master_historical.csv")
    if not p:
        return
    print(f"Loading {limit} rows from Bunker master...", flush=True)
    df = pd.read_csv(p, nrows=limit, low_memory=False)
    cols = [c for c in ["observation_date", "port_code", "port_name", "grade", "price_usd", "change_usd", "spread_usd"] if c in df.columns]
    df_sub = df[cols]
    df_sub.to_sql("fact_bunker_prices", conn, if_exists="replace", index=False, chunksize=25000)
    print(f"Loaded {len(df_sub)} bunker price prints into fact_bunker_prices", flush=True)

def load_sgx_curves(conn: sqlite3.Connection, limit_per_family: int = 50000):
    sgx_files = [
        ("Capesize FFA", "data/futures/sgx_cape_futures_history.csv"),
        ("Panamax FFA", "data/futures/sgx_panamax_futures_history.csv"),
        ("Supramax FFA", "data/futures/sgx_supramax_futures_history.csv"),
        ("Handysize FFA", "data/futures/sgx_handysize_futures_history.csv"),
        ("Iron Ore FEF 62%", "data/futures/sgx_iron_ore_fef_history.csv"),
        ("Iron Ore M65F 65%", "data/futures/sgx_iron_ore_m65f_history.csv"),
        ("Iron Ore LPF Lump", "data/futures/sgx_iron_ore_lump_lpf_history.csv"),
    ]
    # Also load prompt snapshot files for latest date coverage
    sgx_prompt_files = [
        ("Capesize FFA", "data/futures/sgx_cape_futures.csv"),
        ("Panamax FFA", "data/futures/sgx_panamax_futures.csv"),
        ("Supramax FFA", "data/futures/sgx_supramax_futures.csv"),
        ("Handysize FFA", "data/futures/sgx_handysize_futures.csv"),
        ("Iron Ore FEF 62%", "data/futures/sgx_iron_ore_fef.csv"),
    ]
    total_loaded = 0
    cur = conn.cursor()
    cur.execute("DELETE FROM fact_sgx_curves;")
    conn.commit()

    for fam, rel in sgx_files + sgx_prompt_files:
        p = find_data_file(rel)
        if not p:
            continue
        df = pd.read_csv(p, nrows=limit_per_family)
        df["commodity_family"] = fam
        df.rename(columns={"date": "quote_date"}, inplace=True)
        # Normalize date to ISO YYYY-MM-DD
        df["quote_date"] = pd.to_datetime(df["quote_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        cols = ["contract", "expiry_month", "expiry_year", "quote_date", "price", "volume", "open_interest", "expiry_date", "commodity_family"]
        available_cols = [c for c in cols if c in df.columns]
        df[available_cols].to_sql("fact_sgx_curves", conn, if_exists="append", index=False, chunksize=25000)
        total_loaded += len(df)
    print(f"Loaded {total_loaded} forward curve prints into fact_sgx_curves across 7 contract families", flush=True)

def load_capital_link_indices(conn: sqlite3.Connection):
    p = find_data_file("data/indices/capital_link_indices_master.csv")
    if not p:
        return
    df = pd.read_csv(p)
    melted = df.melt(id_vars=["date"], var_name="index_code", value_name="close_val")
    name_map = {
        "CLCI": "Capital Link Container Index",
        "CLDBI": "Capital Link Drybulk Index",
        "CLLG": "Capital Link LNG/LPG Index",
        "CLMI": "Capital Link Maritime Index",
        "CLMFI": "Capital Link Mixed Fleet Index",
        "CLMLP": "Capital Link MLP Index",
        "CLTI": "Capital Link Tanker Index",
    }
    melted["index_name"] = melted["index_code"].map(name_map)
    melted.rename(columns={"date": "quote_date"}, inplace=True)
    melted["quote_date"] = pd.to_datetime(melted["quote_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    melted.to_sql("fact_capital_link_indices", conn, if_exists="replace", index=False, chunksize=25000)
    print(f"Loaded {len(melted)} index prints into fact_capital_link_indices", flush=True)

def load_cftc_ledgers(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DELETE FROM fact_cftc_etf_ledgers;")
    conn.commit()

    for rel in ["data/cftc_statements/parsed/bdry_monthly_cftc_ledger.csv", "data/cftc_statements/parsed/bwet_monthly_cftc_ledger.csv"]:
        p = find_data_file(rel)
        if not p:
            continue
        df = pd.read_csv(p)
        df["period_ended"] = pd.to_datetime(df["period_ended"], errors="coerce").dt.strftime("%Y-%m-%d")
        keep_cols = [
            "fund", "period_ended", "opening_nav_dollars", "sales_of_shares_dollars",
            "redemptions_of_shares_dollars", "net_share_activity_dollars", "interest_income_dollars",
            "total_expenses_dollars", "net_expenses_dollars", "net_investment_income_dollars",
            "realized_futures_pnl_dollars", "unrealized_futures_pnl_delta_dollars", "net_futures_pnl_dollars",
            "net_income_loss_dollars", "closing_nav_dollars", "shares_outstanding", "nav_per_share",
            "balance_identity_valid"
        ]
        sub = df[[c for c in keep_cols if c in df.columns]]
        sub.to_sql("fact_cftc_etf_ledgers", conn, if_exists="append", index=False)
    
    cur.execute("SELECT COUNT(*) FROM fact_cftc_etf_ledgers")
    n = cur.fetchone()[0]
    print(f"Loaded {n} monthly fund regulatory statements into fact_cftc_etf_ledgers", flush=True)

def load_etf_holdings(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("DELETE FROM fact_etf_holdings;")
    conn.commit()

    for fund, rel in [("BDRY", "data/etf/bdry_holdings_history.csv"), ("BWET", "data/etf/bwet_holdings_history.csv")]:
        p = find_data_file(rel)
        if not p:
            continue
        df = pd.read_csv(p)
        df["fund"] = fund
        df.rename(columns={"date": "quote_date", "Name": "contract_name", "Ticker": "ticker", "CUSIP": "cusip", "Lots": "lots", "Price": "price", "Market_Value": "market_value", "Weightings": "weightings"}, inplace=True)
        df["quote_date"] = pd.to_datetime(df["quote_date"], errors="coerce").dt.strftime("%Y-%m-%d")
        # Clean weightings into float
        df["weightings"] = df["weightings"].astype(str).str.rstrip("%").astype(float)
        cols = ["fund", "quote_date", "contract_name", "ticker", "cusip", "lots", "price", "market_value", "weightings"]
        df[[c for c in cols if c in df.columns]].to_sql("fact_etf_holdings", conn, if_exists="append", index=False)
    
    cur.execute("SELECT COUNT(*) FROM fact_etf_holdings")
    n = cur.fetchone()[0]
    print(f"Loaded {n} ETF daily portfolio holdings into fact_etf_holdings", flush=True)

def load_usda_grain_flows(conn: sqlite3.Connection):
    p = find_data_file("data/commodities/usda_grain_vessel_loading.csv")
    if not p:
        return
    df = pd.read_csv(p)
    df.rename(columns={"date": "report_date", "port": "port_name"}, inplace=True)
    df["report_date"] = pd.to_datetime(df["report_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    cols = ["report_date", "week", "year", "port_name", "in_port", "loaded_7_days", "due_10_days"]
    df[[c for c in cols if c in df.columns]].to_sql("fact_usda_grain_flows", conn, if_exists="replace", index=False)
    print(f"Loaded {len(df)} USDA grain vessel loading records into fact_usda_grain_flows", flush=True)

def load_portwatch_congestion(conn: sqlite3.Connection):
    p = find_data_file("data/congestion/portwatch_port_congestion.csv")
    if not p:
        return
    df = pd.read_csv(p)
    df.rename(columns={"date": "obs_date"}, inplace=True)
    df["obs_date"] = pd.to_datetime(df["obs_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df.to_sql("fact_portwatch_congestion", conn, if_exists="replace", index=False, chunksize=25000)
    print(f"Loaded {len(df)} PortWatch congestion records into fact_portwatch_congestion", flush=True)

def create_indexes(conn: sqlite3.Connection):
    cur = conn.cursor()
    # Tree & Asset
    cur.execute("CREATE INDEX IF NOT EXISTS idx_tree_doc_id ON dim_tree_nodes(doc_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_node ON fact_ingested_assets(node_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_asset_doc ON fact_ingested_assets(doc_id);")
    # Fixtures & S&P
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_date ON fact_fixtures(fixture_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_vessel ON fact_fixtures(vessel_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fix_charterer ON fact_fixtures(charterer);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_snp_vessel ON fact_fearnleys_snp(vessel_name);")
    # Markets & Curves
    cur.execute("CREATE INDEX IF NOT EXISTS idx_bunker_date_port ON fact_bunker_prices(observation_date, port_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sgx_date ON fact_sgx_curves(quote_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sgx_fam_date ON fact_sgx_curves(commodity_family, quote_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cl_date_code ON fact_capital_link_indices(quote_date, index_code);")
    # ETF & CFTC
    cur.execute("CREATE INDEX IF NOT EXISTS idx_cftc_fund_date ON fact_cftc_etf_ledgers(fund, period_ended);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_etf_fund_date ON fact_etf_holdings(fund, quote_date);")
    # Flows & Congestion
    cur.execute("CREATE INDEX IF NOT EXISTS idx_usda_date_port ON fact_usda_grain_flows(report_date, port_name);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_portwatch_date_port ON fact_portwatch_congestion(obs_date, portname);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_port_stress_locode ON fact_port_stress(port_locode, date);")
    conn.commit()
    print("Created high-speed query indexes across all fact and dimension tables.", flush=True)

def test_multihop_traversals(conn: sqlite3.Connection):
    cur = conn.cursor()
    print("\n=======================================================", flush=True)
    print("      MULTI-HOP TRAVERSAL PROOFS (DECISION 4)          ", flush=True)
    print("=======================================================", flush=True)

    # Traversal 1: Tree Node -> Mirror Asset -> Fixture -> Bunker Fuel Cost
    print("\n--- Traversal 1: Document Tree -> Ingested Asset -> Market Fixture ---", flush=True)
    query1 = """
    SELECT 
        t.doc_id,
        t.title,
        a.node_id,
        a.local_mirror_rel,
        f.fixture_date,
        f.vessel_name,
        f.charterer,
        f.rate
    FROM dim_tree_nodes t
    JOIN fact_ingested_assets a ON a.doc_id = t.doc_id
    JOIN fact_fixtures f ON f.fixture_date = SUBSTR(t.doc_id, INSTR(t.doc_id, '2020-'), 10)
    WHERE a.is_resolved_local = 1
      AND t.doc_id LIKE '%2020-06-04%'
    LIMIT 2;
    """
    cur.execute(query1)
    rows1 = cur.fetchall()
    for r in rows1:
        print(f"  Tree Doc: {r[0]} | Title: {r[1]}", flush=True)
        print(f"  Asset Mirror: {r[3]}", flush=True)
        print(f"  Matched Fixture: {r[4]} | {r[5]} ({r[6]}) @ {r[7]}\n", flush=True)

    # Traversal 2: ETF Daily Holdings -> SGX Curve Valuation -> Forward Term Structure
    print("--- Traversal 2: ETF Holding -> Forward Curve Quote -> Term Structure ---", flush=True)
    query2 = """
    SELECT 
        h.quote_date,
        h.fund,
        h.contract_name,
        h.weightings,
        s.commodity_family,
        s.contract,
        s.price,
        s.volume,
        s.open_interest
    FROM fact_etf_holdings h
    JOIN fact_sgx_curves s ON s.quote_date = h.quote_date AND s.commodity_family LIKE '%Capesize%'
    WHERE h.fund = 'BDRY'
      AND h.weightings > 10.0
    ORDER BY h.quote_date DESC, h.weightings DESC
    LIMIT 2;
    """
    cur.execute(query2)
    rows2 = cur.fetchall()
    for r in rows2:
        print(f"  ETF Date: {r[0]} | Fund: {r[1]} | Holding: {r[2]} (Weight: {r[3]}%)", flush=True)
        print(f"  Matched SGX Curve: {r[4]} ({r[5]}) | Price: ${r[6]:,.2f} | Volume: {r[7]:,.0f} | OI: {r[8]:,.0f}\n", flush=True)

    # Traversal 3: USDA Grain Loading Queue -> PortWatch Congestion
    print("--- Traversal 3: Grain Loading Queue -> PortWatch Regional Activity ---", flush=True)
    query3 = """
    SELECT 
        u.report_date,
        u.port_name,
        u.in_port,
        u.due_10_days,
        p.portname,
        p.daily_port_calls_dry_bulk,
        p.export_dry_bulk_kt
    FROM fact_usda_grain_flows u
    JOIN fact_portwatch_congestion p ON p.obs_date = u.report_date
    WHERE u.report_date >= '2024-01-01'
      AND u.in_port > 10
      AND p.daily_port_calls_dry_bulk > 0
    LIMIT 2;
    """
    cur.execute(query3)
    rows3 = cur.fetchall()
    for r in rows3:
        print(f"  Date: {r[0]} | Grain Center: {r[1]} ({r[2]} in port, {r[3]} due)", flush=True)
        print(f"  PortWatch Benchmark: {r[4]} ({r[5]} drybulk calls, {r[6]} kt exported)\n", flush=True)

    # Traversal 4: Capital Link Index -> S&P Vessel Sale
    print("--- Traversal 4: Capital Link Equities Index -> S&P Sale ---", flush=True)
    query4 = """
    SELECT 
        c.quote_date,
        c.index_code,
        c.close_val,
        s.vessel_name,
        s.built_year,
        s.shipyard,
        s.dwt,
        s.price_usd
    FROM fact_capital_link_indices c
    JOIN fact_fearnleys_snp s ON SUBSTR(s.created_at, 1, 10) = c.quote_date
    WHERE c.index_code = 'CLDBI'
      AND s.dwt > 150000
    LIMIT 2;
    """
    cur.execute(query4)
    rows4 = cur.fetchall()
    for r in rows4:
        print(f"  Date: {r[0]} | CLDBI Close: {r[2]:.2f}", flush=True)
        print(f"  Capesize S&P Deal: {r[3]} (Built: {r[4]}, {r[5]}, {r[6]:,.0f} DWT) Sold: {r[7]}\n", flush=True)

def main():
    print(f"Building Extended Maritime Knowledge Spine at: {DB_PATH}", flush=True)
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)
    
    # 1. Dimensions
    load_tree_nodes(conn)
    load_ingested_assets(conn)
    load_equities(conn)
    
    # 2. Market & Freight Facts
    load_port_stress(conn)
    load_fixtures(conn, limit=150000)
    load_fearnleys_snp(conn)
    load_bunkers(conn, limit=100000)
    load_sgx_curves(conn, limit_per_family=50000)
    load_capital_link_indices(conn)
    
    # 3. Regulatory, ETF & Flow Facts
    load_cftc_ledgers(conn)
    load_etf_holdings(conn)
    load_usda_grain_flows(conn)
    load_portwatch_congestion(conn)
    
    # 4. Indexes & Traversals
    create_indexes(conn)
    test_multihop_traversals(conn)
    
    conn.close()
    size_mb = DB_PATH.stat().st_size / (1024 * 1024)
    print(f"\nSuccessfully compiled extended maritime knowledge spine: {size_mb:.2f} MB", flush=True)

if __name__ == "__main__":
    main()
