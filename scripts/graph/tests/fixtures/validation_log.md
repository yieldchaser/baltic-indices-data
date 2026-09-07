# LightRAG mock validation log (muse-spark, Decision 3)

- lightrag-hku==1.5.7 (pinned, sandbox pip install)
- fixtures: 20 initial + 5 incremental real tree sections (2 docs: Advanced Shipping wk35 + Star Asia wk35)
- initial: 20 docs -> 55 entities / 44 relations
- chunks: 20 (exactly 1 per section — no re-chunking); entities without source: 0
- entities spanning both docs: 17
  - vessel `Mount Dampier` (vessel) from 2 chunks, docs: ['broker_reports_broker_report_2026-08-29_advanced_shipping_29_08_2026_advanced_shipping_trading_weekly_shipping_marke', 'broker_reports_broker_report_2026-08-31_general_broker_31_08_2026_star_asia_shipbroking_weekly_market_report_week_35']
  - vessel `Efraim A` (vessel) from 2 chunks, docs: ['broker_reports_broker_report_2026-08-29_advanced_shipping_29_08_2026_advanced_shipping_trading_weekly_shipping_marke', 'broker_reports_broker_report_2026-08-31_general_broker_31_08_2026_star_asia_shipbroking_weekly_market_report_week_35']
  - vessel `Amaryllis` (vessel) from 2 chunks, docs: ['broker_reports_broker_report_2026-08-29_advanced_shipping_29_08_2026_advanced_shipping_trading_weekly_shipping_marke', 'broker_reports_broker_report_2026-08-31_general_broker_31_08_2026_star_asia_shipbroking_weekly_market_report_week_35']
  - vessel `Spar Scorpio` (vessel) from 2 chunks, docs: ['broker_reports_broker_report_2026-08-29_advanced_shipping_29_08_2026_advanced_shipping_trading_weekly_shipping_marke', 'broker_reports_broker_report_2026-08-31_general_broker_31_08_2026_star_asia_shipbroking_weekly_market_report_week_35']
  - vessel `Arklow Spirit` (vessel) from 2 chunks, docs: ['broker_reports_broker_report_2026-08-29_advanced_shipping_29_08_2026_advanced_shipping_trading_weekly_shipping_marke', 'broker_reports_broker_report_2026-08-31_general_broker_31_08_2026_star_asia_shipbroking_weekly_market_report_week_35']
- incremental: +5 docs -> 57 nodes (+2); original chunks preserved: True; original nodes kept: 55, lost: 0
- query citations (20): `ing_trading_weekly_shipping_marke__s02_market_report_content`, `dvanced_shipping_trading_weekly_shipping_marke__s03_capesize`, `hipping_trading_weekly_shipping_marke__s04_kamsarmax_panamax`, `ipping_trading_weekly_shipping_marke__s06_handymax_handysize`, `d_shipping_trading_weekly_shipping_marke__s11_reported_sales`, `shipping_trading_weekly_shipping_marke__s22_demolition_sales`, `g_weekly_shipping_marke__s29_shipping_indicative_share_value`, `hipbroking_weekly_market_report_week_35__s03_market_overview`
- fail-closed (keys scrubbed): ollama: fail-closed OK: backend 'ollama' fail-closed: missing required env OLLAMA_BASE_URL, OLLAMA; nim: fail-closed OK: backend 'nim' fail-closed: missing required env NIM_API_KEY (or NVIDIA_API; openrouter: fail-closed OK: backend 'openrouter' fail-closed: missing required env OPENROUTER_API_KEY 
- store: 12 files, 669239 bytes under knowledge/graph/lightrag_store
- git violations before/after: 0/0

> Answer sample:
>
> Reported vessels in the retrieved graph context: - Mount Dampier: ear | Price | Buyer | Comments Capesize | Mount Dampier | 181.469 | 2011 | Imabari, Japan | 11/2026 | MAN-B&W | - | xs $ 38m | European | Scrubber fitted Kamsar - Crimsom Saturn: CRIMSOM SATURN | WOOD CHIP CARRIER | 9,759 | 2001 / JAPAN | 496 | DELIVERED CHATTOGRAM, ABOUT 200 TONS ROBS INCLUDED. BURSA | TANKER | 15,945 | 1999 / JAPAN | 4 - Glory Bridge: GLORY BRIDGE | SMAX | 50,077 | 2001 / JAPAN | 7.5 | CHINESE BUYERS - Princess Eternity: PRINCESS ETERNITY | CAPE | 182,263 | 2022 / JAPAN | 78.0 | UNDISCLOSED - Efraim A:  | European | Scrubber fitted Kamsarmax | Efraim A | 82.174 | 2010 |<SEP>MOUNT DAMPIER | CAPE | 181,469 | 2011 / JAPAN | 38.0 | MERCURIA ENERGY GROUP HOLDING LT - Amaryllis: B&W | - | $ 20m | Undisclosed\nUltramax | Amaryllis | 63.500 | 2013 | Yangzhou, China | 08/2028 | MAN-B&W | 4x35T | Low-Mid $ 24m | C
