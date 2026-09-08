# Wave-1 Pipeline Truth

No `index.html` edits in Wave-1. All truth lives in scripts + workflow + caches.

## 1. Workflow — `data_expansion.yml` (Mon–Thu 05:00 UTC, cron `0 5 * * 1-4`)

Added two steps after the Fearnleys sync, before commit:

- **Geospatial voyage tracker** → `scripts/geospatial/build_geospatial_tracker.py`
- **Port stress matrix + cache** → `scripts/compute_port_stress_matrix.py`,
  then `scripts/congestion/build_port_stress_cache.py`

Step contract: `continue-on-error: true` + `timeout-minutes: 20` + file-exists
guard (`if [ -f ... ]`), same pattern as the existing collectors.
Idempotent: every script is a pure recompute from existing inputs
(deterministic hashes, `drop_duplicates`, full-file rewrite of its own
outputs only). Additive-only: no step deletes from `data/` or `docs/`; the
commit step still stages only `data/` + `docs/`.

## 2. Bunker `change_7d` — `scripts/bunkers/build_bunker_cache.py` §1b

Was hardcoded `0.0` and never updated. Now, per port: latest VLSFO master
observation on/before the port's `latest_date` minus the nearest VLSFO master
observation on/before (latest − 7 days), rounded to 2 dp. Both endpoints are
real `bunker_master_historical.csv` rows; `0.0` survives only when a port has
< 7 days of VLSFO history. Wrapped in try/except so a failure keeps the old
`0.0` defaults instead of breaking the cache.

## 3. Hub counts — 50 series / 41 physical hubs (reconciled)

- **Single source:** `PORT_METADATA` in `scripts/compute_port_stress_matrix.py`
  (50 port-asset series: 16 Dry Bulk + 11 Tankers + 11 LPG + 12 LNG).
- 41 = distinct physical UN/LOCODEs; 9 ports repeat across asset classes
  (NLRTM, CNQDG, CNNGB, USHOU, USCRP, AUDAM, AUGLT, QARLF, NGBON).
- `scripts/congestion/port_universe.py` re-exports that list (zero copies) and
  exposes `SERIES_COUNT` / `PHYSICAL_HUB_COUNT` / `validate_hub_counts()`.
- `scripts/congestion/build_port_stress_cache.py` groups the matrix CSV by
  `(locode, asset_class)` and now logs a non-fatal validation against the
  canonical counts — so `port_stress_summary.json` carries 50 `hubs` entries
  with `summary.total_monitored == 50` on complete input.
- **Frontend fallback:** `index.html` has no hardcoded hub list; it renders
  `summary.hubs` and shows `sm.total_monitored || 50`. The `|| 50` default
  matches the canonical 50-series count — no edit needed.
- Geospatial `PORT_COORDINATES` (46) is a different scope, not a competing
  truth: the 41 stress hubs plus 5 dry-bulk-only waypoints
  (IDKMT, INPRT, KRKAN, TRCKL, USSWP) needed for voyage reconstruction.

## 4. Write-path tests — `tests/test_wave1_pipeline_truth.py`

Each builder test: run script → re-read output from disk → assert content →
SHA-256 → re-run → assert hash unchanged (idempotent). Covers the bunker
cache (incl. real `change_7d`), the stress matrix CSV, the stress summary
JSON (50/41), the canonical universe import, and the frontend `|| 50`
fallback (read-only check).
