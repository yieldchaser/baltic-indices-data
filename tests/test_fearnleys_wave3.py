#!/usr/bin/env python3
"""Wave-3 Fearnleys desk caches: builders + frontend wiring.

- build_desk_caches: tenor long-table (10,055 real rows), NB prices (17
  series), asset curves (33 classes + 8 scrap series), gas extra — all from
  real derived CSVs, idempotent, nulls preserved.
- build_fixtures_tape: deduped (538k → 347k) tape (last 45 days) + facets.
- index.html: fetches wired for the new caches.
"""
import hashlib
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PY = sys.executable
HTML = (ROOT / "index.html").read_text(encoding="utf-8")


def run_script(rel: str, timeout: int = 1500) -> str:
    r = subprocess.run([PY, str(ROOT / rel)], capture_output=True, text=True,
                       cwd=ROOT, timeout=timeout)
    assert r.returncode == 0, f"{rel} failed:\n{r.stderr[-3000:]}"
    return r.stdout


def _build_and_check(rel, path, key_check):
    run_script(rel)
    d = json.loads(path.read_text(encoding="utf-8"))
    key_check(d)
    h1 = hashlib.sha256(path.read_bytes()).hexdigest()
    run_script(rel)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == h1, f"{rel} not idempotent"
    return d


def test_desk_caches_write_path_idempotent():
    tenor = _build_and_check(
        "scripts/fearnleys/build_desk_caches.py",
        ROOT / "data" / "derived" / "fearnleys_desk_tenor.json",
        lambda d: (_ for _ in ()).throw(AssertionError("tenor empty")) if len(d["rows"]) < 10000 else None)
    assert tenor["meta"]["rows"] == len(tenor["rows"])
    # spot-check one tenor row against source
    import pandas as pd
    src = pd.read_csv(ROOT / "data" / "derived" / "time_charter_rates.csv", usecols=["date", "capesize_1y_avg"])
    src = src.dropna().sort_values("date")
    last = src.iloc[-1]
    assert [last["date"], "Capesize", "1Y", round(float(last["capesize_1y_avg"]), 2)] in tenor["rows"]

    nb = json.loads((ROOT / "data" / "derived" / "fearnleys_nb_prices.json").read_text(encoding="utf-8"))
    assert nb["meta"]["series"] >= 17, nb["meta"]

    ac = json.loads((ROOT / "data" / "derived" / "fearnleys_asset_curves.json").read_text(encoding="utf-8"))
    assert len(ac["classes"]) >= 30 and len(ac["scrap"]) >= 8

    tape = _build_and_check(
        "scripts/fearnleys/build_fixtures_tape.py",
        ROOT / "data" / "derived" / "fearnleys_fixtures_tape.json",
        lambda d: (_ for _ in ()).throw(AssertionError("tape empty")) if d["meta"]["rows"] < 10000 else None)
    assert tape["meta"]["last"] >= "2026-09-01", tape["meta"]

    facets = json.loads((ROOT / "data" / "derived" / "fearnleys_fixtures_facets.json").read_text(encoding="utf-8"))
    assert facets["total_rows"] > 300000, facets["total_rows"]


def test_frontend_cache_wiring():
    for marker in [
        "fearnleys_desk_tenor.json", "fearnleys_nb_prices.json",
        "fearnleys_asset_curves.json", "fearnleys_gas_extra.json",
        "fearnleys_fixtures_tape.json", "fearnleys_fixtures_facets.json",
    ]:
        assert marker in HTML, marker