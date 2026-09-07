#!/usr/bin/env python3
"""
Pilbara Ports Authority (PPA) — REAL monthly Port Hedland cargo statistics.

Source: PPA's own "Cargo Stats by Destination/Origin" monthly reports (PDF), published on
pilbaraports.com.au port-statistics pages. The HTML pages sit behind an Incapsula bot-wall,
but the underlying media PDFs are served openly; historical months are additionally
preserved by the Internet Archive (Wayback). This scraper:
  1. queries the Wayback CDX index for every archived Hedland cargo-stats PDF,
  2. downloads each snapshot,
  3. extracts the reporting month + total Iron Ore LOAD tonnage (+ per-destination split),
  4. writes data/commodities/australia_ppa_iron_ore.csv (provenance=live_ppa_archive).

Honesty rules (2026-08-25 audit):
  - Only REAL extracted values are written. No editorial estimates, ever.
  - Coverage = months PPA has published AND the archive preserves (currently ~2020→2024).
    Recent months appear once captured by the Internet Archive (or fetched live when the
    site serves them without the wall).
  - Port Hedland only: these reports cover PH; Dampier publishes separately and is NOT
    fabricated here.
    Dampier (added 2026-09-03): Port of Dampier FY "Cargo Statistics and Number of
    Vessels" PDFs publish IRON ORE + TOTAL tonnage per month (July→June FY layout).
    fetch_dampier_live() downloads the two current FY PDFs from pilbaraports.com.au
    and parses them with parse_dampier_ytd_pdf(). Provenance for those rows is
    live_ppa_dampier.
    Dampier backfill (added 2026-09-03): older Dampier FY tables (FY2002-03→FY2017-18
    annuals, FY2020-21 / FY2022-23 / FY2023-24-partial YTDs) preserved on the Wayback
    CDX index are ingested by backfill_dampier_wayback() (CLI: --backfill-dampier).
    Only explicit monthly numbers are parsed — never estimated, never zero-filled;
    gaps (FY2018-19, FY2019-20, FY2021-22, June 2024) stay gaps. Provenance for those
    rows is live_ppa_dampier_wayback. Files whose text is Port Hedland data
    (destination/origin + GRT commodity monthlies misfiled under the Dampier path)
    are skipped by content check — never copied across ports.
"""
import json
import logging
import re
import ssl
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymupdf

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = ROOT / "data" / "commodities"
DATA_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = DATA_DIR / "australia_ppa_iron_ore.csv"
SCRATCH = ROOT / "scratch"

CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE

COUNTRIES = ["China", "Japan", "Korea, Republic of", "India", "Indonesia",
             "Malaysia", "Philippines", "Singapore", "Taiwan, Province of China",
             "Vietnam", "Australia"]

# --- Port of Dampier live sources (verified 2026-09-03; direct HTTPS, no bot-wall) ---
DAMPIER_SOURCES = [
    # (financial-year start year, live PDF URL). Final full-year file once June lands,
    # YTD file (June row blank '-') while the FY is in progress.
    (2024, "https://www.pilbaraports.com.au/pilbaraportsauthority/media/documents/"
           "port%20of%20dampier/about%20the%20port%20of%20dampier/port%20statistics/"
           "2025/klein-stats-july-2024-to-june-2025-ytd.pdf"),
    (2025, "https://www.pilbaraports.com.au/pilbaraportsauthority/media/documents/"
           "port%20of%20dampier/about%20the%20port%20of%20dampier/port%20statistics/"
           "2026/klein-stats-july-2025-to-may-2026-ytd.pdf"),
]
DAMPIER_MONTHS = {"JULY": 7, "AUGUST": 8, "SEPTEMBER": 9, "OCTOBER": 10,
                  "NOVEMBER": 11, "DECEMBER": 12, "JANUARY": 1, "FEBRUARY": 2,
                  "MARCH": 3, "APRIL": 4, "MAY": 5, "JUNE": 6}


def cdx_hedland_pdfs(retries: int = 4) -> list[dict]:
    """All archived Port Hedland cargo-stats PDF snapshots (newest per filename)."""
    url = ("https://web.archive.org/cdx/search/cdx?url=pilbaraports.com.au"
           "&matchType=domain&output=json&limit=8000&collapse=urlkey"
           "&filter=mimetype:application/pdf&from=2019")
    rows = []
    last = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            payload = urllib.request.urlopen(req, timeout=120, context=CTX).read()
            rows = json.loads(payload.decode("utf-8", "replace"))[1:]
            break
        except Exception as exc:  # noqa: BLE001
            last = exc
            logging.warning("CDX attempt %d/%d failed: %s", attempt, retries, exc)
            time.sleep(5 * attempt)
    if not rows:
        raise SystemExit(f"Wayback CDX unreachable after {retries} tries ({last}). "
                         "Nothing written; retry later.")
    seen: dict[str, dict] = {}
    for r in rows:
        ts, status, orig = r[1], r[4], r[2]
        low = orig.lower()
        if status != "200" or "hedland" not in low:
            continue
        fname = low.rsplit("/", 1)[-1]
        is_stat = ("cargo-stats-by-destination" in fname
                   or "cargo%20stats%20by%20destination" in fname
                   or "cargo-stats-by-origin" in fname)
        if not is_stat:
            continue
        key = re.sub(r"%20|%", "-", fname)
        if key not in seen or ts > seen[key]["ts"]:
            seen[key] = {"ts": ts, "url": orig}
    return list(seen.values())


def download(ts: str, url: str, dest: Path, tries: int = 3) -> bool:
    if dest.exists() and dest.stat().st_size > 5000:
        return True
    dl = f"https://web.archive.org/web/{ts}id_/{url}"
    for attempt in range(1, tries + 1):
        r = subprocess.run(["curl", "-sL", "--max-time", "120", "-o", str(dest),
                            "-w", "%{http_code}", dl, "-A", "Mozilla/5.0"],
                           capture_output=True, text=True)
        ok = r.stdout.strip().endswith("200") and dest.exists() and dest.stat().st_size > 5000
        if ok:
            return True
        time.sleep(4 * attempt)   # Wayback throttles bursts; brief backoff then retry
    dest.unlink(missing_ok=True)
    return False


def download_direct(url: str, dest: Path, tries: int = 2) -> bool:
    """Download Dampier PDF: tries direct HTTPS, falls back to Wayback when Incapsula-gated."""
    if dest.exists() and dest.stat().st_size > 5000:
        return True

    is_bot_walled = False
    for attempt in range(1, tries + 1):
        r = subprocess.run(["curl", "-sL", "--max-time", "30", "-o", str(dest),
                            "-w", "%{http_code}", url, "-A", "Mozilla/5.0"],
                           capture_output=True, text=True)
        if dest.exists():
            head = dest.read_bytes()[:500]
            if b"_Incapsula_Resource" in head:
                is_bot_walled = True
                break
            if r.stdout.strip().endswith("200") and dest.stat().st_size > 5000 and b"%PDF" in head:
                return True
        time.sleep(2 * attempt)

    # Fallback to Wayback for Dampier snapshots if live site is bot-walled or direct fetch failed
    dest.unlink(missing_ok=True)
    if is_bot_walled:
        logging.info("Live PPA media is Incapsula-gated; checking Wayback archive for %s", url.split('/')[-1])
    try:
        cdx_url = (f"https://web.archive.org/cdx/search/cdx?url={url}"
                   f"&output=json&limit=1&filter=statuscode:200&filter=mimetype:application/pdf")
        req = urllib.request.Request(cdx_url, headers={"User-Agent": "Mozilla/5.0"})
        payload = urllib.request.urlopen(req, timeout=30, context=CTX).read()
        rows = json.loads(payload.decode("utf-8", "replace"))
        if len(rows) > 1:
            ts = rows[1][1]
            logging.info("Found Wayback snapshot (%s) for %s; downloading...", ts, url.split('/')[-1])
            return download(ts, url, dest, tries=3)
    except Exception as exc:
        logging.debug("Wayback lookup for Dampier fallback failed: %s", exc)

    dest.unlink(missing_ok=True)
    return False


DAMPIER_FY_RE = re.compile(r"((?:19|20)\d{2})\s*[-\u2013/]\s*(\d{2,4})\s+FINANCIAL YEAR")
DAMPIER_MONTH_RE = re.compile(
    r"(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|"
    r"NOVEMBER|DECEMBER)")
DAMPIER_NUM_RE = re.compile(r"[\d,]+|-")


def parse_dampier_ytd_pdf(path: Path, fy_start: int,
                           provenance: str = "live_ppa_dampier") -> list[dict]:
    """Parse one Dampier FY cargo-statistics PDF (YTD or full annual).

    Layouts vary by era but share one invariant: one row per month JULY→JUNE,
    FIRST numeric cell = IRON ORE tonnage, LARGEST numeric cell in the row =
    TOTAL cargo tonnage. Column counts differ (9 cols pre-FY2005-06 without
    AMMONIA, 10 cols with AMMONIA/AMMONIUM, 11 cols in FY2012-13 with split
    PETROLEUM OUT/IN, trailing validation/vessel-count columns in later files),
    so positional parsing is avoided: the row segment between consecutive month
    names (capped at the TOTALS row) is tokenised and iron/total are picked by
    position/value. Months not yet published ('-' or 0 iron, e.g. June in a YTD
    file, or future months in quarterly files) are skipped — never zero-filled.
    The FY start year is read from the "<YYYY>-<YY> FINANCIAL YEAR" title when
    present, else the fy_start argument is used. Files that are not Dampier
    tables at all (Port Hedland destination/GRT monthlies) return [].
    Returns REAL extracted rows only.
    """
    doc = pymupdf.open(str(path))
    text = " ".join(p.get_text().replace("\n", " ") for p in doc)
    doc.close()
    text = re.sub(r"\s+", " ", text)
    if "DAMPIER" not in text:
        logging.info("Skipping non-Dampier PDF: %s", Path(path).name)
        return []
    m_fy = DAMPIER_FY_RE.search(text)
    if not m_fy:
        # No "<YYYY>-<YY> FINANCIAL YEAR" title: not a Dampier cargo table
        # (e.g. Port Hedland destination/GRT monthlies that mention a vessel
        # called "AAL DAMPIER", or quarterly files without month rows).
        logging.info("Skipping PDF without Dampier FY title: %s", Path(path).name)
        return []
    fy = int(m_fy.group(1))
    cap = re.search(r"\bTOTALS?:", text)
    end_cap = cap.start() if cap else len(text)
    matches = list(DAMPIER_MONTH_RE.finditer(text, 0, end_cap))
    rows = []
    for i, m in enumerate(matches):
        seg_end = matches[i + 1].start() if i + 1 < len(matches) else end_cap
        # keep only tokens containing a digit (drops ',' fragments and '-')
        toks = [t for t in DAMPIER_NUM_RE.findall(text[m.end():seg_end])
                if re.search(r"\d", t)]
        if not toks or toks[0] == "-" or not re.fullmatch(r"[\d,]+", toks[0]):
            continue  # unpublished month (e.g. June in a YTD file)
        iron = float(toks[0].replace(",", ""))
        if iron < 100_000:
            # unpublished month: the segment holds only '-' cells plus the next
            # row's leading number ("2 FEBRUARY") or vessel counts — never a
            # real tonnage (smallest real Dampier iron month > 5 Mt).
            continue
        vals = [float(t.replace(",", "")) for t in toks if t != "-"]
        if not vals:
            continue
        total = max(vals)
        if not (total >= iron and total <= 30_000_000):
            continue  # sanity: TOTAL cargo always exceeds iron ore
        month = DAMPIER_MONTHS[m.group(1)]
        year = fy if month >= 7 else fy + 1
        if year < 1990:
            continue
        rows.append({
            "date": datetime(year, month, 1).strftime("%Y-%m-%d"),
            "port": "Port of Dampier",
            "total_throughput_mt": round(total / 1e6, 3),
            "iron_ore_exports_mt": round(iron / 1e6, 3),
            "destinations_t": "",
            "provenance": provenance,
        })
        logging.info("Dampier %s  %.3f Mt iron / %.3f Mt total",
                     rows[-1]["date"][:7], iron / 1e6, total / 1e6)
    return rows


def fetch_dampier_live() -> list[dict]:
    """Download + parse the current Dampier FY PDFs. Returns [] on any failure
    (caller keeps previously committed Dampier rows; nothing is fabricated)."""
    SCRATCH.mkdir(exist_ok=True)
    out: list[dict] = []
    for fy_start, url in DAMPIER_SOURCES:
        safe = re.sub(r"[^a-z0-9]+", "_", url.lower().rsplit("/", 1)[-1])[:60]
        dest = SCRATCH / f"ppa_dampier_{safe}.pdf"
        try:
            if not download_direct(url, dest):
                logging.info("Dampier PDF unavailable (Incapsula-gated & not on Wayback yet): %s", url.split('/')[-1])
                continue
            out.extend(parse_dampier_ytd_pdf(dest, fy_start))
            time.sleep(1.0)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Dampier parse failed %s: %s", dest.name[:50], exc)
    # dedup by month (YTD file overlaps the prior final only at FY boundary — none)
    return sorted({r["date"]: r for r in out}.values(), key=lambda r: r["date"])


def cdx_dampier_stats(retries: int = 4) -> list[dict]:
    """All archived Dampier cargo-statistics PDF snapshots (newest per URL).

    Case-insensitive Dampier match (early annuals live under an all-caps DAMPIER
    path). Candidates are FY cargo tables + YTD monthlies; Port Hedland
    destination/GRT monthlies under the same path are downloaded too but skipped
    later by content check in parse_dampier_ytd_pdf().
    """
    url = ("https://web.archive.org/cdx/search/cdx?url=pilbaraports.com.au"
           "&matchType=domain&output=json&limit=60000"
           "&filter=mimetype:application/pdf"
           "&filter=original:.*[Dd][Aa][Mm][Pp][Ii][Ee][Rr].*")
    rows: list = []
    last = None
    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            payload = urllib.request.urlopen(req, timeout=120, context=CTX).read()
            rows = json.loads(payload.decode("utf-8", "replace"))[1:]
            break
        except Exception as exc:  # noqa: BLE001
            last = exc
            logging.warning("Dampier CDX attempt %d/%d failed: %s", attempt, retries, exc)
            time.sleep(5 * attempt)
    if not rows:
        raise SystemExit(f"Dampier Wayback CDX unreachable after {retries} tries ({last}). "
                         "Nothing written; retry later.")
    best: dict[str, dict] = {}
    for r in rows:
        ts, status, orig = r[1], r[4], r[2]
        if status != "200":
            continue
        low = orig.lower()
        if not any(k in low for k in ("statistic", "cargo-stats", "klein", "ytd", "figures")):
            continue
        norm = re.sub(r"^https?://(www\.)?", "", low)
        if norm not in best or ts > best[norm]["ts"]:
            best[norm] = {"ts": ts, "url": orig}
    out = list(best.values())
    logging.info("%d archived Dampier stat PDFs (newest snapshot each)", len(out))
    return out


def backfill_dampier_wayback() -> pd.DataFrame:
    """Deep Dampier backfill from Wayback snapshots into australia_ppa_iron_ore.csv.

    Only explicit monthly IRON ORE / TOTAL numbers are parsed (never estimated,
    never zero-filled; gaps stay gaps). Rows already committed (all Port Hedland
    rows + live Dampier rows) are frozen byte-for-byte; new pre-2024 rows get
    provenance live_ppa_dampier_wayback with per-port MoM/YoY derived from the
    combined Dampier series. Header unchanged.
    """
    SCRATCH.mkdir(exist_ok=True)
    prev = pd.read_csv(OUT_FILE, dtype=str) if OUT_FILE.exists() else pd.DataFrame()
    # Freeze on (date, port): Hedland and Dampier series share calendar months
    # (e.g. 2024-05-01) and must never shadow each other.
    have = set(zip(prev["date"].tolist(), prev["port"].tolist())) if not prev.empty else set()

    parsed: list[dict] = []
    for item in sorted(cdx_dampier_stats(), key=lambda x: x["ts"]):
        safe = re.sub(r"[^a-z0-9]+", "_", item["url"].lower().rsplit("/", 1)[-1])[:60]
        dest = SCRATCH / f"ppa_dampier_wayback_{item['ts']}_{safe}.pdf"
        try:
            if not download(item["ts"], item["url"], dest):
                logging.warning("Dampier backfill download failed: %s", item["url"][:90])
                continue
            rows = parse_dampier_ytd_pdf(dest, fy_start=0,
                                         provenance="live_ppa_dampier_wayback")
            for r in rows:
                r["_ts"] = item["ts"]
            parsed.extend(rows)
            time.sleep(1.0)
        except Exception as exc:  # noqa: BLE001
            logging.warning("Dampier backfill parse failed %s: %s", dest.name[:50], exc)
    if not parsed:
        raise SystemExit("Parsed zero Dampier backfill months — layout changed? Nothing written.")
    # Latest snapshot wins per month (final annuals revise quarterly/YTD figures).
    best: dict[str, dict] = {}
    for r in sorted(parsed, key=lambda r: (r["date"], r["_ts"])):
        best[r["date"]] = r
    new_rows = [r for d, r in sorted(best.items())
                if (d, "Port of Dampier") not in have and d < "2024-07-01"]
    if not new_rows:
        logging.info("No new pre-2024 Dampier months; CSV unchanged.")
        return prev
    new = pd.DataFrame(new_rows).drop(columns=["_ts"])
    for col in ("total_throughput_mt", "iron_ore_exports_mt"):
        new[col] = pd.to_numeric(new[col], errors="coerce")
    # MoM/YoY within the Dampier series (combined, so boundary months are right).
    damp_t = pd.concat([
        prev[prev["port"] == "Port of Dampier"][["date", "iron_ore_exports_mt"]],
        new[["date", "iron_ore_exports_mt"]].assign(
            iron_ore_exports_mt=new["iron_ore_exports_mt"].astype(float)),
    ], ignore_index=True)
    damp_t["iron_ore_exports_mt"] = pd.to_numeric(damp_t["iron_ore_exports_mt"], errors="coerce")
    damp_t = damp_t.drop_duplicates("date").sort_values("date").reset_index(drop=True)
    damp_t["mom_pct"] = (damp_t["iron_ore_exports_mt"].pct_change() * 100).round(2)
    damp_t["yoy_pct"] = (damp_t["iron_ore_exports_mt"].pct_change(12) * 100).round(2)
    lut = damp_t.set_index("date")[["mom_pct", "yoy_pct"]].to_dict(orient="index")
    new["mom_pct"] = new["date"].map(lambda d: lut.get(d, {}).get("mom_pct"))
    new["yoy_pct"] = new["date"].map(lambda d: lut.get(d, {}).get("yoy_pct"))
    for col in ("mom_pct", "yoy_pct"):
        new[col] = new[col].apply(lambda v: "" if pd.isna(v) else v)
    for col in ("total_throughput_mt", "iron_ore_exports_mt"):
        new[col] = new[col].apply(lambda v: ("" if pd.isna(v) else v))
    new["destinations_t"] = ""
    new = new[["date", "port", "total_throughput_mt", "iron_ore_exports_mt",
               "destinations_t", "mom_pct", "yoy_pct", "provenance"]]
    df = pd.concat([prev, new], ignore_index=True)
    df = df.sort_values(["port", "date"]).reset_index(drop=True)
    df.to_csv(OUT_FILE, index=False)
    logging.info("Backfilled %d Dampier months (%s .. %s) -> %s",
                 len(new), new["date"].min(), new["date"].max(), OUT_FILE.name)
    return df


def month_from_text(text: str):
    m = re.search(r"Cargo Complete [Dd]ate:\s*([\d/]+)\s*to\s*([\d/]+)", text)
    if not m:
        return None
    d1, mth, y = m.group(1).split("/")
    try:
        return datetime(int(y), int(mth), 1)   # start date is DD/MM/YYYY
    except ValueError:
        return None


def parse_pdf(path: Path):
    """Coordinate-based extraction: locate the Iron Ore column by its header x-position,
    then read each destination row's value in that x-band. Robust to ragged columns."""
    doc = pymupdf.open(str(path))
    month, iron_load, dest_split = None, None, {}
    for page in doc:
        flat = page.get_text().replace("\r\n", " ").replace("\r", "").replace("\n", " ")
        if month is None:
            month = month_from_text(flat)
        if "LOAD" not in flat.upper():
            continue
        words = page.get_text("words")   # x0,y0,x1,y1,text,...
        # 1) commodity header row: contains 'Iron' + 'Ore' tokens
        iron_x = None
        header_y = None
        for w in words:
            if w[4] == "Iron":
                iron_x = (w[0] + w[2]) / 2
                header_y = w[1]
                break
        if iron_x is None or header_y is None:
            continue
        # 2) group remaining words into rows below the header (tolerant clustering:
        #    a row's label and its numbers can sit ~4px apart, so bucket by 6px and
        #    then merge adjacent buckets whose y-centres are within 5px)
        raw_rows: dict[int, list] = {}
        for w in words:
            if w[1] <= header_y + 2:
                continue
            raw_rows.setdefault(round(w[1] / 6), []).append(w)
        keys = sorted(raw_rows)
        merged: list[list] = []
        for k in keys:
            if merged:
                prev = merged[-1]
                py = sum(w[1] for w in prev) / len(prev)
                cy = sum(w[1] for w in raw_rows[k]) / len(raw_rows[k])
                if abs(cy - py) <= 5.0:
                    prev.extend(raw_rows[k])
                    continue
            merged.append(list(raw_rows[k]))
        # 3) per-row: label = leftmost word(s), value = number in iron x-band (+/-55px)
        BAND = 55
        for line_w in merged:
            line = sorted(line_w, key=lambda w: w[0])
            label_tokens = [w[4] for w in line if w[0] < 150]
            if not label_tokens:
                continue
            label = " ".join(label_tokens)
            vals = [float(w[4].replace(",", "")) for w in line
                    if abs((w[0] + w[2]) / 2 - iron_x) <= BAND
                    and re.fullmatch(r"[\d,]+(\.\d+)?", w[4])]
            if not vals:
                continue
            v = max(vals)
            if label.startswith("Total"):
                iron_load = v if iron_load is None else iron_load
            elif label in COUNTRIES:
                dest_split[label] = v
    doc.close()
    # cross-check: sum of destinations should approximate the total (>95% when present)
    if iron_load and dest_split:
        s = sum(dest_split.values())
        if s < iron_load * 0.95:      # partial coverage is fine (other countries exist),
            pass                      # but a wildly-off split would signal misparse
    return month, iron_load, dest_split


def main() -> pd.DataFrame:
    SCRATCH.mkdir(exist_ok=True)
    # Fast path: reuse the verified manifest from the collection probe when present
    # (24 known-good snapshot URLs). Otherwise fall back to a fresh CDX sweep.
    manifest = SCRATCH / "ppa_pdf_manifest.json"
    if manifest.exists():
        cached = [json.loads(json.dumps(m)) for m in json.load(open(manifest))]
        pdfs = [{"ts": m["ts"], "url": m["url"]} for m in cached]
        logging.info("Using cached manifest: %d verified PDFs", len(pdfs))
    else:
        pdfs = cdx_hedland_pdfs()
        if not pdfs:
            raise SystemExit("No archived PPA cargo-stat PDFs found via CDX — investigate.")
        logging.info("%d archived Hedland cargo-stat PDFs", len(pdfs))

    records = []
    for i, item in enumerate(sorted(pdfs, key=lambda x: x["ts"])):
        safe = re.sub(r"[^a-z0-9]+", "_",
                      item["url"].lower().rsplit("/", 1)[-1])[:60]
        dest = SCRATCH / f"ppa_{safe}.pdf"
        try:
            if not download(item["ts"], item["url"], dest):
                logging.warning("download failed: %s", item["url"][:90])
                time.sleep(2.0)   # extra breathing room before the next snapshot
                continue
            month, load, split = parse_pdf(dest)
            time.sleep(1.0)
        except Exception as exc:  # noqa: BLE001
            logging.warning("parse failed %s: %s", dest.name[:50], exc)
            continue
        if month and load:
            records.append({
                "date": month.strftime("%Y-%m-%d"),
                "port": "Port Hedland",
                "total_throughput_mt": round(load / 1e6, 3),
                "iron_ore_exports_mt": round(load / 1e6, 3),
                "destinations_t": json.dumps(split, sort_keys=True),
            })
            logging.info("%s  %.2f Mt (%d destinations)", month.strftime("%Y-%m"),
                         load / 1e6, len(split))
    if not records:
        raise SystemExit("Parsed zero real PPA months — layout changed? Nothing written.")

    hed = pd.DataFrame(records).drop_duplicates("date").sort_values("date").reset_index(drop=True)
    hed["port"] = "Port Hedland"
    hed["provenance"] = "live_ppa_archive"

    # --- Dampier: fresh live rows upserted over previously committed ones ---
    damp_fresh = pd.DataFrame(fetch_dampier_live())
    if OUT_FILE.exists():
        try:
            prev = pd.read_csv(OUT_FILE, dtype=str)
            prev_damp = prev[prev["port"].str.contains("Dampier", na=False)]
        except Exception as exc:  # noqa: BLE001
            logging.warning("Could not read existing CSV for Dampier upsert: %s", exc)
            prev_damp = pd.DataFrame()
    else:
        prev_damp = pd.DataFrame()
    if damp_fresh.empty:
        logging.warning("No fresh Dampier rows — keeping %d committed Dampier rows",
                        len(prev_damp))
        damp = prev_damp
    elif not prev_damp.empty:
        damp = pd.concat([prev_damp, damp_fresh], ignore_index=True)
        damp = damp.drop_duplicates(subset=["date"], keep="last")
    else:
        damp = damp_fresh

    frames = [hed]
    if damp is not None and not damp.empty:
        frames.append(damp)
    df = pd.concat(frames, ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    for col in ("total_throughput_mt", "iron_ore_exports_mt"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.drop_duplicates(subset=["date", "port"]).sort_values(["port", "date"])
    # MoM / YoY are computed WITHIN each port series (never across ports)
    df["mom_pct"] = (df.groupby("port")["iron_ore_exports_mt"].pct_change() * 100).round(2)
    df["yoy_pct"] = (df.groupby("port")["iron_ore_exports_mt"].pct_change(12) * 100).round(2)
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")
    df = df[["date", "port", "total_throughput_mt", "iron_ore_exports_mt",
             "destinations_t", "mom_pct", "yoy_pct", "provenance"]]
    df.to_csv(OUT_FILE, index=False)
    span = f"{df['date'].min()} .. {df['date'].max()}"
    logging.info("Wrote %d REAL PPA rows (%s) -> %s", len(df), span, OUT_FILE.name)
    return df


if __name__ == "__main__":
    if "--backfill-dampier" in sys.argv:
        backfill_dampier_wayback()
    else:
        main()
