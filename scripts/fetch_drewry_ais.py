"""
fetch_drewry_ais.py
Downloads Drewry's weekly AIS Analytics PDFs directly by URL.

Confirmed via incognito-window test (Sep 2026): files at
drewry.co.uk/AcuCustom/Sitename/DAM/<id>/<filename>.pdf are NOT
gated -- no login, no cookies, no session required, despite the
marketing copy on the article page saying "Please Register and Login
to download weekly reports." That wall applies to the *article* page
that links to the files, not the files themselves. So this is a
plain, unauthenticated GET -- no login flow to script.

Cadence finding (see full_sweep's docstring for detail): this is NOT
one report per sector per week despite Drewry's "every week we
publish" copy. Real reports for a given sector/vessel-class turn up
roughly every 3 weeks, and different vessel classes within the same
sector are offset from each other -- so there's no shortcut left
except sweeping DAM id x week x filename template directly.

FILENAME CONVENTION CHANGED BETWEEN YEARS -- this is the important
one. A real 2025 file the user had already downloaded locally
(Drewry_AIS_PDF_Drybulk_Capesize_Week37_20251.pdf) revealed that 2025
used a different naming scheme than 2026:
  2025: Drewry_AIS_PDF_{class}_Week{NN}_{year}1.pdf   (note "PDF_"
        infix and an extra "1" appended after the year)
  2026: Drewry_AIS_{class}_Week{NN}_{year}.pdf         (no infix, no
        extra digit)
No amount of DAM-range guessing would have found this -- it took a
real historical file to reveal the actual convention. Confirmed via
direct sweep (Sep 2026): DAM/027-031 hold real 2025 AIS PDFs covering
weeks 1-50 (96% of the year) once the 2025 template is used. See
HISTORICAL_DAM_MAP below.

Modes:
  1. WEEKLY (default): guess this week's DAM id from the last known
     anchor, try all 2026-pattern filename templates.
  2. --backward [--weeks-back N]: walk backward from a confirmed
     anchor testing whether earlier weeks exist nearby.
  3. --full-sweep: brute-force every (DAM id, week, filename template)
     combination across configurable ranges, for whichever year's
     template you're testing.
  4. --historical: download everything in HISTORICAL_DAM_MAP directly
     -- no sweeping needed, every (DAM, week) pair below is already
     confirmed real.
"""

import os
import sys
import time
from datetime import date, timedelta

import requests

BASE = "https://www.drewry.co.uk/AcuCustom/Sitename/DAM"

VESSEL_CLASSES = [
    "Crude_Suezmax", "Crude_VLCC", "Crude_Aframax",
    "Drybulk_Panamax", "Drybulk_Capesize", "Drybulk_Supramax", "Drybulk_Handysize",
    "Product_LR2", "Product_LR1",
    "LPG_FR",
]

# 2026 pattern: no infix, plain year
FILENAME_TEMPLATES = [f"Drewry_AIS_{cls}_Week{{week}}_{{year}}.pdf" for cls in VESSEL_CLASSES]

# 2025 pattern: "PDF_" infix, extra "1" after the year -- confirmed real,
# not guessed, from a file already on the user's own machine
FILENAME_TEMPLATES_2025 = [f"Drewry_AIS_PDF_{cls}_Week{{week}}_{{year}}1.pdf" for cls in VESSEL_CLASSES]

# 2024 pattern: TWO conventions within the same year, switching mid-year --
# H1 uses the "no digit" form (matches FILENAME_TEMPLATES_2024 below),
# H2 switches to the SAME convention 2025 uses (infix + extra digit,
# matches FILENAME_TEMPLATES_2025). Confirmed by direct sweep after the
# H1-only pattern came up empty for weeks 26+.
FILENAME_TEMPLATES_2024 = [f"Drewry_AIS_PDF_{cls}_Week{{week}}_{{year}}.pdf" for cls in VESSEL_CLASSES]
FILENAME_TEMPLATES_2024_H2 = [f"Drewry_AIS_PDF_{cls}_Week{{week}}_{{year}}1.pdf" for cls in VESSEL_CLASSES]

# Confirmed via direct sweep (Sep 2026) -- every (dam_id, week) pair here
# was verified to hold at least one real PDF. Not exhaustive per vessel
# class within each week (matches the rotation pattern found for 2026),
# but the DAM id and template are right, so --historical finds whichever
# classes actually exist for that week.
#
# 2021/2022: no evidence this product existed in this form -- not
# attempted. 2023: confirmed to exist (Wayback snapshot, "Week 20-2023")
# but with a visibly different page structure (one download button, not
# ten per-class ones) -- tested 1,080 combinations across 3 filename
# conventions x wide DAM range, zero hits. Likely lives under a
# different URL path entirely, pre-dating this DAM/naming scheme.
HISTORICAL_DAM_MAP = {
    2024: [
        # H1 -- "no digit" template
        (23, [2, 4]),
        (24, [5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23]),
        (25, [24, 25]),
    ],
    2025: [
        (27, range(1, 11)),    # weeks 1-10
        (28, range(11, 24)),   # weeks 11-23
        (29, range(24, 35)),   # weeks 24-34
        (30, range(35, 51)),   # weeks 35-50
        (31, range(50, 51)),   # week 50 (partial overlap, one class confirmed)
        # weeks 51-52 checked at DAM 030-033, both 2025 and 2026 naming
        # patterns -- confirmed empty, likely a real holiday-week gap
    ],
}

# 2024 H2 uses the "digit" template (same shape as FILENAME_TEMPLATES_2025)
# with year=2024 -- kept as a separate map since historical_download's
# per-year template lookup can't otherwise express "this year, but only
# for these particular DAM/week entries, use the OTHER year's template."
HISTORICAL_DAM_MAP_H2_2024 = [
    (25, [26, 27, 28, 30, 31, 32, 33, 34, 35]),
    (26, [37, 38, 39, 40, 41, 42, 44, 45, 46, 47]),
    (27, [48, 49, 50, 51, 52]),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}




def try_fetch(dam_id: int, week: int, year: int, out_dir: str = "drewry_ais_pdfs", templates=None):
    templates = templates if templates is not None else FILENAME_TEMPLATES
    os.makedirs(out_dir, exist_ok=True)
    found = []
    for template in templates:
        filename = template.format(week=str(week).zfill(2), year=year)
        url = f"{BASE}/{dam_id:03d}/{filename}"
        resp = None
        for attempt in range(3):
            try:
                resp = requests.get(url, headers=HEADERS, timeout=20)
                break
            except requests.exceptions.RequestException as e:
                # A raw connection/SSL timeout, not an HTTP error -- confirmed to
                # happen (Sep 2026) when this script ran concurrently with
                # another one hitting the same server. Previously uncaught,
                # this crashed the entire multi-thousand-request sweep on a
                # single transient failure. Retry a couple times, then skip
                # this one file and move on rather than losing everything else.
                if attempt == 2:
                    print(f"  --   DAM/{dam_id:03d}  {filename}  (FAILED: {type(e).__name__} after 3 attempts)")
                else:
                    time.sleep(2)
        if resp is None:
            time.sleep(0.3)
            continue
        if resp.status_code == 200 and resp.headers.get("Content-Type") == "application/pdf":
            path = os.path.join(out_dir, filename)
            with open(path, "wb") as f:
                f.write(resp.content)
            print(f"  OK   DAM/{dam_id:03d}  {filename}  ({len(resp.content)} bytes)")
            found.append(filename)
        else:
            print(f"  --   DAM/{dam_id:03d}  {filename}  (status {resp.status_code})")
        time.sleep(0.3)  # light pacing -- these files are unauthenticated but still live traffic
    return found


def historical_download(out_dir: str = "drewry_ais_pdfs"):
    """Download everything in HISTORICAL_DAM_MAP (and HISTORICAL_DAM_MAP_H2_2024)
    directly -- no sweeping, every (DAM, week) pair here is already
    confirmed real. Covers 2024 weeks 2-52 (~90%, 5 genuine gaps) across
    two different naming conventions that switch mid-year, and 2025
    weeks 1-50 (96% of the year) -- three genuinely different filename
    conventions found across the two years, none of them guessable
    without real seed files/patterns to test against."""
    templates_by_year = {2024: FILENAME_TEMPLATES_2024, 2025: FILENAME_TEMPLATES_2025}
    total_found = 0
    for year, dam_entries in HISTORICAL_DAM_MAP.items():
        templates = templates_by_year.get(year, FILENAME_TEMPLATES)
        for dam_id, weeks in dam_entries:
            for week in weeks:
                found = try_fetch(dam_id, week, year, out_dir=out_dir, templates=templates)
                total_found += len(found)
    for dam_id, weeks in HISTORICAL_DAM_MAP_H2_2024:
        for week in weeks:
            found = try_fetch(dam_id, week, 2024, out_dir=out_dir, templates=FILENAME_TEMPLATES_2024_H2)
            total_found += len(found)
    print(f"\nHistorical download complete: {total_found} files.")


def weekly_mode(dam_id_guess: int, spread: int = 2):
    """Try dam_id_guess and a few ids on either side, for this week's
    number. A 404 across the whole range is most likely "not published
    yet" (Drewry hasn't posted this week's report) rather than "wrong
    id" -- rerun in a day or two before assuming the guess needs a
    bigger adjustment. A hit at dam_id_guess + 1 tells you the DAM
    sequence moved by 1 since last week; update the default you pass
    in next time."""
    today = date.today()
    iso_year, iso_week, _ = today.isocalendar()
    candidates = range(dam_id_guess - spread, dam_id_guess + spread + 1)
    print(f"This week: {iso_year}-W{iso_week}. Trying DAM/{candidates.start:03d}-{candidates.stop - 1:03d}...")
    for dam_id in candidates:
        found = try_fetch(dam_id, iso_week, iso_year)
        if found:
            print(f"\nFound at DAM/{dam_id:03d} -- use this as next week's dam_id_guess.")
            return
    print(
        f"\nNothing found across DAM/{candidates.start:03d}-{candidates.stop - 1:03d} for "
        f"{iso_year}-W{iso_week}. Most likely this week's report just isn't published yet "
        f"(check the AIS Analytics landing page) -- rerun in a day or two before widening "
        f"`spread` further."
    )


def backward_sweep(anchor_dam: int = 33, anchor_week: int = 35, anchor_year: int = 2026,
                    weeks_back: int = 12, dam_slack: int = 1):
    """Test the DAM-increments-with-week hypothesis by walking backward
    from the one confirmed anchor point (DAM/033 = Week 35 2026,
    published 01 Sep 2026) instead of waiting for future weeks to
    publish -- if Week 35 exists, earlier weeks almost certainly do
    too, and there's no reason to wait to find out.

    Deliberately does NOT derive week numbers from a calendar date. An
    earlier version used date(2026, 9, 1).isocalendar() to compute
    week numbers, on the assumption Drewry's "Week 35" means ISO 8601
    week 35 -- it doesn't: Sep 1 2026 is actually ISO week 36. Drewry's
    internal week-labeling runs a week behind true ISO calendar weeks
    (or uses some other convention -- doesn't matter which). So this
    just decrements the confirmed (week, year) integers directly,
    sidestepping the mismatch entirely.

    For each week back, tries a small window of DAM ids around the
    naive 1-per-week decrement (dam_slack on each side), since there's
    no evidence the tracking is exactly 1:1 -- only that it's
    plausible from a single data point.

    Reports each week as HIT (with which DAM id it was actually found
    at, and how far that drifted from the naive guess) or MISS, so a
    pattern is visible directly in the output."""
    print(f"Walking backward from DAM/{anchor_dam:03d} = Week {anchor_week} {anchor_year} (confirmed anchor)...")
    print(f"Testing {weeks_back} prior weeks, +/-{dam_slack} DAM slack per week.\n")
    hits, misses = [], []
    week, year = anchor_week, anchor_year
    for weeks_ago in range(1, weeks_back + 1):
        week -= 1
        if week < 1:
            week = 52  # approximate -- doesn't matter for a slack-based search
            year -= 1
        naive_guess = anchor_dam - weeks_ago
        candidates = range(max(1, naive_guess - dam_slack), naive_guess + dam_slack + 1)
        found_dam = None
        for dam_id in candidates:
            found = try_fetch(dam_id, week, year)
            if found:
                found_dam = dam_id
                break
        if found_dam is not None:
            drift = found_dam - naive_guess
            print(f"HIT   {year}-W{week:02d}  -> DAM/{found_dam:03d}  "
                  f"(naive guess was {naive_guess:03d}, drift {drift:+d})")
            hits.append((year, week, found_dam))
        else:
            print(f"MISS  {year}-W{week:02d}  (tried DAM/{candidates.start:03d}-{candidates.stop - 1:03d})")
            misses.append((year, week))

    print(f"\n{len(hits)} hit(s), {len(misses)} miss(es) out of {weeks_back} weeks tested.")
    if not hits:
        print("Zero hits at all -- the 1-per-week DAM hypothesis doesn't hold, or dam_slack "
              "needs to be wider before concluding that. Widen dam_slack and retry first.")
    elif misses:
        print("Mix of hits and misses -- likely means the DAM sequence isn't purely "
              "sequential (other Drewry documents interleaved between AIS publishes, or a "
              "week was skipped/republished under a different id). Consider widening "
              "dam_slack rather than assuming history stops at the first miss.")
    else:
        print("Clean hits across the whole tested range -- the 1:1 hypothesis holds well "
              "enough to trust for backfill. Safe to widen weeks_back further.")
    return hits, misses


def full_sweep(dam_start: int = 15, dam_end: int = 35, week_start: int = 1, week_end: int = 52,
                year: int = 2026):
    """The real backfill, built on what was actually found rather than
    the earlier 1-DAM-per-week guess: within a single confirmed DAM id
    (033), real reports turned up at weeks 29, 32, and 35 -- a 3-week
    gap, not weekly, despite Drewry's own copy claiming "every week we
    publish." Different sector PDFs inside the same DAM folder also
    carried different week numbers (Crude/Suezmax at 29/32/35,
    Drybulk/Panamax at 31, LPG FR at 32) -- so each sector likely runs
    its own publishing cadence, not a shared one.

    That means there's no shortcut left to guess -- this sweeps every
    (DAM id, week number, filename template) combination in the given
    ranges. It's more requests than the earlier attempts (dam_range x
    week_range x 10 templates -- the defaults are 21 x 52 x 10 = ~10,920),
    but still cheap, unauthenticated GETs on files already confirmed
    public. Expect a low hit rate; most combinations are correctly
    empty, not broken."""
    print(f"Full sweep: DAM/{dam_start:03d}-{dam_end:03d} x weeks {week_start}-{week_end}, {year}")
    print(f"({(dam_end - dam_start + 1) * (week_end - week_start + 1) * len(FILENAME_TEMPLATES)} requests total)\n")
    all_found = {}
    for dam_id in range(dam_start, dam_end + 1):
        for week in range(week_start, week_end + 1):
            found = try_fetch(dam_id, week, year)
            if found:
                all_found[(dam_id, week)] = found
    print(f"\n{len(all_found)} (DAM, week) combination(s) with at least one real PDF:")
    for (dam_id, week), files in sorted(all_found.items()):
        print(f"  DAM/{dam_id:03d} Week {week:02d}: {', '.join(files)}")
    return all_found


if __name__ == "__main__":
    if "--historical" in sys.argv:
        historical_download()
    elif "--full-sweep" in sys.argv:
        full_sweep()
    elif "--backward" in sys.argv:
        weeks_back = 12
        if "--weeks-back" in sys.argv:
            weeks_back = int(sys.argv[sys.argv.index("--weeks-back") + 1])
        backward_sweep(weeks_back=weeks_back)
    elif "--backfill" in sys.argv:
        print("--backfill is deprecated -- it swept DAM ids blindly with no anchor, which is "
              "far more expensive than --backward for no extra information. Use --backward "
              "[--weeks-back N] instead.")
    else:
        weekly_mode(dam_id_guess=33)  # last confirmed: DAM/033 = Week 35 2026. Update this
        # once you know Week 36's actual id -- see the landing-page check requested earlier.
