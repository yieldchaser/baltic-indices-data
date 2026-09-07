# P1 calibration fixtures (muse-spark)

Provenance (M2): `p1_pass1.py` / `p1_pass2.py` are the pre-merge temp-run
scripts scrubbed to POSIX repo-relative paths (B5) — no Windows absolute
paths, no cross-worktree imports. NOT rebuilt: logic is line-identical to the
2026-09-06 temp run; only path handling changed (`REPO_ROOT` derives from this
file's location, outputs land in this dir). The three JSON/JSONL outputs are
verbatim copies of that run (already repo-relative; verified free of absolute
paths before copying).

Rerun (reproduces the recorded counts):

    python3 calibration/p1/p1_pass1.py   # -> p1_pass1_extract.json
    python3 calibration/p1/p1_pass2.py   # -> p1_pass2_verify.json + audit log

Recorded: A raw 9x10 -> pass1a FAIL (8 vs manual 5) -> redo 5x10 PASS;
B 13 visual fragments -> text-grouped 9x4 PASS + subtotal tie-out exact;
C 5 ledger-first linked-image entries (see the P1 doc: that survey sampled
ingested assets — struck in favour of the M1 true-skipped reselection).

M3: `verify_table.py` is the proposed shared-harness location — both branches
import `ExtractionVerifier` from here after merge
(`sys.path.insert(0, "<repo>/calibration/p1")`); no committed file imports
from `shipping-antigravity`. P1 results were cross-checked with the sibling
harness pre-merge: identical pass/fail and issue check names on these
fixtures. One deliberate determinism fix vs the sibling: the repeated-header
diagnostic is sorted.

M4/B6: the `expected_rows` (tolerance +-1) / `expected_cols` (exact)
assertions are implemented in `verify_table.py`, plus POSIX normalization of
`source_file` on every result and audit entry.
