"""CI support for the live 35-image re-OCR pilot (NOT part of the reviewed harness).

Subcommands:
  preflight   Set-size cap check, $25 spend-projection gate, live venue probe in
              directive priority (nim -> ollama -> openrouter -> groq). Writes
              data/derived/pilot_reocr_out/choice.json and exports env for later
              steps via $GITHUB_ENV so `reocr_pilot.py --venue auto` resolves to
              the verified vision-capable venue (failed venues are blanked).
  account     Build data/derived/pilot_reocr_out/cost_latency.json from
              audit.jsonl + results.json + choice.json (per-image latencies from
              venue_ok events, token sums from vendor usage blocks, $ via the
              chosen venue's stated rate, extrapolation math vs 13,591 assets).

Probe cost is bounded: at most one vision ping per candidate venue (<=4 calls),
counted against the 140-call pilot budget (PROBE_CALLS exported for the run step).
"""
from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
OUT_DIR = REPO_ROOT / "data" / "derived" / "pilot_reocr_out"
SET_PATH = REPO_ROOT / "data" / "derived" / "pilot_image_set.jsonl"

MAX_IMAGES = 35
SPEND_GATE_USD = 25.0
TOTAL_CALL_BUDGET = 140
# Conservative per-call token ceilings for the projection gate.
PROBE_TOK_IN, PROBE_TOK_OUT = 6000, 1500
# Conservative $/MTok ceilings (Sep 2026; real billing may be lower).
PRICE_CEIL = {
    "nim": (1.00, 1.00),
    "ollama": (1.00, 1.00),
    "openrouter": (0.20, 0.20),
    "groq": (0.30, 0.60),
}
# Pinned vision-capable models: the repo-configured OPENROUTER/GROQ defaults are
# text-only (llama-3.3-70b-instruct / gpt-oss-120b), so the pilot pins vision
# models here (recorded in choice.json). NIM/Ollama probe with their
# secret-configured models first (repo preference); no blind override.
PINNED_MODELS = {
    "openrouter": "meta-llama/llama-3.2-11b-vision-instruct",
    "groq": "meta-llama/llama-4-scout-17b-16e-instruct",
}
# Stated $/MTok list rates for actuals (verified Sep 2026 vendor listings).
LIST_RATES = {
    "openrouter": {"model": PINNED_MODELS["openrouter"],
                   "in_mtok": 0.055, "out_mtok": 0.055,
                   "source": "OpenRouter model listing, Sep 2026"},
    "groq": {"model": PINNED_MODELS["groq"],
             "in_mtok": 0.11, "out_mtok": 0.34,
             "source": "Groq pricing listing, Sep 2026"},
}


def load_harness():
    spec = importlib.util.spec_from_file_location(
        "reocr_pilot", REPO_ROOT / "scripts" / "pilot" / "reocr_pilot.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def gh_env(lines: dict):
    ghp = os.environ.get("GITHUB_ENV")
    if not ghp:
        return
    with open(ghp, "a", encoding="utf-8") as f:
        for k, v in lines.items():
            f.write(f"{k}={v}\n")


def cmd_preflight() -> int:
    import requests  # noqa: PLC0415 (CI installs it in the prior step)

    H = load_harness()
    records = [json.loads(l) for l in open(SET_PATH, encoding="utf-8") if l.strip()]
    if len(records) > MAX_IMAGES:
        print(f"PREFLIGHT FAIL: set has {len(records)} images, cap is {MAX_IMAGES}")
        return 1
    if not records:
        print("PREFLIGHT FAIL: empty image set")
        return 1

    # --- spend projection gate (worst case over candidate venues) ---
    worst = max((PROBE_TOK_IN * pi + PROBE_TOK_OUT * po) / 1e6
                for pi, po in PRICE_CEIL.values())
    projected = TOTAL_CALL_BUDGET * worst
    print(f"spend gate: {TOTAL_CALL_BUDGET} calls x ${worst:.4f}/call ceiling "
          f"= ${projected:.2f} projected (gate ${SPEND_GATE_USD:.2f})")
    if projected > SPEND_GATE_USD:
        print("PREFLIGHT FAIL: projected spend exceeds $25 gate — aborting before any call")
        return 1

    # --- venue probe in directive priority order ---
    smallest = min(records, key=lambda r: (REPO_ROOT / r["image_rel"]).stat().st_size)
    img_path = REPO_ROOT / smallest["image_rel"]
    b64, mime, w, h, n = H.encode_image(img_path)
    ping = 'Reply ONLY as JSON: {"ping": "ok", "chart_type": "table"}'
    transcript = []
    chosen = None
    probe_calls = 0

    order = ["nim", "ollama", "openrouter", "groq"]
    for venue in order:
        if venue in PINNED_MODELS:
            os.environ["OPENROUTER_MODEL" if venue == "openrouter" else "GROQ_MODEL"] = \
                PINNED_MODELS[venue]
            H = load_harness()  # re-read env for pinned model
        avail = H.venue_available(venue)
        model = {"nim": H.NIM_MODEL, "ollama": H.OLLAMA_MODEL,
                 "openrouter": H.OPENROUTER_MODEL, "groq": H.GROQ_MODEL}[venue]
        entry = {"venue": venue, "model": model, "available": avail}
        if not avail:
            entry["result"] = "skipped (no credentials/model configured)"
            transcript.append(entry)
            continue
        try:
            if venue == "ollama":
                url = f"{H.OLLAMA_BASE_URL}/chat"
                payload = H.build_ollama_chat_payload(H.OLLAMA_MODEL, ping, b64)
                headers_real = {"Content-Type": "application/json"}
                if H.OLLAMA_API_KEY:
                    headers_real["Authorization"] = f"Bearer {H.OLLAMA_API_KEY}"
            else:
                conf = {"nim": (H.NIM_BASE_URL, H.NIM_MODEL, H.NIM_API_KEY),
                        "openrouter": (H.OPENROUTER_BASE_URL, H.OPENROUTER_MODEL,
                                       H.OPENROUTER_API_KEY),
                        "groq": (H.GROQ_BASE_URL, H.GROQ_MODEL, H.GROQ_API_KEY)}[venue]
                base, model, key = conf
                url = f"{base}/chat/completions"
                payload = H.build_openai_compat_payload(model, ping, b64, mime)
                headers_real = {"Content-Type": "application/json",
                                "Authorization": "Bearer REDACTED"}
                headers_real["Authorization"] = "Bearer " + key
            probe_calls += 1
            resp = requests.post(url, json=payload,
                                 headers={k: v for k, v in headers_real.items()},
                                 timeout=60)
            entry["http"] = resp.status_code
            if resp.status_code == 200:
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                if venue == "ollama":
                    text = ((data.get("message") or {}).get("content") or "").strip()
                else:
                    ch = data.get("choices") or []
                    text = (((ch[0].get("message") if ch else {}) or {}).get("content")
                            or "").strip()
                if text:
                    entry["result"] = f"OK ({len(text)} chars)"
                    transcript.append(entry)
                    chosen = {"venue": venue, "model": model}
                    break
                entry["result"] = "empty completion"
            else:
                entry["result"] = f"HTTP {resp.status_code}: {resp.text[:160]!r}"
        except Exception as exc:
            entry["result"] = f"{type(exc).__name__}: {str(exc)[:160]}"
        transcript.append(entry)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if chosen is None:
        choice = {"status": "no_live_venue", "n_images": len(records),
                  "projected_spend_usd": round(projected, 4),
                  "probe_calls": probe_calls, "transcript": transcript,
                  "note": "All probes failed/skipped; harness will exit 2 (no live venue). "
                          "Do NOT fall back to mock — report the blocker."}
        json.dump(choice, open(OUT_DIR / "choice.json", "w", encoding="utf-8"), indent=2)
        print(json.dumps(choice, indent=2))
        return 1

    why = {
        "nim": "repo-configured NIM model proved vision-capable on live probe "
               "(directive priority 1).",
        "ollama": "NIM unusable; Ollama model proved vision-capable on live probe "
                  "(directive priority 2).",
        "openrouter": "NIM/Ollama unusable on probe; OpenRouter pinned to "
                      f"{PINNED_MODELS['openrouter']} because the repo-configured "
                      "default is text-only (directive priority 3).",
        "groq": "NIM/Ollama/OpenRouter unusable on probe; Groq pinned to "
                f"{PINNED_MODELS['groq']} because the repo-configured default "
                "is text-only (directive priority 4).",
    }[chosen["venue"]]
    choice = {"status": "live", "n_images": len(records),
              "venue": chosen["venue"], "model": chosen["model"], "why": why,
              "projected_spend_usd": round(projected, 4),
              "spend_gate_usd": SPEND_GATE_USD,
              "probe_calls": probe_calls,
              "call_budget_for_run": TOTAL_CALL_BUDGET - probe_calls,
              "antigravity_reference": {
                  "commit": "568a76787",
                  "their_order": "nim -> ollama -> openrouter -> groq "
                                 "(meta/llama-3.2-11b-vision-instruct family)",
                  "their_run_was_mock": True,
                  "mock_evidence": "95 audit events within ~70ms "
                                    "(2026-09-07T05:11:44.99Z..05:11:45.06Z), zero network "
                                    "latency; results contain fabricated declarations; "
                                    "tests run in 0.06s with mock responder.",
              },
              "transcript": transcript}
    json.dump(choice, open(OUT_DIR / "choice.json", "w", encoding="utf-8"), indent=2)

    # Shape env so `--venue auto` resolves to the verified venue only.
    blank = {}
    if chosen["venue"] != "nim":
        blank.update(NIM_API_KEY="", NVIDIA_API_KEY="")
    if chosen["venue"] != "ollama":
        blank.update(OLLAMA_BASE_URL="", OLLAMA_MODEL="")
    if chosen["venue"] != "openrouter":
        blank.update(OPENROUTER_API_KEY="")
    else:
        blank.update(OPENROUTER_MODEL=PINNED_MODELS["openrouter"])
    if chosen["venue"] != "groq":
        blank.update(GROQ_API_KEY="")
    else:
        blank.update(GROQ_MODEL=PINNED_MODELS["groq"])
    blank["PROBE_CALLS"] = str(probe_calls)
    gh_env(blank)
    print(json.dumps(choice, indent=2))
    return 0


def cmd_account() -> int:
    audit = [json.loads(l) for l in open(OUT_DIR / "audit.jsonl", encoding="utf-8")
             if l.strip()] if (OUT_DIR / "audit.jsonl").exists() else []
    results = json.load(open(OUT_DIR / "results.json", encoding="utf-8")) \
        if (OUT_DIR / "results.json").exists() else []
    choice = json.load(open(OUT_DIR / "choice.json", encoding="utf-8")) \
        if (OUT_DIR / "choice.json").exists() else {}

    venue = choice.get("venue", "?")
    rate = LIST_RATES.get(venue)
    per_img = {}
    ok_events = [e for e in audit if e.get("event") == "venue_ok"]
    total_in = sum(int((e.get("usage") or {}).get("prompt_tokens") or 0) for e in ok_events)
    total_out = sum(int((e.get("usage") or {}).get("completion_tokens") or 0) for e in ok_events)
    usage_events = sum(1 for e in ok_events if (e.get("usage") or {}).get("prompt_tokens"))
    for e in ok_events:
        d = per_img.setdefault(e.get("record", "?"),
                               {"calls": 0, "latency_ms": [], "in_tok": 0, "out_tok": 0})
        d["calls"] += 1
        d["latency_ms"].append(int(e.get("latency_ms") or 0))
        d["in_tok"] += int((e.get("usage") or {}).get("prompt_tokens") or 0)
        d["out_tok"] += int((e.get("usage") or {}).get("completion_tokens") or 0)
    lats = sorted(sum((d["latency_ms"] for d in per_img.values()), []))

    def pct(p):
        return lats[min(len(lats) - 1, int(p * len(lats)))] if lats else 0

    if rate:
        cost_usd = (total_in * rate["in_mtok"] + total_out * rate["out_mtok"]) / 1e6
        cost_note = f"actual usage tokens x list rate ({rate['source']})"
    else:
        cost_usd = None
        cost_note = "no confirmed list rate for chosen venue — tokens reported, $ estimated in live doc"
    n = len(results) or len(per_img) or 1
    target = 13591
    per_img_cost = (cost_usd / n) if cost_usd is not None else None
    mean_lat = (sum(lats) / len(lats)) if lats else 0
    summary = {
        "venue": venue, "model": choice.get("model"),
        "n_results": len(results), "n_images": choice.get("n_images"),
        "probe_calls": choice.get("probe_calls", 0),
        "model_calls": len(ok_events),
        "calls_with_usage": usage_events,
        "tokens_in": total_in, "tokens_out": total_out,
        "cost_usd": round(cost_usd, 4) if cost_usd is not None else None,
        "cost_note": cost_note,
        "latency_ms": {"n": len(lats), "mean": round(mean_lat, 1),
                       "median": pct(0.5), "p90": pct(0.9),
                       "total": sum(lats)},
        "extrapolation_vs_13591": {
            "calls_per_image": round(len(ok_events) / n, 3),
            "projected_calls": int(round(len(ok_events) / n * target)),
            "projected_cost_usd": round(per_img_cost * target, 2)
            if per_img_cost is not None else None,
            "projected_serial_hours": round(mean_lat / 1000 * target / 3600, 1),
            "math": f"calls: ({len(ok_events)}/{n})x{target}; "
                    + (f"cost: (${per_img_cost:.5f}/img)x{target}; " if per_img_cost
                       else "cost: unconfirmed-rate; ") +
                    f"time serial: ({mean_lat:.0f}ms/img)x{target}/3600s",
        },
        "rate_limit_hits": sum(1 for e in audit if "429" in str(e.get("error", ""))
                               or e.get("event") == "run_abort_429"),
        "separator_mix_flags": sum(1 for e in audit
                                   if "separator_mix" in " ".join(e.get("issues", []))),
        "redo_ok_events": sum(1 for e in audit if e.get("event") == "extract+verify"
                              and int(e.get("attempt", 0)) > 0 and e.get("ok")),
        "per_image": {k: {"calls": v["calls"],
                          "latency_ms_total": sum(v["latency_ms"]),
                          "in_tok": v["in_tok"], "out_tok": v["out_tok"]}
                      for k, v in per_img.items()},
    }
    json.dump(summary, open(OUT_DIR / "cost_latency.json", "w", encoding="utf-8"), indent=2)
    print(json.dumps({k: v for k, v in summary.items() if k != "per_image"}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit({"preflight": cmd_preflight, "account": cmd_account}[sys.argv[1]]())
