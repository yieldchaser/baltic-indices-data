# Shipping

> *"I am a Man of Fortune, and I must seek my Fortune."*  
> — Henry Avery, 1694

A fully automated, zero-infrastructure shipping freight intelligence platform. Tracks Baltic freight indices, shipping ETF holdings, and a proprietary dry bulk composite — surfaced through a multi-tab analytical dashboard built entirely in a single `index.html` file.

**No server. No build step. No cost.**

---

## Live Dashboard

Served directly from this repository via GitHub Pages.  
Open `index.html` in any browser, or visit the GitHub Pages URL.

---

## How It Works (Self-Sustaining)

The system runs entirely on its own via two GitHub Actions workflows:

| Workflow | Schedule | What it does |
|---|---|---|
| `daily_update.yml` | **2 PM + 7 PM UTC daily** | Scrapes all 6 Baltic indices from stockq.org, deduplicates by date, appends new rows, commits `*_historical.csv` |
| `etf_holdings_update.yml` | **2 PM UTC Mon–Fri** | Downloads the master Amplify ETF holdings CSV, extracts BDRY and BWET, sorts by vessel class → contract month, commits `*_holdings.csv` |

Both workflows are **idempotent** — safe to re-run at any time. Duplicate rows are deduplicated by date before writing. Both workflows pull the latest remote state before running to prevent push conflicts.

The dashboard itself fetches everything client-side at page load — no backend, no API keys, no secrets required by the browser.

---

## What This Tracks

### Freight Indices — 7 series, daily since Dec 2007

| File | Index | Code | Vessel / Cargo |
|---|---|---|---|
| `bdiy_historical.csv` | Baltic Dry Index | BDI | Headline dry bulk composite |
| `cape_historical.csv` | Baltic Capesize Index | BCI | 180,000 DWT — iron ore, coal |
| `panama_historical.csv` | Baltic Panamax Index | BPI | 82,000 DWT — grain, coal |
| `suprama_historical.csv` | Baltic Supramax Index | BSI | 58,000 DWT — minor bulk |
| `handysize_historical.csv` | Baltic Handysize Index | BHSI | 28,000 DWT — minor bulk |
| `cleantanker_historical.csv` | Baltic Clean Tanker Index | BCTI | Refined products |
| `dirtytanker_historical.csv` | Baltic Dirty Tanker Index | BDTI | Crude oil |

CSV schema: `Date (DD-MM-YYYY), Index, % Change`

### BDRY Spot Composite — Computed client-side

Replicates the **Solactive Breakwave Dry Freight Futures Index** methodology using daily spot values:

```
BDRY_Spot(t) = 0.50 × BCI(t) + 0.40 × BPI(t) + 0.10 × BSI(t)
```

Available from October 2008 (~4,200 data points). Computed in the browser on every page load from the three existing CSVs — no extra file. Selectable across all tabs. Useful for comparing against the BDRY ETF market price to monitor premium/discount to spot.

### ETF Holdings — updated each market day

| File | ETF | What it holds |
|---|---|---|
| `bdry_holdings.csv` | Breakwave Dry Bulk Shipping ETF (BDRY) | Capesize 5TC, Panamax 5TC, Supramax 58 FFA futures — front 5 months |
| `bwet_holdings.csv` | Breakwave Tanker Shipping ETF (BWET) | TD3C (MEG→China 270kt VLCC) and TD20 (WAF→Continent 130kt Suezmax) FFA futures |

CSV schema: `Name, Ticker, CUSIP, Lots, Price, Market_Value, Weightings`

BDRY index weights: **50% Capesize, 40% Panamax, 10% Supramax** (Solactive ISIN DE000SLA4BY3).  
BWET index weights: **90% TD3C, 10% TD20** (Solactive ISIN DE000SL0HLG3, Excess Return).

---

## Repository Structure

```
Shipping/
│
├── index.html                          # Full dashboard — self-contained, CDN-only deps
│
├── bdiy_historical.csv                 # Baltic Dry Index history (from Dec 2007)
├── cape_historical.csv                 # Capesize (from Oct 2008)
├── panama_historical.csv               # Panamax (from Oct 2008)
├── suprama_historical.csv              # Supramax (from Oct 2008)
├── cleantanker_historical.csv          # Clean Tanker (from Jan 2008)
├── dirtytanker_historical.csv          # Dirty Tanker (from Dec 2007)
│
├── bdry_holdings.csv                   # BDRY FFA curve holdings (updated daily)
├── bwet_holdings.csv                   # BWET FFA curve holdings (updated daily)
├── BDRY_Daily.csv                      # BDRY daily premium/discount
├── BWET_Daily.csv                      # BWET daily premium/discount
├── bdryff_history.csv                  # Solactive BDRY freight futures index history
├── bwetff_history.csv                  # Solactive BWET freight futures index history
│
├── sgx_cape_futures.csv                # SGX Capesize FFA futures curve
├── sgx_panamax_futures.csv             # SGX Panamax FFA futures curve
├── sgx_supramax_futures.csv            # SGX Supramax FFA futures curve
├── sgx_handysize_futures.csv           # SGX Handysize FFA futures curve
│
├── scripts/
│   ├── update_indices.py               # Baltic index scraper (all 6 indices + SGX FFA)
│   └── update_etf_holdings.py          # ETF holdings scraper (BDRY & BWET)
│
├── assets/
│   ├── BDRY_Export-Map-1024x548.webp   # Dry bulk trade route map (ETF tab)
│   ├── BWET_Tanker-Map-1-1024x585.webp # Crude tanker route map (ETF tab)
│   ├── Picture1.png                    # Reference charts
│   ├── Picture2.png
│   ├── Picture3.png
│   └── Picture4.png
│
├── docs/
│   └── Shipping_Main.xlsm              # Offline Excel workbook (same CSV data)
│
└── .github/workflows/
    ├── daily_update.yml                # Cron: 10:30 AM + 2/7/10 PM UTC daily
    └── etf_holdings_update.yml         # Cron: 2 PM UTC Mon–Fri
```

---

## Dashboard Tabs

Built on **Chart.js 4.4.0** and **PapaParse 5.4.1**. All data fetched client-side — no backend. The global **Index:** dropdown in the header switches the active product across all tabs instantly.

**12 products available:** BDI · Capesize · Panamax · Supramax · Handysize · Clean Tanker · Dirty Tanker · BDRY Spot Composite · BDRYFF · BWETFF · BDRY Stock Price · BWET Stock Price

---

### 📊 Dashboard

Main overview for the selected index.

- **Hero KPI + signal badge** — algorithmic signal based on percentile and Z-score:

  | Signal | Condition |
  |---|---|
  | ⛔ SELL | 5Y pctl > 80% |
  | 💎 GOLDEN DIP | 5Y pctl < 20%, Z < −0.5, all-time pctl > 40% |
  | 🔥 CATCHING KNIFE | 5Y pctl < 10%, Z < −0.6 |
  | ⚠️ VALUE TRAP | 5Y pctl < 30%, all-time pctl < 30% |
  | 🔹 ACCUMULATE | 5Y pctl < 40% |
  | ⏳ WAIT | all other |

- **6 stat cards:** All-Time Pctl · 10Y Pctl · 5Y Pctl · Z-Score · 52-Week Drawdown · 20D RoC
- **Historical Context Strip:** 5Y avg, current vs 5Y avg %, current vs 10Y avg %
- **Current Year vs Historical Overlay chart** — current year vs user-selected prior years
- **Drawdown from 52-Week High** — last 3 years
- **Recent Daily Changes table** — last 10 sessions: day Δ, day Δ%, 5D change %
- **Yearly Performance table** *(collapsible)* — annual avg, YoY %, min, max, range % (range = (max−min)/avg, handles years where min ≤ 0)
- **Index Correlation Matrix** — Pearson correlation for all 7 products, switchable All Time / 5Y / 1Y

---

### 📅 Yearly

- **Historical Price chart** — full history with rolling average toggle (5Y / 10Y / All-Time). Dual-handle range slider.
- **Z-Score (Rolling 252-Day)** — all 7 products, selected product thicker. Range slider defaults to last 3 years.
- **Historical Z-Score (All Time from 2008)** — full-history view.
- **Multi-Year Rates** — annual averages by product, all years.
- **Current Year Monthly Bar** — MoM colour coding.
- **Rates — All Products Multi-Year Overlay** — last 4 years by trading day.
- **Drawdown % (52-Week Rolling, Last 5 Years)**

---

### 📆 Monthly

- Monthly bar chart (last 12 months, MoM colour)
- Monthly trend area chart (last 3 years)
- Monthly area comparison (current vs prior year)
- Monthly data grid — last 5 years × 12 months heatmap with 5Y avg and MoM % rows

---

### 📊 Quarterly

- **Win Rate KPI cards** — historical probability each quarter beats the prior quarter
- **Quarterly Heatmap** — all years × Q1–Q4, absolute or QoQ % switchable
- **Spaghetti Chart** — Q1/Q2/Q3/Q4 across all years as 4 coloured lines
- **Area comparisons** — current vs prior year, current year vs 5Y seasonal average
- **Quarterly Data Grid** — last 8 years with full-year avg and YoY %

---

### 🌡️ Heatmaps

- **Monthly Heatmap** — year × month, absolute value or MoM % toggle. Column-normalised so Jan values compare cleanly across all years.

---

### 📈 Indices

All 6 base indices as individual chart cards:
- Current value, day change %
- Dual-handle date range slider — zoom to any window, defaults to last 5 years
- Stats strip: All-Time High · All-Time Low · Current vs ATH · YTD %

---

### 🏦 ETFs

#### BDRY & BWET Card Layout (identical structure for both)

Each card contains:
1. **Live price + day change** — Yahoo Finance v8 API via CORS proxy; NAV populated from the same response (`meta.navPrice`)
2. **Metrics row 1:** Total Futures · Collateral Cash · Futures/AUM %
3. **Metrics row 2:** NAV · Expense Ratio (3.50%) · Exposure Ratio
4. **Metrics row 3:** 52W High — Low · 52W Position (%) · From Last Trough (%)
5. **Holdings table** — FFA contracts sorted by vessel class → expiry month (nearest first). Scrollable, fixed-height.
6. **Futures Allocation donut** — normalised to 100% of futures notional (cash excluded)
7. **Trade route map** — with inline legend (exporting nations / importing nations / routes / BWET focused routes)
8. **Fundamentals / Data Sources** — sector-specific data links:
   - BDRY: China Steel & Bulk Demand + Export Flow Indicators (macromicro.me)
   - BWET: Crude & Product Demand (Trading Economics, EIA) + Key Trade Routes (TradingView: TD3C / TD20)
9. **Market Outlook & Research Sources** — categorized market intelligence:
   - **Research & Insights**: Breakwave Advisors (Research & Insights), BIMCO
   - **Weekly Market Reports**: Fearnleys Weekly Pulse, Baltic Exchange Weekly Roundup
   - **Charter Rate Estimates**: Hellenic Shipping News (Weekly dry/tanker charter estimates)
   - **ETF Data**: Official Amplify and Solactive pages

#### BDRY & BWET Liquidity Tracker *(below the ETF cards)*

Position-sizing model applied to BDRY's full daily history (~1,994 days), fetched live from Yahoo Finance:

| Column | Formula |
|---|---|
| Dollar Value Traded | `Close × Volume` |
| Tier % | Vol < 50K → 2% · < 100K → 3.5% · < 500K → 5% · ≥ 500K → 6.5% |
| Possible Shares | `floor(Volume × Tier%)` |
| Safe Liquidity | `Possible Shares × Close` |

- **KPI strip** — current session values for all fields + **Total Safe Liquidity (1M)** (rolling 21-day sum)
- **Safe Liquidity chart** — historical $ tradeable per day
- **Volume chart** — daily bars coloured by tier, with 50K / 100K / 500K threshold lines
- **Rolling Averages chart** — 7 windows (10D / 20D / 1M / 3M / 6M / 12M / 24M)
- **Full data table** — all rows newest-first, scrollable, CSV export, window toggle (1Y / 3Y / All)

---

### 🎯 Signals

Five analytical charts:

| Chart | Description |
|---|---|
| **Bollinger Bands (20D, 2σ)** | Price + upper/SMA/lower bands. Dual-handle date range slider. |
| **Cape / Panamax Ratio** | Ratio time series + all-time mean + rolling 252D percentile. Window: 3Y / 5Y / All |
| **Rate-of-Change Heatmap** | 7 products × 6 timeframes (5D / 10D / 20D / 60D / 90D / 1Y) divergent colour scale |
| **Seasonal Decomposition** | Historical avg intra-year pattern ± 1σ with current year overlaid |
| **FFA Term Structure** | Forward curves from live holdings CSVs. Slope labels: 📉 Backwardation / 📈 Contango / ➡️ Flat |
| **Futures vs Spot Premium** | Basis tracking between front-month futures and spot index. Dual-handle range slider. |
| **BDI Contribution** | Decomposition of BDI daily change by vessel class (Cape/Pana/Supra). Range slider. |
| **Lead–Lag Correlation** | Log-return cross-correlation (-30 to +30 days) to detect leads. Categorized: Financial Speed (ETF vs Index), Vessel Rotation (Cape vs Panamax), and Basis Discovery. |

---

## Statistics Reference

| Metric | Calculation |
|---|---|
| **Percentile Rank** | Fraction of historical values ≤ current within lookback window |
| **Z-Score (Dashboard)** | `(current − mean of same calendar trading day across all prior years) / stddev` |
| **Z-Score (Rolling 252D)** | `(current − trailing 252D mean) / trailing 252D stddev` |
| **52-Week Drawdown** | `(current − max over trailing 365 calendar days) / max` |
| **Rate of Change (20D)** | `(current − value 20 trading days ago) / value 20 trading days ago × 100` |
| **Bollinger Bands** | `SMA(20) ± 2 × population stddev(20)` |
| **Cape/Panamax Percentile** | Percentile rank of ratio vs trailing 252D of ratio values |
| **Seasonal Avg** | Mean of `value[trading_day_N]` across all historical years except current |
| **FFA Slope** | `(back_month − front_month) / front_month × 100` |
| **BDRY Spot** | `0.50 × BCI + 0.40 × BPI + 0.10 × BSI` |
| **Lead–Lag Corr** | `corr(log_returns_A_t, log_returns_B_t+lag)` for lag ‐30 to +30 days |
| **Range %** | `(yearly_max − yearly_min) / yearly_avg × 100` *(uses avg denominator — handles years where min ≤ 0 correctly)* |
| **Leverage / Exposure** | `(Total Exposure / Collateral Cash) − 1` expressed as % |
| **52W Position** | Relative position within the trailing 252-day price range (0% = low, 100% = high) |
| **From Last Trough** | Percentage increase from the lowest price reached in the last 365 calendar days |
| **Safe Liquidity** | `floor(Volume × tier%) × Close` |
| **Total Safe Liquidity (1M)** | Sum of `Safe Liquidity` over the trailing 21 trading days (approx. one month) |

---

## Automation Details

### `scripts/update_indices.py`

- Scrapes `en.stockq.org` for all 6 Baltic indices
- `raise_for_status()` on every HTTP response — fails loudly on 4xx/5xx
- Sanity-checks scraped values (skips zero or negative index readings)
- Deduplicates by parsed date (chronological sort, not lexicographic)
- Idempotent — re-running never corrupts existing data

### `scripts/update_etf_holdings.py`

- Downloads the master Amplify ETF holdings CSV from `amplifyetfs.com`
- Filters to BDRY and BWET
- Sorts by vessel class → contract month (nearest expiry first)
- Index-reset before sort to prevent merge misalignment on filtered DataFrames
- Validates `Market_Value` as numeric before any arithmetic
- Idempotent — overwrites the output file each run

### GitHub Actions Schedules

| Workflow | Cron | Rationale |
|---|---|---|
| `daily_update.yml` | `0 14,19 * * *` | Runs at 2 PM UTC (2 hrs after BDI ~12:00 UTC publish) and 7 PM UTC |
| `etf_holdings_update.yml` | `0 14 * * 1-5` | Runs at 2 PM UTC Mon–Fri after Amplify publishes updated holdings |

Both workflows: pull latest before running (prevents push conflicts on concurrent runs), use explicit file paths for `git add` (prevents staging unintended files), include `GITHUB_TOKEN` in checkout for write access.

---

## Running Scrapers Locally

```bash
pip install requests beautifulsoup4 pandas lxml

python scripts/update_indices.py        # update all 6 Baltic indices + SGX FFA futures
python scripts/update_etf_holdings.py   # update BDRY and BWET holdings
```

Both scripts are safe to re-run at any time.

---

## Dependencies

### Dashboard (browser, CDN-loaded)

| Library | Version | Purpose |
|---|---|---|
| [Chart.js](https://www.chartjs.org/) | 4.4.0 | All charts |
| [PapaParse](https://www.papaparse.com/) | 5.4.1 | CSV parsing |
| [allorigins.win](https://allorigins.win/) | — | CORS proxy for Yahoo Finance (live prices + BDRY liquidity) |

### Scrapers (GitHub Actions only)

```
requests · beautifulsoup4 · pandas · lxml
```

---

## Data Sources

| Data | Source | Update Frequency |
|---|---|---|
| Baltic freight indices (BDI, BCI, BPI, BSI, BCTI, BDTI) | [stockq.org](https://en.stockq.org) | 2× daily via GitHub Actions |
| BDRY / BWET FFA holdings | [amplifyetfs.com](https://amplifyetfs.com) | Daily Mon–Fri via GitHub Actions |
| BDRY / BWET live price + NAV | Yahoo Finance v8 API (via CORS proxy) | On ETF tab open |
| BDRY liquidity history | Yahoo Finance v8 API (via CORS proxy) | On ETF tab open (`range=10y`) |

---

## Notes

- CSV dates are in `DD-MM-YYYY` format
- BDI history starts **December 2007** — tail end of the commodity supercycle peak (~10,000+)
- BDRY Spot Composite starts **October 2008** (earliest date all three dry bulk components overlap)
- Tanker index histories: BCTI from Jan 2008, BDTI from Dec 2007
- The FFA term structure chart is only as fresh as the last `bdry_holdings.csv` / `bwet_holdings.csv` commit — check the commit timestamp to confirm
- `Shipping_Main.xlsm` is an offline Excel workbook for ad-hoc analysis consuming the same CSV data
- Capesize went briefly negative in 2020; the yearly Range % uses `(max−min)/avg` rather than `(max−min)/min` to avoid nonsensical outputs in such years
