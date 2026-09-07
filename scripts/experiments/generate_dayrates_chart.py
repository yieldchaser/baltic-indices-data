import json
import os

with open('scratch_osv_chart_data.json', 'r', encoding='utf-8') as f:
    chart_data = json.load(f)

artifact_dir = r"C:\Users\Dell\.gemini\antigravity\brain\0c979471-3634-4d8e-9c0d-ac7c89e1e50e"
artifact_path = os.path.join(artifact_dir, "osv_dayrates_chart.html")

data_json = json.dumps(chart_data)

html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Seabrokers Offshore Dayrate Tracker</title>
  <script src="https://www.gstatic.com/antigravity/web/dev/tailwindcss.min.js"></script>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    }}
  </style>
</head>
<body class="bg-transparent text-[var(--foreground)] antialiased p-2 sm:p-4">
  <div class="max-w-6xl mx-auto bg-[var(--card)] text-[var(--foreground)] border border-[var(--border)] rounded-2xl p-4 sm:p-6 shadow-sm space-y-5">
    
    <!-- Header -->
    <div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 border-b border-[var(--border)] pb-4">
      <div>
        <div class="flex items-center gap-2">
          <span class="px-2.5 py-0.5 rounded-full text-xs font-semibold bg-sky-500/10 text-sky-500 border border-sky-500/20">Seabrokers Seabreeze Intelligence</span>
          <span class="text-xs text-[var(--muted-foreground)]">North Sea Spot Charter Market</span>
        </div>
        <h1 class="text-xl sm:text-2xl font-bold tracking-tight text-[var(--foreground)] mt-1">Offshore Vessel Dayrate Tracker (2018–2026)</h1>
        <p class="text-xs sm:text-sm text-[var(--muted-foreground)]">Historical spot dayrates (£/day) and fleet utilisation across AHTS & PSV classes</p>
      </div>

      <!-- Log / Linear Scale Toggle -->
      <div class="flex items-center gap-2 self-start sm:self-auto">
        <span class="text-xs text-[var(--muted-foreground)] font-medium">Scale:</span>
        <div class="inline-flex p-0.5 rounded-lg border border-[var(--border)] bg-[var(--background)]">
          <button id="scaleLinear" class="px-2.5 py-1 text-xs font-semibold rounded-md bg-[var(--card)] shadow-xs transition-all">Linear</button>
          <button id="scaleLog" class="px-2.5 py-1 text-xs font-medium rounded-md text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-all">Log</button>
        </div>
      </div>
    </div>

    <!-- KPI HUD Cards -->
    <div class="grid grid-cols-2 lg:grid-cols-4 gap-3">
      <!-- Large AHTS -->
      <div class="p-3.5 rounded-xl border border-sky-500/20 bg-sky-500/5 hover:border-sky-500/40 transition-colors">
        <div class="flex items-center justify-between text-xs text-[var(--muted-foreground)]">
          <span class="font-semibold text-sky-600 dark:text-sky-400 flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full bg-sky-500 inline-block"></span>
            Large AHTS (&gt;22k BHP)
          </span>
          <span class="text-[10px] uppercase font-bold text-emerald-600 dark:text-emerald-400">+499% YoY</span>
        </div>
        <div class="mt-2 flex items-baseline justify-between">
          <span class="text-xl sm:text-2xl font-black tracking-tight text-[var(--foreground)]">£96,015</span>
          <span class="text-xs text-[var(--muted-foreground)] font-mono">/ day</span>
        </div>
        <div class="mt-2 pt-2 border-t border-sky-500/10 flex justify-between text-[11px] text-[var(--muted-foreground)]">
          <span>Utilisation: <strong class="text-[var(--foreground)] font-mono">73%</strong></span>
          <span>Max: <strong class="text-[var(--foreground)] font-mono">£195k</strong></span>
        </div>
      </div>

      <!-- Medium AHTS -->
      <div class="p-3.5 rounded-xl border border-purple-500/20 bg-purple-500/5 hover:border-purple-500/40 transition-colors">
        <div class="flex items-center justify-between text-xs text-[var(--muted-foreground)]">
          <span class="font-semibold text-purple-600 dark:text-purple-400 flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full bg-purple-500 inline-block"></span>
            Med AHTS (&lt;22k BHP)
          </span>
          <span class="text-[10px] uppercase font-bold text-emerald-600 dark:text-emerald-400">+292% YoY</span>
        </div>
        <div class="mt-2 flex items-baseline justify-between">
          <span class="text-xl sm:text-2xl font-black tracking-tight text-[var(--foreground)]">£62,332</span>
          <span class="text-xs text-[var(--muted-foreground)] font-mono">/ day</span>
        </div>
        <div class="mt-2 pt-2 border-t border-purple-500/10 flex justify-between text-[11px] text-[var(--muted-foreground)]">
          <span>Utilisation: <strong class="text-[var(--foreground)] font-mono">46%</strong></span>
          <span>Max: <strong class="text-[var(--foreground)] font-mono">£117k</strong></span>
        </div>
      </div>

      <!-- Large PSV -->
      <div class="p-3.5 rounded-xl border border-emerald-500/20 bg-emerald-500/5 hover:border-emerald-500/40 transition-colors">
        <div class="flex items-center justify-between text-xs text-[var(--muted-foreground)]">
          <span class="font-semibold text-emerald-600 dark:text-emerald-400 flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full bg-emerald-500 inline-block"></span>
            Large PSV (&gt;900m²)
          </span>
          <span class="text-[10px] uppercase font-bold text-emerald-600 dark:text-emerald-400">+262% YoY</span>
        </div>
        <div class="mt-2 flex items-baseline justify-between">
          <span class="text-xl sm:text-2xl font-black tracking-tight text-[var(--foreground)]">£21,445</span>
          <span class="text-xs text-[var(--muted-foreground)] font-mono">/ day</span>
        </div>
        <div class="mt-2 pt-2 border-t border-emerald-500/10 flex justify-between text-[11px] text-[var(--muted-foreground)]">
          <span>Utilisation: <strong class="text-[var(--foreground)] font-mono">79%</strong></span>
          <span>Max: <strong class="text-[var(--foreground)] font-mono">£31k</strong></span>
        </div>
      </div>

      <!-- Medium PSV -->
      <div class="p-3.5 rounded-xl border border-amber-500/20 bg-amber-500/5 hover:border-amber-500/40 transition-colors">
        <div class="flex items-center justify-between text-xs text-[var(--muted-foreground)]">
          <span class="font-semibold text-amber-600 dark:text-amber-400 flex items-center gap-1.5">
            <span class="w-2 h-2 rounded-full bg-amber-500 inline-block"></span>
            Med PSV (&lt;900m²)
          </span>
          <span class="text-[10px] uppercase font-bold text-emerald-600 dark:text-emerald-400">+280% YoY</span>
        </div>
        <div class="mt-2 flex items-baseline justify-between">
          <span class="text-xl sm:text-2xl font-black tracking-tight text-[var(--foreground)]">£18,000</span>
          <span class="text-xs text-[var(--muted-foreground)] font-mono">/ day</span>
        </div>
        <div class="mt-2 pt-2 border-t border-amber-500/10 flex justify-between text-[11px] text-[var(--muted-foreground)]">
          <span>Utilisation: <strong class="text-[var(--foreground)] font-mono">82%</strong></span>
          <span>Max: <strong class="text-[var(--foreground)] font-mono">£20k</strong></span>
        </div>
      </div>
    </div>

    <!-- Controls Row: Timeframe & Series Visibility -->
    <div class="flex flex-wrap items-center justify-between gap-3 pt-1">
      <!-- Timeframe Buttons -->
      <div class="flex items-center gap-1.5">
        <span class="text-xs text-[var(--muted-foreground)] font-medium mr-1">Horizon:</span>
        <button data-range="all" class="range-btn px-3 py-1 text-xs font-semibold rounded-lg bg-[var(--foreground)] text-[var(--background)] transition-all">All (8Y)</button>
        <button data-range="5y" class="range-btn px-3 py-1 text-xs font-medium rounded-lg border border-[var(--border)] text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-all">5Y</button>
        <button data-range="3y" class="range-btn px-3 py-1 text-xs font-medium rounded-lg border border-[var(--border)] text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-all">3Y</button>
        <button data-range="1y" class="range-btn px-3 py-1 text-xs font-medium rounded-lg border border-[var(--border)] text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-all">1Y</button>
      </div>

      <!-- Series Checkboxes -->
      <div class="flex flex-wrap items-center gap-3 text-xs">
        <label class="inline-flex items-center gap-1.5 cursor-pointer select-none">
          <input type="checkbox" id="chkLargeAhts" checked class="rounded accent-sky-500">
          <span class="text-sky-600 dark:text-sky-400 font-medium">Large AHTS</span>
        </label>
        <label class="inline-flex items-center gap-1.5 cursor-pointer select-none">
          <input type="checkbox" id="chkMedAhts" checked class="rounded accent-purple-500">
          <span class="text-purple-600 dark:text-purple-400 font-medium">Med AHTS</span>
        </label>
        <label class="inline-flex items-center gap-1.5 cursor-pointer select-none">
          <input type="checkbox" id="chkLargePsv" checked class="rounded accent-emerald-500">
          <span class="text-emerald-600 dark:text-emerald-400 font-medium">Large PSV</span>
        </label>
        <label class="inline-flex items-center gap-1.5 cursor-pointer select-none">
          <input type="checkbox" id="chkMedPsv" checked class="rounded accent-amber-500">
          <span class="text-amber-600 dark:text-amber-400 font-medium">Med PSV</span>
        </label>
      </div>
    </div>

    <!-- Chart Canvas Container -->
    <div class="relative w-full rounded-xl border border-[var(--border)] bg-[var(--background)]/50 p-2 sm:p-4 overflow-hidden">
      <div style="height: 380px; position: relative;">
        <canvas id="osvChart" class="w-full h-full"></canvas>
        <div id="chartTooltip" class="absolute hidden pointer-events-none bg-[var(--card)]/95 backdrop-blur-md border border-[var(--border)] text-[var(--foreground)] rounded-xl p-3 shadow-xl text-xs z-20 min-w-[220px]"></div>
      </div>
    </div>

    <!-- Market Cycle Context Footer -->
    <div class="grid grid-cols-1 sm:grid-cols-4 gap-2.5 pt-2 border-t border-[var(--border)] text-xs text-[var(--muted-foreground)]">
      <div class="p-2.5 rounded-lg bg-[var(--background)] border border-[var(--border)]">
        <span class="font-bold text-[var(--foreground)] block mb-1">2018–2020: Deep Trough</span>
        <span>Extreme vessel layup, low utilization (&lt;50%), dayrates hovering at OPEX breakeven (£3k–£10k/day).</span>
      </div>
      <div class="p-2.5 rounded-lg bg-[var(--background)] border border-[var(--border)]">
        <span class="font-bold text-[var(--foreground)] block mb-1">2020–2021: COVID Shock</span>
        <span>Discretionary drilling paused, sudden drop in North Sea drilling campaign mobilizations.</span>
      </div>
      <div class="p-2.5 rounded-lg bg-[var(--background)] border border-[var(--border)]">
        <span class="font-bold text-[var(--foreground)] block mb-1">2022–2024: Energy Security</span>
        <span>Reactivation of stacked floaters, Norway/UK exploration restart, dayrates jumping to £30k–£70k.</span>
      </div>
      <div class="p-2.5 rounded-lg bg-[var(--background)] border border-[var(--border)]">
        <span class="font-bold text-[var(--foreground)] block mb-1">2025–2026: Supply Crunch</span>
        <span>Fleet attrition, auction sales to overseas basins, and offshore wind installation pushing AHTS &gt;£150k/day.</span>
      </div>
    </div>

  </div>

  <script>
    const RAW_DATA = {data_json};

    let currentRange = 'all';
    let isLogScale = false;
    let visibleSeries = {{
      ahts_large: true,
      ahts_med: true,
      psv_large: true,
      psv_med: true
    }};

    const colors = {{
      ahts_large: '#0284c7', // cyan
      ahts_med: '#9333ea',   // purple
      psv_large: '#059669',  // emerald
      psv_med: '#d97706'     // amber
    }};

    const canvas = document.getElementById('osvChart');
    const ctx = canvas.getContext('2d');
    const tooltip = document.getElementById('chartTooltip');

    function getFilteredData() {{
      if (currentRange === 'all') return RAW_DATA;
      const latestDate = new Date(RAW_DATA[RAW_DATA.length - 1].date);
      let cutoffYear = latestDate.getFullYear();
      if (currentRange === '1y') cutoffYear -= 1;
      else if (currentRange === '3y') cutoffYear -= 3;
      else if (currentRange === '5y') cutoffYear -= 5;
      const cutoff = new Date(cutoffYear, latestDate.getMonth(), 1);
      return RAW_DATA.filter(d => new Date(d.date) >= cutoff);
    }}

    function resizeCanvas() {{
      const rect = canvas.parentElement.getBoundingClientRect();
      const dpr = window.devicePixelRatio || 1;
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.resetTransform();
      ctx.scale(dpr, dpr);
      renderChart();
    }}

    window.addEventListener('resize', resizeCanvas);

    function renderChart() {{
      const data = getFilteredData();
      if (!data.length) return;

      const rect = canvas.parentElement.getBoundingClientRect();
      const W = rect.width;
      const H = rect.height;

      ctx.clearRect(0, 0, W, H);

      const padLeft = 65;
      const padRight = 25;
      const padTop = 25;
      const padBottom = 35;

      const plotW = W - padLeft - padRight;
      const plotH = H - padTop - padBottom;

      let minVal = Infinity;
      let maxVal = -Infinity;

      data.forEach(d => {{
        if (visibleSeries.ahts_large && d.ahts_large) {{
          minVal = Math.min(minVal, d.ahts_large);
          maxVal = Math.max(maxVal, d.ahts_large);
        }}
        if (visibleSeries.ahts_med && d.ahts_med) {{
          minVal = Math.min(minVal, d.ahts_med);
          maxVal = Math.max(maxVal, d.ahts_med);
        }}
        if (visibleSeries.psv_large && d.psv_large) {{
          minVal = Math.min(minVal, d.psv_large);
          maxVal = Math.max(maxVal, d.psv_large);
        }}
        if (visibleSeries.psv_med && d.psv_med) {{
          minVal = Math.min(minVal, d.psv_med);
          maxVal = Math.max(maxVal, d.psv_med);
        }}
      }});

      if (minVal === Infinity) {{ minVal = 1000; maxVal = 200000; }}
      if (!isLogScale) {{
        minVal = 0;
        maxVal = Math.ceil(maxVal / 20000) * 20000;
      }} else {{
        minVal = Math.max(1000, minVal * 0.8);
        maxVal = maxVal * 1.2;
      }}

      function getY(val) {{
        if (!val || val <= 0) return plotH + padTop;
        if (!isLogScale) {{
          const norm = (val - minVal) / (maxVal - minVal || 1);
          return padTop + plotH - norm * plotH;
        }} else {{
          const logMin = Math.log10(minVal);
          const logMax = Math.log10(maxVal);
          const norm = (Math.log10(val) - logMin) / (logMax - logMin || 1);
          return padTop + plotH - norm * plotH;
        }}
      }}

      function getX(idx) {{
        return padLeft + (idx / (data.length - 1 || 1)) * plotW;
      }}

      // Draw Grid Lines & Y-Labels
      ctx.lineWidth = 1;
      ctx.strokeStyle = 'rgba(150, 150, 150, 0.15)';
      ctx.fillStyle = 'rgba(150, 150, 150, 0.8)';
      ctx.font = '11px monospace';
      ctx.textAlign = 'right';

      const yTicks = isLogScale ? [2000, 5000, 10000, 25000, 50000, 100000, 200000] : [0, 40000, 80000, 120000, 160000, 200000];

      yTicks.forEach(tick => {{
        if (tick >= minVal && tick <= maxVal) {{
          const y = getY(tick);
          ctx.beginPath();
          ctx.moveTo(padLeft, y);
          ctx.lineTo(W - padRight, y);
          ctx.stroke();

          ctx.fillText('£' + (tick >= 1000 ? (tick/1000) + 'k' : tick), padLeft - 10, y + 4);
        }}
      }});

      // Draw X-Axis Dates
      ctx.textAlign = 'center';
      const step = Math.max(1, Math.floor(data.length / 6));
      for (let i = 0; i < data.length; i += step) {{
        const x = getX(i);
        const label = data[i].date.substring(0, 7);
        ctx.beginPath();
        ctx.moveTo(x, padTop + plotH);
        ctx.lineTo(x, padTop + plotH + 5);
        ctx.stroke();
        ctx.fillText(label, x, padTop + plotH + 18);
      }}

      // Draw Series Lines
      function drawSeries(key, strokeColor, lineWidth = 2.5) {{
        if (!visibleSeries[key]) return;

        ctx.lineWidth = lineWidth;
        ctx.strokeStyle = strokeColor;
        ctx.beginPath();
        let started = false;

        for (let i = 0; i < data.length; i++) {{
          const val = data[i][key];
          if (val !== null && val !== undefined) {{
            const x = getX(i);
            const y = getY(val);
            if (!started) {{
              ctx.moveTo(x, y);
              started = true;
            }} else {{
              ctx.lineTo(x, y);
            }}
          }}
        }}
        ctx.stroke();
      }}

      drawSeries('psv_med', colors.psv_med, 2);
      drawSeries('psv_large', colors.psv_large, 2.5);
      drawSeries('ahts_med', colors.ahts_med, 2);
      drawSeries('ahts_large', colors.ahts_large, 3);
    }}

    // Mouse Hover & Crosshair Tooltip
    canvas.addEventListener('mousemove', (e) => {{
      const data = getFilteredData();
      if (!data.length) return;

      const rect = canvas.getBoundingClientRect();
      const mouseX = e.clientX - rect.left;
      const mouseY = e.clientY - rect.top;

      const padLeft = 65;
      const padRight = 25;
      const plotW = rect.width - padLeft - padRight;

      if (mouseX < padLeft || mouseX > rect.width - padRight) {{
        tooltip.classList.add('hidden');
        renderChart();
        return;
      }}

      const ratio = (mouseX - padLeft) / plotW;
      const idx = Math.min(data.length - 1, Math.max(0, Math.round(ratio * (data.length - 1))));
      const item = data[idx];

      renderChart();

      // Vertical crosshair
      const x = padLeft + (idx / (data.length - 1 || 1)) * plotW;
      ctx.save();
      ctx.lineWidth = 1;
      ctx.setLineDash([4, 4]);
      ctx.strokeStyle = 'rgba(150, 150, 150, 0.4)';
      ctx.beginPath();
      ctx.moveTo(x, 25);
      ctx.lineTo(x, rect.height - 35);
      ctx.stroke();
      ctx.restore();

      // Tooltip HTML
      let ttHtml = `<div class="font-bold text-xs pb-1.5 border-b border-[var(--border)] mb-2">${{item.date}}</div>`;
      ttHtml += '<div class="space-y-1.5">';

      if (visibleSeries.ahts_large && item.ahts_large) {{
        ttHtml += `<div class="flex justify-between items-center text-sky-500 font-medium">
          <span>Large AHTS:</span>
          <span class="font-mono font-bold">£${{item.ahts_large.toLocaleString()}} <span class="text-[10px] text-[var(--muted-foreground)]">(${{item.ahts_large_yoy}})</span></span>
        </div>`;
      }}
      if (visibleSeries.ahts_med && item.ahts_med) {{
        ttHtml += `<div class="flex justify-between items-center text-purple-500 font-medium">
          <span>Med AHTS:</span>
          <span class="font-mono font-bold">£${{item.ahts_med.toLocaleString()}} <span class="text-[10px] text-[var(--muted-foreground)]">(${{item.ahts_med_yoy}})</span></span>
        </div>`;
      }}
      if (visibleSeries.psv_large && item.psv_large) {{
        ttHtml += `<div class="flex justify-between items-center text-emerald-500 font-medium">
          <span>Large PSV:</span>
          <span class="font-mono font-bold">£${{item.psv_large.toLocaleString()}} <span class="text-[10px] text-[var(--muted-foreground)]">(${{item.psv_large_yoy}})</span></span>
        </div>`;
      }}
      if (visibleSeries.psv_med && item.psv_med) {{
        ttHtml += `<div class="flex justify-between items-center text-amber-500 font-medium">
          <span>Med PSV:</span>
          <span class="font-mono font-bold">£${{item.psv_med.toLocaleString()}} <span class="text-[10px] text-[var(--muted-foreground)]">(${{item.psv_med_yoy}})</span></span>
        </div>`;
      }}

      if (item.large_psv_util || item.large_ahts_util) {{
        ttHtml += `<div class="pt-1.5 border-t border-[var(--border)] text-[10px] text-[var(--muted-foreground)] flex justify-between">
          <span>PSV Util: <strong class="text-[var(--foreground)]">${{item.large_psv_util || 'N/A'}}</strong></span>
          <span>AHTS Util: <strong class="text-[var(--foreground)]">${{item.large_ahts_util || 'N/A'}}</strong></span>
        </div>`;
      }}

      ttHtml += '</div>';

      tooltip.innerHTML = ttHtml;
      tooltip.classList.remove('hidden');

      let tipLeft = mouseX + 15;
      if (tipLeft + 230 > rect.width) {{
        tipLeft = mouseX - 240;
      }}
      tooltip.style.left = tipLeft + 'px';
      tooltip.style.top = Math.min(rect.height - 150, Math.max(20, mouseY - 40)) + 'px';
    }});

    canvas.addEventListener('mouseleave', () => {{
      tooltip.classList.add('hidden');
      renderChart();
    }});

    // Button Wiring
    document.querySelectorAll('.range-btn').forEach(btn => {{
      btn.addEventListener('click', () => {{
        document.querySelectorAll('.range-btn').forEach(b => {{
          b.className = 'range-btn px-3 py-1 text-xs font-medium rounded-lg border border-[var(--border)] text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-all';
        }});
        btn.className = 'range-btn px-3 py-1 text-xs font-semibold rounded-lg bg-[var(--foreground)] text-[var(--background)] transition-all';
        currentRange = btn.dataset.range;
        renderChart();
      }});
    }});

    document.getElementById('chkLargeAhts').addEventListener('change', (e) => {{ visibleSeries.ahts_large = e.target.checked; renderChart(); }});
    document.getElementById('chkMedAhts').addEventListener('change', (e) => {{ visibleSeries.ahts_med = e.target.checked; renderChart(); }});
    document.getElementById('chkLargePsv').addEventListener('change', (e) => {{ visibleSeries.psv_large = e.target.checked; renderChart(); }});
    document.getElementById('chkMedPsv').addEventListener('change', (e) => {{ visibleSeries.psv_med = e.target.checked; renderChart(); }});

    document.getElementById('scaleLinear').addEventListener('click', () => {{
      isLogScale = false;
      document.getElementById('scaleLinear').className = 'px-2.5 py-1 text-xs font-semibold rounded-md bg-[var(--card)] shadow-xs transition-all';
      document.getElementById('scaleLog').className = 'px-2.5 py-1 text-xs font-medium rounded-md text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-all';
      renderChart();
    }});

    document.getElementById('scaleLog').addEventListener('click', () => {{
      isLogScale = true;
      document.getElementById('scaleLog').className = 'px-2.5 py-1 text-xs font-semibold rounded-md bg-[var(--card)] shadow-xs transition-all';
      document.getElementById('scaleLinear').className = 'px-2.5 py-1 text-xs font-medium rounded-md text-[var(--muted-foreground)] hover:text-[var(--foreground)] transition-all';
      renderChart();
    }});

    setTimeout(resizeCanvas, 50);
  </script>
</body>
</html>
"""

with open(artifact_path, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"Generated chart artifact at: {artifact_path} ({len(html_content)} bytes)")
