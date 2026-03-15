function renderQDDataGrid(productKey) {
      var cy = new Date().getFullYear(), years8 = [cy, cy - 1, cy - 2, cy - 3, cy - 4, cy - 5, cy - 6, cy - 7], qs = ['Q1', 'Q2', 'Q3', 'Q4'];
      var seen = {}, allYears = [];
      DATA.master.forEach(function (r) { if (!seen[r.year]) { seen[r.year] = 1; allYears.push(r.year); } });
      allYears.sort(function (a, b) { return a - b; });
      var matrix = {};
      allYears.forEach(function (y) { matrix[y] = {}; qs.forEach(function (q) { matrix[y][q] = getQAvgPD(productKey, y, q); }); });
      var colStats = {};
      qs.forEach(function (q) { var v = years8.map(function (y) { return matrix[y] ? matrix[y][q] : null; }).filter(function (v) { return v != null; }); colStats[q] = { min: Math.min.apply(null, v), max: Math.max.apply(null, v) }; });
      
      var avg8y = {};
      qs.forEach(function (q) { var v = years8.map(function (y) { return matrix[y] ? matrix[y][q] : null; }).filter(function (v) { return v != null; }); avg8y[q] = v.length ? mean(v) : null; });
      
      var html = '<table class="heatmap-table"><thead><tr><th>Year</th>' + qs.map(function (q) { return '<th>' + q + '</th>'; }).join('') + '<th>Full Year Avg</th><th>YoY %</th></tr></thead><tbody>';
      years8.forEach(function (y, i) {
        var qv = qs.map(function (q) { return matrix[y] ? matrix[y][q] : null; }), nn = qv.filter(function (v) { return v != null; }), fy = nn.length ? mean(nn) : null;
        var py2 = years8[i + 1], pv = py2 != null ? qs.map(function (q) { return matrix[py2] ? matrix[py2][q] : null; }).filter(function (v) { return v != null; }) : [], pfy = pv.length ? mean(pv) : null;
        var yoy = fy != null && pfy != null && pfy !== 0 ? ((fy - pfy) / pfy) * 100 : null;
        html += '<tr><td class="year-col">' + y + '</td>';
        qs.forEach(function (q) { var v = matrix[y] ? matrix[y][q] : null; if (v == null) { html += '<td style="color:#484f58">&mdash;</td>'; return; } var cs = colStats[q], r2 = cs.max > cs.min ? (v - cs.min) / (cs.max - cs.min) : 0.5; html += '<td style="background:' + heatmapColorAbs(r2) + ';color:#e6edf3">' + fmt(v, 0) + '</td>'; });
        html += '<td style="color:var(--text-muted)">' + (fy != null ? fmt(fy, 0) : '&mdash;') + '</td>';
        html += '<td class="' + (yoy == null ? '' : yoy >= 0 ? 'val-green' : 'val-red') + '" style="font-weight:600">' + (yoy != null ? (yoy >= 0 ? '+' : '') + yoy.toFixed(1) + '%' : '&mdash;') + '</td></tr>';
      });
      
      html += '<tr><td class="year-col" style="color:var(--accent);font-weight:700">8Y Avg</td>';
      var a8all = [];
      qs.forEach(function (q) { var v = avg8y[q]; a8all.push(v); if (v == null) { html += '<td style="color:#484f58">&mdash;</td>'; return; } var cs = colStats[q], r2 = cs.max > cs.min ? (v - cs.min) / (cs.max - cs.min) : 0.5; html += '<td style="background:' + heatmapColorAbs(r2) + ';color:#e6edf3;font-weight:600">' + fmt(v, 0) + '</td>'; });
      var a8avg = mean(a8all.filter(function (v) { return v != null; }));
      html += '<td style="color:var(--accent);font-weight:600">' + (a8avg != null && !isNaN(a8avg) ? fmt(a8avg, 0) : '&mdash;') + '</td><td>&mdash;</td></tr>';
      
      var ly = years8[0];
      html += '<tr><td class="year-col" style="color:var(--text-muted)">QoQ %</td>';
      for (var i = 0; i < qs.length; i++) {
        var q = qs[i];
        var curr = matrix[ly] ? matrix[ly][q] : null;
        var prev = null;
        if (i > 0) { prev = matrix[ly] ? matrix[ly][qs[i-1]] : null; } 
        else { prev = matrix[ly-1] ? matrix[ly-1][qs[3]] : null; }
        
        if (curr == null || prev == null) { html += '<td style="color:#484f58">&mdash;</td>'; continue; }
        var pct = ((curr - prev) / prev) * 100;
        html += '<td style="background:' + heatmapColorPct(pct) + ';color:#e6edf3;font-size:11px">' + (pct >= 0 ? '+' : '') + pct.toFixed(1) + '%</td>';
      }
      
      html += '<td>&mdash;</td><td>&mdash;</td></tr></tbody></table>';
      
      document.getElementById('quarterlyDashDataGrid').innerHTML = html;
      document.getElementById('quarterlyDashGridDlBtn').onclick = function () {
        var headers = ['Year'].concat(qs).concat(['Full Year Avg', 'YoY %']);
        var rows = years8.map(function (y, i) {
          var qv = qs.map(function (q) { return matrix[y] ? matrix[y][q] : null; }), nn = qv.filter(function (v) { return v != null; }), fy = nn.length ? mean(nn) : null;
          var py2 = years8[i + 1], pv = py2 != null ? qs.map(function (q) { return matrix[py2] ? matrix[py2][q] : null; }).filter(function (v) { return v != null; }) : [], pfy = pv.length ? mean(pv) : null;
          var yoy = fy != null && pfy != null && pfy !== 0 ? ((fy - pfy) / pfy * 100).toFixed(1) + '%' : '';
          return [y].concat(qv.map(function (v) { return v != null ? v.toFixed(2) : '' })).concat([fy != null ? fy.toFixed(2) : '', yoy]);
        });
        downloadCSV('quarterly_dash_' + productKey + '.csv', headers, rows);
      };
    }