function renderMDDataGrid(productKey) {
      var cy = new Date().getFullYear(), years8 = [cy, cy - 1, cy - 2, cy - 3, cy - 4, cy - 5, cy - 6, cy - 7];
      var seen = {}, allYears = [];
      DATA.master.forEach(function (r) { if (!seen[r.year]) { seen[r.year] = 1; allYears.push(r.year); } });
      allYears.sort(function (a, b) { return a - b; });
      var matrix = {};
      allYears.forEach(function (y) { matrix[y] = {}; for (var m = 0; m < 12; m++) { var v = DATA.master.filter(function (r) { return r.year === y && r.month === m && r[productKey] != null; }).map(function (r) { return r[productKey]; }); matrix[y][m] = v.length ? mean(v) : null; } });
      var colStats = {};
      for (var m = 0; m < 12; m++) { var v = years8.map(function (y) { return matrix[y] ? matrix[y][m] : null; }).filter(function (v) { return v != null; }); colStats[m] = { min: Math.min.apply(null, v), max: Math.max.apply(null, v) }; }
      
      var avg8y = {};
      for (var m = 0; m < 12; m++) { var v = years8.map(function (y) { return matrix[y] ? matrix[y][m] : null; }).filter(function (v) { return v != null; }); avg8y[m] = v.length ? mean(v) : null; }
      
      var html = '<table class="heatmap-table"><thead><tr><th>Year</th>' + MONTH_NAMES.map(function (mn) { return '<th>' + mn + '</th>'; }).join('') + '<th>Full Year Avg</th><th>YoY %</th></tr></thead><tbody>';
      
      years8.forEach(function (y, i) {
        var rv = []; for (var m = 0; m < 12; m++)rv.push(matrix[y] ? matrix[y][m] : null);
        var nn = rv.filter(function (v) { return v != null; }), fy = nn.length ? mean(nn) : null;
        
        var py2 = years8[i + 1];
        var pv = [];
        if (py2 != null) { for (var m = 0; m < 12; m++) pv.push(matrix[py2] ? matrix[py2][m] : null); }
        var pnn = pv.filter(function (v) { return v != null; }), pfy = pnn.length ? mean(pnn) : null;
        
        var yoy = fy != null && pfy != null && pfy !== 0 ? ((fy - pfy) / pfy) * 100 : null;
        
        html += '<tr><td class="year-col">' + y + '</td>';
        for (var m = 0; m < 12; m++) { var v = rv[m]; if (v == null) { html += '<td style="color:#484f58">&mdash;</td>'; continue; } var cs = colStats[m], r2 = cs.max > cs.min ? (v - cs.min) / (cs.max - cs.min) : 0.5; html += '<td style="background:' + heatmapColorAbs(r2) + ';color:#e6edf3">' + fmt(v, 0) + '</td>'; }
        
        html += '<td style="color:var(--text-muted)">' + (fy != null ? fmt(fy, 0) : '&mdash;') + '</td>';
        html += '<td class="' + (yoy == null ? '' : yoy >= 0 ? 'val-green' : 'val-red') + '" style="font-weight:600">' + (yoy != null ? (yoy >= 0 ? '+' : '') + yoy.toFixed(1) + '%' : '&mdash;') + '</td></tr>';
      });
      
      html += '<tr><td class="year-col" style="color:var(--accent);font-weight:700">8Y Avg</td>';
      var a8all = [];
      for (var m = 0; m < 12; m++) { var v = avg8y[m]; a8all.push(v); if (v == null) { html += '<td style="color:#484f58">&mdash;</td>'; continue; } var cs = colStats[m], r2 = cs.max > cs.min ? (v - cs.min) / (cs.max - cs.min) : 0.5; html += '<td style="background:' + heatmapColorAbs(r2) + ';color:#e6edf3;font-weight:600">' + fmt(v, 0) + '</td>'; }
      var a8avg = mean(a8all.filter(function (v) { return v != null; }));
      html += '<td style="color:var(--accent);font-weight:600">' + (a8avg != null && !isNaN(a8avg) ? fmt(a8avg, 0) : '&mdash;') + '</td><td>&mdash;</td></tr>';
      
      var ly = years8[0];
      html += '<tr><td class="year-col" style="color:var(--text-muted)">MoM %</td>';
      for (var m = 0; m < 12; m++) {
        var curr = matrix[ly] ? matrix[ly][m] : null, prev = m > 0 ? (matrix[ly] ? matrix[ly][m - 1] : null) : (matrix[ly - 1] ? matrix[ly - 1][11] : null);
        if (curr == null || prev == null) { html += '<td style="color:#484f58">&mdash;</td>'; continue; }
        var pct = ((curr - prev) / prev) * 100;
        html += '<td style="background:' + heatmapColorPct(pct) + ';color:#e6edf3;font-size:11px">' + (pct >= 0 ? '+' : '') + pct.toFixed(1) + '%</td>';
      }
      html += '<td>&mdash;</td><td>&mdash;</td></tr></tbody></table>';
      
      document.getElementById('monthlyDataGrid').innerHTML = html;
      document.getElementById('monthlyGridDlBtn').onclick = function () {
        var headers = ['Year'].concat(MONTH_NAMES).concat(['Full Year Avg', 'YoY %']);
        var rows = years8.map(function (y, i) { 
          var rv = []; for (var m = 0; m < 12; m++)rv.push(matrix[y] ? matrix[y][m] : null); 
          var nn = rv.filter(function (v) { return v != null; }), fy = nn.length ? mean(nn) : null; 
          
          var py2 = years8[i + 1];
          var pv = [];
          if (py2 != null) { for (var m = 0; m < 12; m++) pv.push(matrix[py2] ? matrix[py2][m] : null); }
          var pnn = pv.filter(function (v) { return v != null; }), pfy = pnn.length ? mean(pnn) : null;
          var yoy = fy != null && pfy != null && pfy !== 0 ? ((fy - pfy) / pfy * 100).toFixed(1) + '%' : '';
          
          return [y].concat(rv.map(function (v) { return v != null ? v.toFixed(2) : '' })).concat([fy != null ? fy.toFixed(2) : '', yoy]); 
        });
        downloadCSV('monthly_grid_' + productKey + '.csv', headers, rows);
      };
    }