// =============================================================================
// dashboard/app.js — Logic tương tác, fetch dữ liệu HDFS & vẽ biểu đồ
// Hỗ trợ chế độ Sáng (Light Theme) mặc định và chuyển đổi Sáng/Tối
// =============================================================================

let reportData = null;
let metricsData = null;

let trafficChartInstance = null;
let errorRateChartInstance = null;
let latencyChartInstance = null;
let ipFloodChartInstance = null;
let endpointChartInstance = null;

document.addEventListener("DOMContentLoaded", () => {
  initTheme();
  initDatePicker();
  checkHdfsStatus();
  loadDashboardData();

  // Sự kiện
  document.getElementById("refreshBtn").addEventListener("click", () => {
    loadDashboardData();
  });

  document.getElementById("reportDatePicker").addEventListener("change", (e) => {
    loadDashboardData(e.target.value);
  });

  document.getElementById("filterRule").addEventListener("change", renderAnomaliesTable);
  document.getElementById("filterSeverity").addEventListener("change", renderAnomaliesTable);
  document.getElementById("filterSearch").addEventListener("input", renderAnomaliesTable);
});

// Quản lý theme Sáng / Tối
function initTheme() {
  const savedTheme = localStorage.getItem("dashboard_theme") || "light";
  applyTheme(savedTheme);

  const toggleBtn = document.getElementById("themeToggleBtn");
  if (toggleBtn) {
    toggleBtn.addEventListener("click", () => {
      const current = document.documentElement.getAttribute("data-theme") || "light";
      const next = current === "dark" ? "light" : "dark";
      applyTheme(next);
      localStorage.setItem("dashboard_theme", next);
      if (reportData && metricsData) {
        renderCharts(reportData, metricsData);
      }
    });
  }
}

function applyTheme(theme) {
  document.documentElement.setAttribute("data-theme", theme);
  const icon = document.getElementById("themeIcon");
  const text = document.getElementById("themeText");
  if (theme === "dark") {
    if (icon) icon.textContent = "🌙";
    if (text) text.textContent = "Tối";
  } else {
    if (icon) icon.textContent = "☀️";
    if (text) text.textContent = "Sáng";
  }
}

function getThemePalette() {
  const isLight = document.documentElement.getAttribute("data-theme") !== "dark";
  return {
    gridColor: isLight ? "rgba(0, 0, 0, 0.06)" : "rgba(255, 255, 255, 0.06)",
    tickColor: isLight ? "#64748b" : "#94a3b8",
    legendColor: isLight ? "#334155" : "#cbd5e1",
    labelPrimary: isLight ? "#0f172a" : "#f1f5f9",
    trafficLine: isLight ? "#0284c7" : "#00f2fe",
    trafficFill: isLight ? "rgba(2, 132, 199, 0.12)" : "rgba(0, 242, 254, 0.12)",
    errBar: isLight ? "rgba(220, 38, 38, 0.85)" : "rgba(255, 107, 129, 0.75)",
    c5xxBar: isLight ? "rgba(185, 28, 28, 0.95)" : "rgba(255, 71, 87, 0.95)",
    latencyBarNormal: isLight ? "rgba(37, 99, 235, 0.75)" : "rgba(79, 172, 254, 0.75)",
    latencyBarDanger: isLight ? "rgba(220, 38, 38, 0.85)" : "rgba(255, 71, 87, 0.85)",
    ipFloodBar: isLight ? "rgba(217, 119, 6, 0.88)" : "rgba(255, 165, 2, 0.85)",
    epBarNormal: isLight ? "rgba(37, 99, 235, 0.75)" : "rgba(79, 172, 254, 0.75)",
    epBarDanger: isLight ? "rgba(220, 38, 38, 0.85)" : "rgba(255, 71, 87, 0.85)",
    warnLine: isLight ? "#d97706" : "#ffa502",
    critLine: isLight ? "#dc2626" : "#ff4757"
  };
}

function initDatePicker() {
  const picker = document.getElementById("reportDatePicker");
  const today = new Date().toISOString().split("T")[0];
  picker.value = today;
}

async function checkHdfsStatus() {
  const ind = document.getElementById("hdfsIndicator");
  const txt = document.getElementById("hdfsStatusText");

  try {
    const res = await fetch("/api/status");
    const data = await res.json();
    if (data.hdfs_status === "CONNECTED") {
      ind.className = "status-indicator connected";
      txt.textContent = "HDFS: Đã Kết Nối";
    } else {
      ind.className = "status-indicator disconnected";
      txt.textContent = "HDFS: Chưa kết nối (Bản Sao Cục Bộ)";
    }
  } catch (err) {
    ind.className = "status-indicator disconnected";
    txt.textContent = "Server Cục Bộ";
  }
}

async function loadDashboardData(customDate = null) {
  const dateStr = customDate || document.getElementById("reportDatePicker").value;
  const banner = document.getElementById("sourceBannerText");
  banner.textContent = `Đang nạp dữ liệu phân tích cho ngày ${dateStr}...`;

  try {
    // 1. Fetch report JSON từ HDFS / Fallback
    const repRes = await fetch(`/api/report?date=${dateStr}`);
    reportData = await repRes.json();

    // 2. Fetch metrics chi tiết 24h
    const metRes = await fetch(`/api/metrics?date=${dateStr}`);
    metricsData = await metRes.json();

    updateSourceBanner(reportData);
    updateKpis(reportData, metricsData);
    updateRulesOverview(reportData);
    renderCharts(reportData, metricsData);
    renderAnomaliesTable();
  } catch (error) {
    console.error("Lỗi khi tải dữ liệu:", error);
    banner.textContent = `Lỗi tải dữ liệu: ${error.message}`;
  }
}

function updateSourceBanner(data) {
  const banner = document.getElementById("sourceBannerText");
  const ind = document.getElementById("hdfsIndicator");
  const txt = document.getElementById("hdfsStatusText");

  if (data._source === "HDFS") {
    banner.innerHTML = `<strong>Nguồn Dữ Liệu:</strong> Trực tiếp từ cụm <strong>Hadoop HDFS</strong> (<code>${data._source_path}</code>)`;
    if (ind && txt) {
      ind.className = "status-indicator connected";
      txt.textContent = "HDFS: Đã Kết Nối";
    }
  } else if (data._source) {
    banner.innerHTML = `<strong>Nguồn Dữ Liệu:</strong> Bản sao dự phòng cục bộ (<code>${data._source_path}</code>) — <em>Sẵn sàng đồng bộ lại khi khởi động HDFS</em>`;
    if (ind && txt) {
      ind.className = "status-indicator disconnected";
      txt.textContent = "HDFS: Chưa kết nối (Bản Sao Cục Bộ)";
    }
  } else {
    banner.textContent = data.error || "Không có dữ liệu.";
  }
}

function updateKpis(report, metrics) {
  let totalReqs = 0;
  if (metrics && metrics.hour_requests) {
    totalReqs = Object.values(metrics.hour_requests).reduce((a, b) => a + b, 0);
  } else if (report.baseline) {
    const mean = report.baseline.mean_hourly_traffic ?? report.baseline.mean ?? 0;
    totalReqs = Math.round(mean * 24);
  }
  document.getElementById("kpiTotalRequests").textContent = totalReqs.toLocaleString();

  const meanTraffic = report.baseline
    ? Math.round(report.baseline.mean_hourly_traffic ?? report.baseline.mean ?? (totalReqs / 24))
    : (totalReqs > 0 ? Math.round(totalReqs / 24) : 0);
  document.getElementById("kpiMeanTraffic").textContent = `Trung bình: ${meanTraffic.toLocaleString()} reqs/giờ`;

  const totalAnomalies = report.total_anomalies_detected ?? report.total_anomalies ?? (report.anomalies ? report.anomalies.length : 0);
  document.getElementById("kpiTotalAnomalies").textContent = totalAnomalies;

  const affectedHours = report.affected_hours_count !== undefined
    ? report.affected_hours_count
    : new Set((report.anomalies || []).map(a => a.hour).filter(h => h !== undefined)).size;
  const affectedHoursEl = document.getElementById("kpiAffectedHours");
  if (affectedHoursEl) {
    affectedHoursEl.textContent = `Ảnh hưởng: ${affectedHours} khung giờ`;
  }

  const criticals = (report.anomalies || []).filter(a => a.severity === "CRITICAL").length;
  const warnings = (report.anomalies || []).filter(a => a.severity === "WARNING").length;

  document.getElementById("kpiCriticalCount").textContent = criticals;
  document.getElementById("kpiWarningCount").textContent = `Cảnh báo WARNING: ${warnings}`;

  const ipFloodList = (report.anomalies || []).filter(a => a.rule === "R04_IP_FLOOD");
  document.getElementById("kpiIpFloodCount").textContent = ipFloodList.length;
  if (ipFloodList.length > 0) {
    const topIp = ipFloodList.sort((a, b) => (b.request_count || 0) - (a.request_count || 0))[0];
    document.getElementById("kpiTopFloodIp").textContent = `Cao nhất: ${topIp.ip} (${(topIp.request_count || 0).toLocaleString()} reqs)`;
  } else {
    document.getElementById("kpiTopFloodIp").textContent = "Không có IP nào vượt ngưỡng 1.000";
  }
}

function updateRulesOverview(report) {
  const anomalies = report.anomalies || [];

  const r01Items = anomalies.filter(a => a.rule === "R01_TRAFFIC_SPIKE");
  const r02Items = anomalies.filter(a => a.rule === "R02_ERROR_RATE");
  const r03Items = anomalies.filter(a => a.rule === "R03_HTTP_5XX_RATE");
  const r04Items = anomalies.filter(a => a.rule === "R04_IP_FLOOD");
  const r05Items = anomalies.filter(a => a.rule === "R05_RESPONSE_TIME");
  const r06Items = anomalies.filter(a => a.rule === "R06_ENDPOINT_ERROR_SPIKE");

  const critThreshold = report.baseline ? Math.round(report.baseline.critical_threshold || 0) : 0;
  document.getElementById("valCritThreshold").textContent = `≥ ${critThreshold.toLocaleString()} reqs/h`;

  document.getElementById("countR01").textContent = `${r01Items.length} giờ`;
  document.getElementById("countR02").textContent = `${r02Items.length} giờ`;
  document.getElementById("countR03").textContent = `${r03Items.length} giờ`;
  document.getElementById("countR04").textContent = `${r04Items.length} IP`;
  document.getElementById("countR05").textContent = `${r05Items.length} giờ`;
  document.getElementById("countR06").textContent = `${r06Items.length} endpoint`;

  updateCardStatus("cardR01", "statusR01", r01Items);
  updateCardStatus("cardR02", "statusR02", r02Items);
  updateCardStatus("cardR03", "statusR03", r03Items);
  updateCardStatus("cardR04", "statusR04", r04Items);
  updateCardStatus("cardR05", "statusR05", r05Items);
  updateCardStatus("cardR06", "statusR06", r06Items);
}

function updateCardStatus(cardId, badgeId, items) {
  const card = document.getElementById(cardId);
  const badge = document.getElementById(badgeId);
  if (!card || !badge) return;

  card.classList.remove("triggered-critical", "triggered-warning");

  if (items.length === 0) {
    badge.className = "rule-status-badge badge-normal";
    badge.textContent = "Bình thường";
  } else {
    const hasCrit = items.some(i => i.severity === "CRITICAL");
    if (hasCrit) {
      card.classList.add("triggered-critical");
      badge.className = "rule-status-badge badge-critical";
      badge.textContent = "BÁO ĐỘNG (CRITICAL)";
    } else {
      card.classList.add("triggered-warning");
      badge.className = "rule-status-badge badge-warning";
      badge.textContent = "CẢNH BÁO (WARNING)";
    }
  }
}

function renderCharts(report, metrics) {
  const colors = getThemePalette();
  const hours = Array.from({ length: 24 }, (_, i) => `${String(i).padStart(2, "0")}:00`);

  const reqData = hours.map(h => metrics.hour_requests?.[h.slice(0, 2)] || 0);
  const errData = hours.map(h => metrics.hour_errors?.[h.slice(0, 2)] || 0);
  const c5xxData = hours.map(h => metrics.hour_5xx?.[h.slice(0, 2)] || 0);
  const respData = hours.map(h => metrics.hour_avg_resp?.[h.slice(0, 2)] || 0);

  // Tính tỷ lệ %
  const errRateData = hours.map((h, i) => reqData[i] > 0 ? +(errData[i] / reqData[i] * 100).toFixed(1) : 0);
  const c5xxRateData = hours.map((h, i) => reqData[i] > 0 ? +(c5xxData[i] / reqData[i] * 100).toFixed(1) : 0);

  const warnThresh = report.baseline?.warning_threshold || 0;
  const critThresh = report.baseline?.critical_threshold || 0;

  // Chart 1: Traffic 24h (Quy tắc 1)
  if (trafficChartInstance) trafficChartInstance.destroy();
  const ctxTraffic = document.getElementById("trafficChart").getContext("2d");
  trafficChartInstance = new Chart(ctxTraffic, {
    type: "line",
    data: {
      labels: hours,
      datasets: [
        {
          label: "Lưu lượng thực tế (Yêu cầu/giờ)",
          data: reqData,
          borderColor: colors.trafficLine,
          backgroundColor: colors.trafficFill,
          fill: true,
          tension: 0.35,
          borderWidth: 2.5,
          pointRadius: 4,
          pointHoverRadius: 6,
        },
        {
          label: `Ngưỡng Báo Động Critical (${Math.round(critThresh)})`,
          data: Array(24).fill(critThresh),
          borderColor: colors.critLine,
          borderDash: [6, 4],
          borderWidth: 2,
          pointRadius: 0,
          fill: false,
        },
        {
          label: `Ngưỡng Cảnh Báo Warning (${Math.round(warnThresh)})`,
          data: Array(24).fill(warnThresh),
          borderColor: colors.warnLine,
          borderDash: [4, 4],
          borderWidth: 1.5,
          pointRadius: 0,
          fill: false,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: colors.legendColor, font: { family: "Plus Jakarta Sans" } } },
        tooltip: { mode: "index", intersect: false }
      },
      scales: {
        x: { grid: { color: colors.gridColor }, ticks: { color: colors.tickColor } },
        y: { grid: { color: colors.gridColor }, ticks: { color: colors.tickColor } }
      }
    }
  });

  // Chart 2: Tỷ lệ Error vs 5xx (Quy tắc 2 & 3)
  if (errorRateChartInstance) errorRateChartInstance.destroy();
  const ctxErr = document.getElementById("errorRateChart").getContext("2d");
  errorRateChartInstance = new Chart(ctxErr, {
    type: "bar",
    data: {
      labels: hours,
      datasets: [
        {
          label: "Tỷ lệ Log ERROR Ứng dụng (%)",
          data: errRateData,
          backgroundColor: colors.errBar,
          borderRadius: 4
        },
        {
          label: "Tỷ lệ Lỗi Máy Chủ 5xx (%)",
          data: c5xxRateData,
          backgroundColor: colors.c5xxBar,
          borderRadius: 4
        },
        {
          type: "line",
          label: "Ngưỡng Cảnh Báo (5.0%)",
          data: Array(24).fill(5.0),
          borderColor: colors.warnLine,
          borderDash: [5, 5],
          pointRadius: 0,
          borderWidth: 1.5
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: colors.legendColor } },
        tooltip: { mode: "index", intersect: false }
      },
      scales: {
        x: { grid: { color: colors.gridColor }, ticks: { color: colors.tickColor } },
        y: { 
          grid: { color: colors.gridColor }, 
          ticks: { color: colors.tickColor, callback: v => v + "%" },
          suggestedMax: 15
        }
      }
    }
  });

  // Chart 3: Độ trễ xử lý 24h (Quy tắc 5)
  if (latencyChartInstance) latencyChartInstance.destroy();
  const ctxLat = document.getElementById("latencyChart").getContext("2d");
  latencyChartInstance = new Chart(ctxLat, {
    type: "bar",
    data: {
      labels: hours,
      datasets: [
        {
          label: "Độ trễ TB mỗi giờ (ms)",
          data: respData,
          backgroundColor: respData.map(v => v >= 2000 ? colors.latencyBarDanger : colors.latencyBarNormal),
          borderRadius: 4
        },
        {
          type: "line",
          label: "Ngưỡng Trần Cho Phép (2.000 ms)",
          data: Array(24).fill(2000),
          borderColor: colors.critLine,
          borderDash: [5, 4],
          borderWidth: 2,
          pointRadius: 0
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: colors.legendColor } },
        tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${ctx.raw} ms` } }
      },
      scales: {
        x: { grid: { color: colors.gridColor }, ticks: { color: colors.tickColor } },
        y: { 
          grid: { color: colors.gridColor }, 
          ticks: { color: colors.tickColor, callback: v => v + "ms" } 
        }
      }
    }
  });

  // Chart 4: Top IP Flood (Quy tắc 4)
  if (ipFloodChartInstance) ipFloodChartInstance.destroy();
  const ipFloodAnomalies = (report.anomalies || [])
    .filter(a => a.rule === "R04_IP_FLOOD")
    .sort((a, b) => (b.request_count || 0) - (a.request_count || 0))
    .slice(0, 8);

  const ipLabels = ipFloodAnomalies.length > 0
    ? ipFloodAnomalies.map(a => `${a.ip} (${a.hour || '??'}h)`)
    : ["Không có IP quá tải"];
  const ipCounts = ipFloodAnomalies.length > 0
    ? ipFloodAnomalies.map(a => a.request_count || 0)
    : [0];

  const ctxIp = document.getElementById("ipFloodChart").getContext("2d");
  ipFloodChartInstance = new Chart(ctxIp, {
    type: "bar",
    data: {
      labels: ipLabels,
      datasets: [
        {
          label: "Số yêu cầu / giờ (reqs/h)",
          data: ipCounts,
          backgroundColor: colors.ipFloodBar,
          borderRadius: 6
        },
        {
          type: "line",
          label: "Ngưỡng Quá Tải (1.000 reqs/h)",
          data: Array(ipLabels.length).fill(1000),
          borderColor: colors.critLine,
          borderDash: [5, 4],
          borderWidth: 2,
          pointRadius: 0
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: colors.legendColor } },
        tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${ctx.raw.toLocaleString()} reqs` } }
      },
      scales: {
        x: { grid: { color: colors.gridColor }, ticks: { color: colors.labelPrimary, font: { family: "JetBrains Mono" } } },
        y: { 
          grid: { color: colors.gridColor }, 
          ticks: { color: colors.tickColor },
          suggestedMax: 2000
        }
      }
    }
  });

  // Chart 5: Top Error Endpoints (Quy tắc 6)
  if (endpointChartInstance) endpointChartInstance.destroy();
  const topEps = report.top_error_endpoints || [];
  const epLabels = topEps.map(e => e.endpoint);
  const epCounts = topEps.map(e => e.error_count);

  const ctxEp = document.getElementById("endpointChart").getContext("2d");
  endpointChartInstance = new Chart(ctxEp, {
    type: "bar",
    data: {
      labels: epLabels,
      datasets: [
        {
          label: "Tổng số lần phát sinh lỗi (status >= 400)",
          data: epCounts,
          backgroundColor: epCounts.map(c => c >= 15000 ? colors.epBarDanger : colors.epBarNormal),
          borderRadius: 6
        }
      ]
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: colors.legendColor } }
      },
      scales: {
        x: { grid: { color: colors.gridColor }, ticks: { color: colors.tickColor } },
        y: { grid: { color: colors.gridColor }, ticks: { color: colors.labelPrimary, font: { family: "JetBrains Mono" } } }
      }
    }
  });
}

function renderAnomaliesTable() {
  const tbody = document.getElementById("anomaliesTableBody");
  if (!reportData || !reportData.anomalies) {
    tbody.innerHTML = '<tr><td colspan="5" class="table-empty">Chưa có dữ liệu cảnh báo.</td></tr>';
    return;
  }

  const selectedRule = document.getElementById("filterRule").value;
  const selectedSev = document.getElementById("filterSeverity").value;
  const searchText = document.getElementById("filterSearch").value.toLowerCase().trim();

  let filtered = reportData.anomalies.filter(item => {
    if (selectedRule !== "ALL" && item.rule !== selectedRule) return false;
    if (selectedSev !== "ALL" && item.severity !== selectedSev) return false;
    if (searchText) {
      const targetStr = `${item.rule} ${item.message} ${item.hour || ""} ${item.ip || ""} ${item.endpoint || ""}`.toLowerCase();
      if (!targetStr.includes(searchText)) return false;
    }
    return true;
  });

  if (filtered.length === 0) {
    tbody.innerHTML = '<tr><td colspan="5" class="table-empty">Không tìm thấy cảnh báo phù hợp với bộ lọc.</td></tr>';
    return;
  }

  tbody.innerHTML = filtered.map(item => {
    const sevBadgeClass = item.severity === "CRITICAL" ? "table-badge-critical" : "table-badge-warning";
    const hourDisplay = item.hour ? `${item.hour}:00` : "Cả ngày";
    const targetEntity = item.ip ? `IP: <code>${item.ip}</code>` : (item.endpoint ? `<code>${item.endpoint}</code>` : "--");

    return `
      <tr>
        <td><strong>${hourDisplay}</strong></td>
        <td><span class="rule-code-badge">${item.rule}</span></td>
        <td><span class="${sevBadgeClass}">${item.severity}</span></td>
        <td>${escapeHtml(item.message)}</td>
        <td>${targetEntity}</td>
      </tr>
    `;
  }).join("");
}

function escapeHtml(text) {
  if (!text) return "";
  return text
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
