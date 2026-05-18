/**
 * QuantPilot AI - Charts Module
 * Chart.js configuration and rendering.
 */

// Chart instances
let equityChart = null;
let dailyPnlChart = null;
let winlossChart = null;
let userEquityChart = null;
window.marketChart = window.marketChart || null;

/**
 * Default chart options
 */
function chartOptions(yLabel) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
            intersect: false,
            mode: 'index',
        },
        plugins: {
            legend: {
                display: false,
            },
            tooltip: {
                backgroundColor: '#1a1f2e',
                borderColor: '#2a3042',
                borderWidth: 1,
                titleColor: '#e8eaed',
                bodyColor: '#9ca3af',
                cornerRadius: 8,
                padding: 12,
            },
        },
        scales: {
            x: {
                grid: {
                    color: 'rgba(42, 48, 66, 0.5)',
                },
                ticks: {
                    color: '#6b7280',
                    font: { size: 11 },
                    maxTicksLimit: 12,
                },
            },
            y: {
                grid: {
                    color: 'rgba(42, 48, 66, 0.5)',
                },
                ticks: {
                    color: '#6b7280',
                    font: { size: 11 },
                },
                title: {
                    display: true,
                    text: yLabel,
                    color: '#6b7280',
                },
            },
        },
    };
}

/**
 * Render equity curve chart
 */
function renderEquityChart(curve) {
    const ctx = document.getElementById('equity-chart')?.getContext('2d');
    if (!ctx) return;

    if (equityChart) {
        equityChart.destroy();
    }

    // Create gradient
    const gradient = ctx.createLinearGradient(0, 0, 0, 280);
    gradient.addColorStop(0, 'rgba(37, 99, 235, 0.3)');
    gradient.addColorStop(1, 'rgba(37, 99, 235, 0.0)');

    // Format labels: prefer date/timestamp, fall back to trade number
    const labels = curve.map(c => {
        if (c.date) return c.date;
        if (c.timestamp) {
            return new Date(c.timestamp).toLocaleDateString(undefined, {
                month: 'short',
                day: 'numeric',
            });
        }
        return `#${c.trade || ''}`;
    });

    equityChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [{
                label: 'Cumulative P&L %',
                data: curve.map(c => c.cumulative_pnl),
                borderColor: '#2563eb',
                backgroundColor: gradient,
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                pointRadius: 0,
                pointHoverRadius: 5,
            }],
        },
        options: chartOptions('P&L %'),
    });
}

/**
 * Render daily PnL bar chart
 */
function renderDailyPnlChart(daily) {
    const ctx = document.getElementById('daily-pnl-chart')?.getContext('2d');
    if (!ctx) return;

    if (dailyPnlChart) {
        dailyPnlChart.destroy();
    }

    dailyPnlChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: daily.map(d => d.date),
            datasets: [{
                label: 'Daily P&L %',
                data: daily.map(d => d.pnl),
                backgroundColor: daily.map(d =>
                    d.pnl >= 0 ? 'rgba(5, 150, 105, 0.7)' : 'rgba(225, 29, 72, 0.7)'
                ),
                borderRadius: 4,
            }],
        },
        options: chartOptions('P&L %'),
    });
}

/**
 * Render win/loss doughnut chart
 */
function renderWinLossChart(perf) {
    const ctx = document.getElementById('winloss-chart')?.getContext('2d');
    if (!ctx) return;

    if (winlossChart) {
        winlossChart.destroy();
    }

    winlossChart = new Chart(ctx, {
        type: 'doughnut',
        data: {
            labels: ['Wins', 'Losses', 'Breakeven'],
            datasets: [{
                data: [
                    perf.winning_trades || 0,
                    perf.losing_trades || 0,
                    perf.breakeven_trades || 0,
                ],
                backgroundColor: ['#059669', '#e11d48', '#6b7280'],
                borderColor: 'transparent',
                borderWidth: 0,
            }],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    labels: {
                        color: '#9ca3af',
                        padding: 16,
                        font: { size: 12 },
                    },
                },
            },
            cutout: '65%',
        },
    });
}

/**
 * Set chart period
 */
function setChartPeriod(evt, days) {
    // Update button states
    document.querySelectorAll('.card-actions .btn-sm').forEach(b => {
        b.classList.remove('active');
    });
    evt.target.classList.add('active');

    // Fetch and render
    fetchAPI(`/api/performance?days=${days}`)
        .then(perf => renderEquityChart(perf.equity_curve || []))
        .catch(e => showToast(e.message, 'error'));
}

/**
 * Destroy all charts
 */
function destroyAllCharts() {
    if (equityChart) {
        equityChart.destroy();
        equityChart = null;
    }
    if (dailyPnlChart) {
        dailyPnlChart.destroy();
        dailyPnlChart = null;
    }
    if (winlossChart) {
        winlossChart.destroy();
        winlossChart = null;
    }
    if (userEquityChart) {
        userEquityChart.destroy();
        userEquityChart = null;
    }
    if (window.marketChart) {
        window.marketChart.destroy();
        window.marketChart = null;
    }
}

// Export functions globally for app.js compatibility
window.chartOptions = chartOptions;
window.renderEquityChart = renderEquityChart;
window.renderDailyPnlChart = renderDailyPnlChart;
window.renderWinLossChart = renderWinLossChart;
window.setChartPeriod = setChartPeriod;
window.destroyAllCharts = destroyAllCharts;
