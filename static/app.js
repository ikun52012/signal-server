/**
 * QuantPilot AI - Dashboard Frontend Logic
 * v4.5 — AI command center redesign, trading controls, strategies, PWA, i18n content pages
 */

const API = '';
const USDT_PAYMENT_NETWORKS = [
    { id: 'TRC20', name: 'Tron (TRC20)' },
    { id: 'ERC20', name: 'Ethereum (ERC20)' },
    { id: 'BEP20', name: 'BSC (BEP20)' },
    { id: 'ARBITRUM', name: 'Arbitrum One' },
    { id: 'APT', name: 'Aptos (APT)' },
    { id: 'SOL', name: 'Solana (SPL)' },
];

let currentUserSettings = null;
let currentRuntimeStatus = null;
let _strategyTemplates = [];
let _systemSocket = null;
let _priceSocket = null;
let _priceSocketTicker = '';
let _chartRealtimeState = null;
let _launchContext = null;

// ─── i18n / Multi-language Support ───
let _i18nCache = {};
let _currentLang = localStorage.getItem('qp_lang') || navigator.language.split('-')[0] || 'en';
const _supportedLangs = ['en', 'zh', 'ja', 'ko', 'es'];

async function loadTranslations(lang) {
    if (!_supportedLangs.includes(lang)) lang = 'en';
    if (_i18nCache[lang]) return _i18nCache[lang];
    try {
        const r = await fetch(`/api/i18n/translations/${lang}`, { credentials: 'include' });
        if (!r.ok) throw new Error('Failed to load translations');
        const data = await r.json();
        _i18nCache[lang] = data.translations || {};
        return _i18nCache[lang];
    } catch (e) {
        console.warn('[i18n] Translation load failed:', e);
        return {};
    }
}

function t(key, fallback) {
    const parts = key.split('.');
    let current = _i18nCache[_currentLang];
    if (!current) return fallback || key;
    for (const part of parts) {
        if (current && typeof current === 'object' && part in current) {
            current = current[part];
        } else {
            return fallback || key;
        }
    }
    return typeof current === 'string' ? current : (fallback || key);
}

function applyTranslations() {
    document.querySelectorAll('[data-i18n]').forEach(el => {
        const key = el.getAttribute('data-i18n');
        const fallback = el.getAttribute('data-i18n-fallback') || el.textContent;
        const translation = t(key, fallback);
        if (el.tagName === 'INPUT' || el.tagName === 'TEXTAREA') {
            if (el.getAttribute('data-i18n-attr') === 'placeholder') {
                el.placeholder = translation;
            } else {
                el.value = translation;
            }
        } else {
            el.textContent = translation;
        }
    });
}

async function changeLanguage(lang) {
    if (!_supportedLangs.includes(lang)) return;
    _currentLang = lang;
    localStorage.setItem('qp_lang', lang);
    await loadTranslations(lang);
    applyTranslations();
    // Update nav item texts
    updateNavTexts();
    // Update page title if on a recognized page
    const activePage = document.querySelector('.page.active');
    if (activePage) {
        const pageId = activePage.id.replace('page-', '');
        const navItem = document.querySelector(`.nav-item[data-page="${pageId}"]`);
        if (navItem) {
            const titleEl = document.getElementById('page-title');
            if (titleEl) titleEl.textContent = navItem.querySelector('span')?.textContent || pageId;
        }
    }
    await refreshAll().catch(() => {});
    showToast(t('messages.updated', 'Language updated'), 'success');
}

function updateNavTexts() {
    const navMap = {
        'nav-dashboard': 'nav.dashboard',
        'nav-user': 'nav.my_trading',
        'nav-positions': 'nav.positions',
        'nav-history': 'nav.history',
        'nav-analytics': 'nav.analytics',
        'nav-charts': 'nav.charts',
        'nav-social': 'nav.social',
        'nav-backtest': 'nav.backtest',
        'nav-strategies': 'nav.strategies',
        'nav-strategy-editor': 'nav.strategy_editor',
        'nav-subscription': 'nav.subscription',
        'nav-settings': 'nav.settings',
        'nav-admin': 'nav.admin',
    };
    Object.entries(navMap).forEach(([id, key]) => {
        const el = document.getElementById(id);
        if (el) {
            const span = el.querySelector('span');
            if (span) {
                const fallback = span.getAttribute('data-i18n-fallback') || span.textContent;
                span.textContent = t(key, fallback);
            }
        }
    });
}

async function initI18n() {
    const langSelect = document.getElementById('lang-select');
    if (langSelect) {
        langSelect.value = _currentLang;
        document.getElementById('language-selector').style.display = '';
    }
    await loadTranslations(_currentLang);
    applyTranslations();
    updateNavTexts();
}

// ─── Auth Helper ───
// Token lives in httpOnly cookie managed by the server.
// We keep a lightweight in-memory user profile fetched via /api/auth/me.
// IMPORTANT: Sync with QP.Auth namespace to avoid state inconsistencies
let _cachedUser = null;
let _sessionRedirecting = false;

async function ensureUser() {
    // Sync with QP.Auth._cachedUser if available (qp-core.js loaded)
    if (window.QP && QP.Auth && QP.Auth._cachedUser) {
        _cachedUser = QP.Auth._cachedUser; // Sync local cache
        return _cachedUser;
    }
    // Fallback: fetch from API
    if (_cachedUser) return _cachedUser;
    try {
        const r = await fetch('/api/auth/me', { credentials: 'include', cache: 'no-store' });
        if (!r.ok) return null;
        _cachedUser = await r.json();
        // Sync back to QP.Auth if available
        if (window.QP && QP.Auth) {
            QP.Auth._cachedUser = _cachedUser;
        }
        return _cachedUser;
    } catch { return null; }
}

function getUser() {
    // Sync with QP.Auth if available
    if (window.QP && QP.Auth) {
        return QP.Auth.getUser();
    }
    return _cachedUser || {};
}

function isAdmin() {
    // Sync with QP.Auth if available
    if (window.QP && QP.Auth) {
        return QP.Auth.isAdmin();
    }
    return getUser().role === 'admin';
}

async function requireAuth() {
    const user = await ensureUser();
    if (!user) {
        redirectToLogin('expired');
        return false;
    }
    return true;
}

function redirectToLogin(reason = 'expired') {
    if (_sessionRedirecting) return;
    _sessionRedirecting = true;
    _cachedUser = null;
    // Sync with QP.Auth if available
    if (window.QP && QP.Auth) {
        QP.Auth._cachedUser = null;
        QP.Auth._sessionRedirecting = true;
    }
    const query = reason ? `?${encodeURIComponent(reason)}=1` : '';
    window.location.replace(`/login${query}`);
}

async function logout() {
    try {
        const csrf = getCookie('tvss_csrf');
        await fetch('/api/auth/logout', {
            method: 'POST',
            credentials: 'include',
            headers: csrf ? { 'X-CSRF-Token': decodeURIComponent(csrf) } : {},
        });
    } catch {}
    redirectToLogin('logout');
}

function getCookie(name) {
    const prefix = `${name}=`;
    return document.cookie.split(';').map(v => v.trim()).find(v => v.startsWith(prefix))?.slice(prefix.length) || '';
}

// ─── Initialization ───
document.addEventListener('DOMContentLoaded', async () => {
    if (!await requireAuth()) return;
    await initI18n();
    setupNavigation();
    setupExchangeToggle();
    detectWebhookUrl();
    updateUserUI();
    setupRealtimeStatus();
    setupSpotlight();
    _launchContext = parseLaunchContext();
    const hashPage = _launchContext.page || (window.location.hash ? window.location.hash.slice(1) : '');
    const initialPage = document.getElementById(`page-${hashPage}`)
        ? hashPage
        : (isAdmin() ? 'dashboard' : 'user');
    switchPage(initialPage);
});

function parseLaunchContext() {
    const params = new URLSearchParams(window.location.search || '');
    const hashPage = window.location.hash ? window.location.hash.slice(1) : '';
    return {
        page: hashPage,
        title: params.get('title') || '',
        text: params.get('text') || '',
        url: params.get('url') || '',
        data: params.get('data') || '',
    };
}

function clearLaunchQuery() {
    if (!window.location.search) return;
    const nextHash = window.location.hash || '';
    history.replaceState(null, '', `${window.location.pathname}${nextHash}`);
}

function getPendingProtocolSignal() {
    try {
        return sessionStorage.getItem('qp_protocol_signal') || '';
    } catch {
        return '';
    }
}

function sanitizeTickerSymbol(value) {
    return String(value || '').toUpperCase().replace(/[^A-Z0-9./:_-]/g, '').trim();
}

function applySocialLaunchContext() {
    if (!_launchContext) return;
    const sharedTitle = (_launchContext.title || '').trim();
    const sharedText = (_launchContext.text || '').trim();
    const sharedUrl = (_launchContext.url || '').trim();
    if (!sharedTitle && !sharedText && !sharedUrl) return;

    const combinedText = [sharedTitle, sharedText].filter(Boolean).join(' ').trim();
    const tickerMatch = combinedText.toUpperCase().match(/\b([A-Z0-9]{2,20}(?:USDT|USDC|BUSD|USD|BTC|ETH|BNB)(?:\.P)?)\b/);
    const entryMatch = combinedText.match(/(?:entry|price)\s*[:=@-]?\s*(\d+(?:\.\d+)?)/i);
    const directionMatch = combinedText.match(/\b(long|short)\b/i);

    const ticker = sanitizeTickerSymbol(tickerMatch ? tickerMatch[1] : '');
    if (ticker) setFieldValue('social-ticker', ticker);
    if (directionMatch) setFieldValue('social-direction', String(directionMatch[1]).toLowerCase());
    if (entryMatch) setFieldValue('social-entry', entryMatch[1]);

    const reasonParts = [];
    if (sharedTitle) reasonParts.push(sharedTitle);
    if (sharedText) reasonParts.push(sharedText);
    if (sharedUrl) reasonParts.push(sharedUrl);
    if (reasonParts.length) {
        setFieldValue('social-reason', reasonParts.join('\n').trim());
    }

    showToast('Shared content imported into the signal form.', 'info', 'Share Target');
    _launchContext.title = '';
    _launchContext.text = '';
    _launchContext.url = '';
    clearLaunchQuery();
}

function applyProtocolLaunchContext() {
    if (!_launchContext?.data) return;
    const payload = String(_launchContext.data || '').trim();
    if (!payload) return;
    try {
        sessionStorage.setItem('qp_protocol_signal', payload);
    } catch {}
    showToast('Protocol signal payload imported into the dashboard.', 'info', 'Protocol Launch');
    _launchContext.data = '';
    clearLaunchQuery();
}

function setupSpotlight() {
    document.addEventListener('mousemove', e => {
        document.querySelectorAll('.card, .chart-card, .kpi-card, .plan-card, .option-card').forEach(card => {
            const rect = card.getBoundingClientRect();
            const x = e.clientX - rect.left;
            const y = e.clientY - rect.top;
            card.style.setProperty('--mouse-x', `${x}px`);
            card.style.setProperty('--mouse-y', `${y}px`);
        });
    }, { passive: true });
}

function updateUserUI() {
    const user = getUser();
    const usernameEl = document.getElementById('user-display-name');
    if (usernameEl) usernameEl.textContent = user.username || 'User';
    const roleEl = document.getElementById('user-role-badge');
    if (roleEl) {
        roleEl.textContent = user.role === 'admin' ? 'Admin' : 'User';
        roleEl.className = `role-badge ${user.role === 'admin' ? 'admin' : 'user'}`;
    }
    document.querySelectorAll('.admin-only').forEach(el => {
        el.style.display = isAdmin() ? '' : 'none';
    });
    document.querySelectorAll('.user-only').forEach(el => {
        el.style.display = ''; // user-only items always visible (My Trading)
    });
    // Hide advanced pages from regular users
    ['dashboard','positions','history','analytics','settings','strategies','strategy-editor','social'].forEach(page => {
        const el = document.querySelector(`.nav-item[data-page="${page}"]`);
        if (el && !isAdmin()) el.style.display = 'none';
    });
}

function setServerStatus(label, state = 'offline') {
    const status = document.getElementById('server-status');
    if (!status) return;
    status.dataset.state = state;
    const labelEl = status.querySelector('span:last-child');
    if (labelEl) labelEl.textContent = label;
}

function wsUrl(path) {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    return `${protocol}//${window.location.host}${path}`;
}

function closeSocket(socket) {
    if (!socket) return;
    try {
        socket.onopen = null;
        socket.onmessage = null;
        socket.onerror = null;
        socket.onclose = null;
        socket.close();
    } catch {}
}

function setupRealtimeStatus() {
    setServerStatus(navigator.onLine ? 'Realtime standby' : 'Offline', navigator.onLine ? 'connecting' : 'offline');
    window.addEventListener('online', () => {
        setServerStatus('Realtime standby', 'connecting');
        connectSystemSocket();
    });
    window.addEventListener('offline', () => {
        setServerStatus('Offline', 'offline');
        closeSocket(_systemSocket);
        _systemSocket = null;
    });
    connectSystemSocket();
}

function connectSystemSocket() {
    if (!navigator.onLine || _systemSocket) return;
    setServerStatus('Connecting realtime...', 'connecting');
    const socket = new WebSocket(wsUrl('/ws/positions'));
    _systemSocket = socket;

    socket.onopen = () => {
        setServerStatus('Realtime connected', 'online');
        try {
            socket.send(JSON.stringify({ type: 'subscribe', channels: ['positions'] }));
        } catch {}
    };

    socket.onmessage = (event) => {
        let message = null;
        try {
            message = JSON.parse(event.data);
        } catch {
            return;
        }
        if (message?.type === 'connected' || message?.type === 'subscribed' || message?.type === 'pong') {
            setServerStatus('Realtime connected', 'online');
            return;
        }
        if (['position_update', 'position_closed', 'trade_executed'].includes(message?.type)) {
            setServerStatus('Realtime live', 'online');
            const page = document.querySelector('.page.active')?.id?.replace('page-', '');
            if (page === 'positions') { loadPositions(); loadPendingOrders(); }
            if (page === 'dashboard') loadRecentSignals();
        }
    };

    socket.onerror = () => {
        setServerStatus('Realtime unavailable', 'offline');
    };

    socket.onclose = () => {
        if (_systemSocket === socket) _systemSocket = null;
        if (!navigator.onLine) {
            setServerStatus('Offline', 'offline');
            return;
        }
        setServerStatus('Realtime retrying...', 'connecting');
        setTimeout(() => {
            if (!_systemSocket && navigator.onLine) connectSystemSocket();
        }, 5000);
    };
}

function teardownPriceSocket() {
    closeSocket(_priceSocket);
    _priceSocket = null;
    _priceSocketTicker = '';
}

function connectPriceSocket(ticker) {
    if (!ticker || !navigator.onLine) return;
    if (_priceSocket && _priceSocketTicker === ticker) return;
    teardownPriceSocket();
    _priceSocketTicker = ticker;
    const socket = new WebSocket(wsUrl('/ws/prices'));
    _priceSocket = socket;

    socket.onopen = () => {
        try {
            socket.send(JSON.stringify({ type: 'subscribe_tickers', tickers: [ticker] }));
        } catch {}
    };

    socket.onmessage = (event) => {
        let message = null;
        try {
            message = JSON.parse(event.data);
        } catch {
            return;
        }
        if (message?.type !== 'price_update' || String(message.ticker || '').toUpperCase() !== _priceSocketTicker) return;

        _chartRealtimeState = message;
        setText('chart-price', message.price ? `$${formatNum(message.price)}` : '--');
        const changeValue = Number(message.change_1h_pct || 0);
        const changeEl = document.getElementById('chart-change');
        if (changeEl) {
            changeEl.textContent = `${changeValue >= 0 ? '+' : ''}${changeValue.toFixed(2)}%`;
            changeEl.className = `kpi-value ${changeValue >= 0 ? 'pnl-positive' : 'pnl-negative'}`;
        }
        setText('chart-rsi', formatValue(message.rsi_1h));
        setText('chart-volume', formatCompact(message.volume_24h));
    };

    socket.onclose = () => {
        if (_priceSocket === socket) _priceSocket = null;
        const activePage = document.querySelector('.page.active')?.id?.replace('page-', '');
        if (activePage === 'charts' && navigator.onLine && _priceSocketTicker === ticker) {
            setTimeout(() => {
                if (!_priceSocket && _priceSocketTicker === ticker) connectPriceSocket(ticker);
            }, 5000);
        }
    };
}

// ─── Toast ───
function escapeHtml(str) {
    return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function safeClassToken(str) {
    return String(str || '').toLowerCase().replace(/[^a-z0-9_-]/g, '');
}
function escapeJsSingle(str) {
    return String(str || '')
        .replace(/\\/g, '\\\\')
        .replace(/'/g, "\\'")
        .replace(/"/g, '&quot;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/\r?\n/g, ' ');
}
function getCookie(name) {
    const prefix = `${name}=`;
    return document.cookie.split(';').map(v => v.trim()).find(v => v.startsWith(prefix))?.slice(prefix.length) || '';
}
function copyText(text, label = 'Copied') {
    navigator.clipboard.writeText(text).then(() => showToast(label, 'success'));
}
function showToast(message, type = 'info', title = '') {
    const container = document.getElementById('toast-container');
    if (!container) return;
    const icons = { success:'ri-checkbox-circle-line', error:'ri-error-warning-line', warning:'ri-alert-line', info:'ri-information-line' };
    const defaultTitles = { success:'Success', error:'Error', warning:'Warning', info:'Info' };
    const safeTitle = escapeHtml(title || defaultTitles[type] || 'Notice');
    const safeMessage = message ? escapeHtml(message) : '';
    const toast = document.createElement('div');
    toast.className = `toast ${type}`;
    toast.setAttribute('role','alert');
    toast.innerHTML = `<i class="toast-icon ${icons[type]||icons.info}"></i><div class="toast-body"><div class="toast-title">${safeTitle}</div>${safeMessage?`<div class="toast-msg">${safeMessage}</div>`:''}</div>`;
    container.appendChild(toast);
    const dismiss = () => { toast.classList.add('removing'); toast.addEventListener('animationend', () => toast.remove(), {once:true}); };
    setTimeout(dismiss, 4000);
    toast.addEventListener('click', dismiss);
}

// ─── Navigation ───
function setupNavigation() {
    document.querySelectorAll('.nav-item[data-page]').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            switchPage(item.dataset.page);
            closeSidebar();
        });
    });
    document.getElementById('menu-toggle')?.addEventListener('click', () => {
        document.getElementById('sidebar')?.classList.toggle('open');
        document.getElementById('sidebar-overlay')?.classList.toggle('visible');
    });
    document.getElementById('sidebar-overlay')?.addEventListener('click', closeSidebar);
}

function closeSidebar() {
    document.getElementById('sidebar')?.classList.remove('open');
    document.getElementById('sidebar-overlay')?.classList.remove('visible');
}

function switchPage(page) {
    // Block non-admin from admin-only pages
    if (!isAdmin() && (page === 'backtest' || page === 'admin' || page === 'settings' || page === 'dashboard' || page === 'positions' || page === 'history' || page === 'analytics' || page === 'strategies' || page === 'strategy-editor' || page === 'social')) {
        page = 'user';
    }
    document.querySelectorAll('.nav-item').forEach(n => { n.classList.remove('active'); n.removeAttribute('aria-current'); });
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    const navEl = document.querySelector(`[data-page="${page}"]`);
    navEl?.classList.add('active');
    navEl?.setAttribute('aria-current','page');
    document.getElementById(`page-${page}`)?.classList.add('active');
    if (page !== 'charts') teardownPriceSocket();
    const titles = {
        dashboard: t('nav.dashboard', 'Dashboard'),
        user: t('nav.my_trading', 'My Trading'),
        positions: t('nav.positions', 'Positions'),
        history: t('nav.history', 'Trade History'),
        analytics: t('nav.analytics', 'Analytics'),
        charts: t('nav.charts', 'Charts'),
        social: t('nav.social', 'Signals'),
        settings: t('nav.settings', 'Settings'),
        subscription: t('nav.subscription', 'Subscription'),
        admin: t('nav.admin', 'Admin Panel'),
        backtest: t('nav.backtest', 'Backtest'),
        strategies: t('nav.strategies', 'Strategies'),
        'strategy-editor': t('nav.strategy_editor', 'Editor'),
    };
    document.getElementById('page-title').textContent = titles[page] || page;
    if (window.location.hash !== `#${page}`) {
        history.replaceState(null, '', `${window.location.pathname}#${page}`);
    }
    if (page === 'dashboard') applyProtocolLaunchContext();
    if (page === 'dashboard') loadDashboard();
    if (page === 'positions') { loadPositions(); loadPendingOrders(); }
    if (page === 'history') loadHistory();
    if (page === 'analytics') loadAnalytics();
    if (page === 'charts') loadChartPage();
    if (page === 'social') applySocialLaunchContext();
    if (page === 'social') loadSocialPage();
    if (page === 'settings') loadSettings();
    if (page === 'user') loadUserPortal();
    if (page === 'subscription') loadSubscription();
    if (page === 'admin') loadAdmin();
    if (page === 'strategies') {
        loadStrategiesOverview();
        loadDCAList();
        loadGridList();
        loadStrategyHistory();
    }
    if (page === 'strategy-editor') loadStrategyEditorPage();
    // Re-apply translations after page switch so content pages get translated
    applyTranslations();
}

// ─── Dashboard ───
async function loadDashboard() {
    try {
        const [status, perf, strategyOverview] = await Promise.all([
            fetchAPI('/api/status'),
            fetchAPI('/api/performance?days=30'),
            fetchAPI('/api/strategies/overview').catch(() => null),
        ]);
        if (status.live_trading) {
            const el = document.getElementById('trading-mode');
            el.innerHTML = `<span class="mode-dot live"></span><span>${status.exchange_sandbox_mode ? 'Sandbox Trading' : 'LIVE Trading'}</span>`;
            el.style.background = 'var(--accent-red-bg)';
            el.style.color = 'var(--accent-red)';
        }
        setText('dash-api-health', 'API Online');
        setText('dash-risk-mode', status.live_trading ? 'Live Risk Guard' : 'Paper Risk Guard');
        setText('dash-webhook-state', status.webhook_configured === false ? 'Webhook Needs Setup' : 'Webhook Ready');
        const pnl = perf.total_pnl_pct || 0;
        const pnlEl = document.getElementById('kpi-pnl');
        pnlEl.textContent = `${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}%`;
        pnlEl.className = `kpi-value ${pnl >= 0 ? 'pnl-positive' : 'pnl-negative'}`;
        document.getElementById('kpi-trades').textContent = perf.total_trades || 0;
        document.getElementById('kpi-winrate').textContent = `${(perf.win_rate || 0).toFixed(1)}%`;
        document.getElementById('kpi-sharpe').textContent = (perf.sharpe_ratio || 0).toFixed(2);
        renderDashboardBrief(perf, strategyOverview, status);
        renderMetrics(perf);
        renderEquityChart(perf.equity_curve || []);
        await loadRecentSignals();
        checkSystemHealth();
    } catch (err) {
console.error('Dashboard load error:', err);
    }
}

async function checkSystemHealth() {
    try {
        const health = await fetchAPI('/health/quick');
        const chipEl = document.getElementById('system-health-chip');
        const healthTextEl = document.getElementById('dash-system-health');

        if (health.status === 'healthy') {
            if (chipEl) chipEl.className = 'status-chip';
            if (healthTextEl) healthTextEl.textContent = 'System Healthy';
        } else if (health.status === 'degraded') {
            if (chipEl) chipEl.className = 'status-chip warning';
            if (healthTextEl) healthTextEl.textContent = 'System Degraded';
        } else {
            if (chipEl) chipEl.className = 'status-chip error';
            if (healthTextEl) healthTextEl.textContent = 'System Issues';
        }
    } catch (err) {
        const chipEl = document.getElementById('system-health-chip');
        const healthTextEl = document.getElementById('dash-system-health');
        if (chipEl) chipEl.className = 'status-chip error';
        if (healthTextEl) healthTextEl.textContent = 'Health Check Failed';
    }
}

function renderDashboardBrief(perf = {}, overview = {}, status = {}) {
    const pnl = Number(perf.total_pnl_pct || 0);
    const win = Number(perf.win_rate || 0);
    const confidence = Math.max(0, Math.min(99, Math.round((win * 0.55) + (pnl > 0 ? 25 : 12) + Math.min(Number(perf.sharpe_ratio || 0) * 8, 16))));
    setText('ai-confidence', confidence ? `${confidence}%` : '--');
    setText('ai-brief-title', pnl >= 0 ? 'Constructive Market Posture' : 'Defensive Market Posture');
    setText(
        'ai-brief-text',
        pnl >= 0
            ? `Portfolio trend is positive over the selected window. Keep execution gated by risk controls and review any low-confidence signal before automation.`
            : `Performance is under pressure. Prefer paper mode, smaller sizing, and manual review until drawdown and win-rate stabilize.`
    );
    setText('dash-dca-active', overview?.dca?.active_count ?? '--');
    setText('dash-grid-active', overview?.grid?.active_count ?? '--');
    const botPnl = Number((overview?.dca?.total_pnl || 0) + (overview?.grid?.total_pnl || 0));
    setText('dash-bot-pnl', Number.isFinite(botPnl) ? `$${formatNum(botPnl)}` : '--');

    const queue = document.getElementById('dashboard-action-queue');
    if (!queue) return;
    const items = [
        {
            icon: status.live_trading ? 'ri-alarm-warning-line' : 'ri-shield-check-line',
            title: status.live_trading ? 'Live trading enabled' : 'Paper trading active',
            text: status.live_trading ? 'Confirm exchange keys, leverage, and emergency stop readiness.' : 'Execution is isolated from real funds.',
        },
        {
            icon: 'ri-robot-2-line',
            title: `${overview?.dca?.active_count || 0} DCA / ${overview?.grid?.active_count || 0} Grid bots`,
            text: 'Automation health is tracked from persisted strategy state.',
        },
        {
            icon: pnl >= 0 ? 'ri-line-chart-line' : 'ri-arrow-down-circle-line',
            title: pnl >= 0 ? 'Performance stable' : 'Performance needs review',
            text: `Win rate ${win.toFixed(1)}%, Sharpe ${(Number(perf.sharpe_ratio || 0)).toFixed(2)}.`,
        },
    ];
    const protocolSignal = getPendingProtocolSignal();
    if (protocolSignal) {
        items.unshift({
            icon: 'ri-radar-line',
            title: 'Protocol signal imported',
            text: protocolSignal.length > 120 ? `${protocolSignal.slice(0, 117)}...` : protocolSignal,
        });
    }
    queue.innerHTML = items.map(item => `<div class="queue-item"><i class="${item.icon}"></i><div><strong>${escapeHtml(item.title)}</strong><span>${escapeHtml(item.text)}</span></div></div>`).join('');
}

function renderMetrics(perf) {
    const grid = document.getElementById('metrics-grid');
    const items = [
        ['Profit Factor', formatValue(perf.profit_factor)], ['Risk/Reward', formatValue(perf.risk_reward_ratio)],
        ['Max Drawdown', `${(perf.max_drawdown_pct||0).toFixed(2)}%`], ['Sortino Ratio', (perf.sortino_ratio||0).toFixed(2)],
        ['Best Trade', `${(perf.best_trade_pct||0).toFixed(2)}%`], ['Worst Trade', `${(perf.worst_trade_pct||0).toFixed(2)}%`],
        ['Consec. Wins', perf.max_consecutive_wins||0], ['Consec. Losses', perf.max_consecutive_losses||0],
    ];
    grid.innerHTML = items.map(([l,v]) => `<div class="metric-item"><span class="metric-label">${l}</span><span class="metric-value">${v}</span></div>`).join('');
}

async function loadRecentSignals() {
    try {
        const trades = await fetchAPI('/api/trades');
        const container = document.getElementById('recent-signals');
        if (!trades.length) { container.innerHTML = '<div class="empty-state" style="padding:40px;text-align:center;color:var(--text-muted)">No signals today</div>'; return; }
        container.innerHTML = trades.slice(-20).reverse().map(t => {
            const dir = t.direction || 'long', isLong = dir.includes('long');
            const conf = t.ai?.confidence || 0, time = t.timestamp ? new Date(t.timestamp).toLocaleTimeString() : '--';
            return `<div class="signal-item"><div class="signal-icon ${isLong?'long':'short'}"><i class="ri-arrow-${isLong?'up':'down'}-line"></i></div><div class="signal-info"><div class="signal-ticker">${escapeHtml(t.ticker||'--')}</div><div class="signal-detail">${escapeHtml(time)} · ${escapeHtml(dir.toUpperCase())}</div></div><div class="signal-conf ${conf>=0.7?'pnl-positive':conf<0.5?'pnl-negative':''}">${(conf*100).toFixed(0)}%</div></div>`;
        }).join('');
    } catch (e) { console.error('Failed to load signals:', e); }
}

function setChartPeriod(evt, days) {
    document.querySelectorAll('.card-actions .btn-sm').forEach(b => b.classList.remove('active'));
    evt.target.classList.add('active');
    fetchAPI(`/api/performance?days=${days}`).then(perf => {
        if (typeof renderEquityChart === 'function') {
            renderEquityChart(perf.equity_curve || []);
        }
    }).catch(e => showToast(e.message, 'error'));
}

// ─── Positions ───
async function loadPositions() {
    try {
        const [positions, balance] = await Promise.all([fetchAPI('/api/positions'), fetchAPI('/api/balance')]);
        const tbody = document.getElementById('positions-body');
        const badge = document.getElementById('open-count-badge');
        if (badge) badge.textContent = positions.length;
        if (!positions.length) { tbody.innerHTML = '<tr><td colspan="11" class="empty-state">No open positions</td></tr>'; }
else { tbody.innerHTML = positions.map(p => {
            const entry = firstDefined(p.entry_price, p.entryPrice);
            const mark = firstDefined(p.mark_price, p.markPrice);
            const liq = firstDefined(p.liquidation_price, p.liquidationPrice);
            const margin = firstDefined(p.margin, null);
            const pnl = Number(firstDefined(p.unrealized_pnl, p.unrealizedPnl, 0));
            const pct = firstDefined(p.percentage, null);
            const pctText = pct == null ? '--' : `${Number(pct) >= 0 ? '+' : ''}${Number(pct).toFixed(2)}%`;
            const mode = p.source === 'exchange_live' ? 'Exchange' : (p.mode || 'paper');
            const statusText = p.status_text ? `<div class="hint">${escapeHtml(p.status_text)}</div>` : (p.status ? `<div class="hint">${escapeHtml(String(p.status).toUpperCase())}</div>` : '');
            const symbolDisplay = p.symbol_short || p.symbol || '--';
            const posId = p.id || '';
            const closeBtn = posId ? `
                <button class="btn btn-sm btn-danger" onclick="closePosition('${escapeHtml(posId)}', '${escapeHtml(symbolDisplay)}')" title="Close position">
                    <i class="ri-close-line"></i>
                </button>
                <button class="btn btn-sm btn-warning" onclick="closePositionPartial('${escapeHtml(posId)}', '${escapeHtml(symbolDisplay)}')" title="Close 50%">
                    <i class="ri-percent-line"></i>
                </button>
            ` : '';
            return `<tr><td><strong>${escapeHtml(symbolDisplay)}</strong><div class="hint">${escapeHtml(mode)}</div>${statusText}</td><td><span class="badge ${p.side==='long'?'badge-long':'badge-short'}">${escapeHtml(p.side||'--')}</span></td><td>${escapeHtml(p.contracts)}</td><td>$${formatNum(entry)}</td><td>$${formatNum(mark)}</td><td>${liq?'$'+formatNum(liq):'--'}</td><td>${margin?'$'+formatNum(margin):'--'}</td><td class="${pnl>=0?'pnl-positive':'pnl-negative'}">$${formatNum(pnl)}</td><td class="${pct == null || Number(pct)>=0?'pnl-positive':'pnl-negative'}">${pctText}</td><td>${escapeHtml(p.leverage||'--')}x</td><td>${closeBtn}</td></tr>`;
        }).join(''); }
        document.getElementById('bal-total').textContent = `$${formatNum(balance.total_quote ?? pickBalance(balance.total, balance.quote))}`;
        document.getElementById('bal-free').textContent = `$${formatNum(balance.free_quote ?? pickBalance(balance.free, balance.quote))}`;
        document.getElementById('bal-used').textContent = `$${formatNum(balance.used_quote ?? pickBalance(balance.used, balance.quote))}`;
    } catch (err) { showToast(err.message, 'error', 'Positions Load Failed'); }
}

async function loadPendingOrders() {
    try {
        const orders = await fetchAPI('/api/pending-orders');
        const tbody = document.getElementById('pending-orders-body');
        const badge = document.getElementById('pending-count-badge');
        if (badge) badge.textContent = orders.length;
        if (!orders.length) {
            tbody.innerHTML = '<tr><td colspan="8" class="empty-state">No pending orders</td></tr>';
            return;
        }
        tbody.innerHTML = orders.map(o => {
            const time = o.timestamp || o.datetime ? new Date(o.timestamp || o.datetime).toLocaleString() : '--';
            const price = o.price ? `$${formatNum(o.price)}` : 'Market';
            return `<tr>
                <td><strong>${escapeHtml(o.symbol || '--')}</strong></td>
                <td><span class="badge ${o.side === 'buy' ? 'badge-long' : 'badge-short'}">${escapeHtml(o.side || '--')}</span></td>
                <td>${escapeHtml(o.type || '--')}</td>
                <td>${escapeHtml(o.amount ? formatNum(o.amount) : '--')}</td>
                <td>${price}</td>
                <td>${escapeHtml(time)}</td>
                <td><span class="badge badge-warning">${escapeHtml(o.status || 'open')}</span></td>
                <td><button class="btn btn-sm btn-danger" onclick="cancelPendingOrder('${escapeHtml(o.id)}', '${escapeHtml(o.symbol)}')"><i class="ri-close-line"></i></button></td>
            </tr>`;
        }).join('');
    } catch (err) { showToast(err.message, 'error', 'Pending Orders Load Failed'); }
}

async function cancelPendingOrder(orderId, symbol) {
    if (!confirm(`Cancel order ${orderId} for ${symbol}?`)) return;
    try {
        await fetchAPI(`/api/cancel-order?order_id=${orderId}&symbol=${symbol}`, { method: 'POST' });
        showToast('Order cancelled', 'success');
        await loadPendingOrders();
    } catch (err) { showToast(err.message, 'error', 'Cancel Failed'); }
}

async function cancelAllPendingOrders() {
    if (!confirm('Cancel ALL pending orders?')) return;
    try {
        const orders = await fetchAPI('/api/pending-orders');
        await Promise.all(orders.map(o => fetchAPI(`/api/cancel-order?order_id=${o.id}&symbol=${o.symbol}`, { method: 'POST' })));
        showToast(`Cancelled ${orders.length} orders`, 'success');
        await loadPendingOrders();
    } catch (err) { showToast(err.message, 'error', 'Cancel All Failed'); }
}

async function closePosition(positionId, symbol) {
    if (!confirm(`Close entire position for ${symbol}?\nPosition ID: ${positionId}`)) return;
    try {
        const result = await fetchAPI(`/api/positions/${encodeURIComponent(positionId)}/close`, { method: 'POST' });
        showToast(`Position closed: ${symbol} (PnL: ${result.pnl_pct ? result.pnl_pct.toFixed(2) : 'N/A'}%)`, 'success', 'Position Closed');
        await loadPositions();
    } catch (err) { showToast(err.message, 'error', 'Close Failed'); }
}

async function closePositionPartial(positionId, symbol) {
    const pct = prompt('Close percentage (1-100):', '50');
    if (!pct || isNaN(pct) || pct < 1 || pct > 100) return;
    try {
        const result = await fetchAPI(`/api/positions/${encodeURIComponent(positionId)}/close?close_pct=${pct}`, { method: 'POST' });
        showToast(`Partial close: ${symbol} ${pct}% (${result.close_qty} contracts)`, 'success', 'Partial Close');
        await loadPositions();
    } catch (err) { showToast(err.message, 'error', 'Partial Close Failed'); }
}

async function closeAllPositions() {
    if (!confirm('Close ALL open positions?\nThis will close both paper and live positions.')) return;
    try {
        const result = await fetchAPI('/api/positions/close-all', { method: 'POST' });
        if (result.errors && result.errors.length > 0) {
            showToast(`Closed ${result.closed}/${result.total} positions. Errors: ${result.errors.join(', ')}`, 'warning', 'Partial Close');
        } else {
            showToast(`All ${result.closed} positions closed`, 'success', 'Positions Closed');
        }
        await loadPositions();
    } catch (err) { showToast(err.message, 'error', 'Close All Failed'); }
}

// ─── History ───
let _historyAllTrades = [];
let _historyPage = 1;

async function loadHistory() {
    try {
        const days = document.getElementById('history-days')?.value || 30;
        const trades = await fetchAPI(`/api/history?days=${days}`);
        _historyAllTrades = trades;
        _historyPage = 1;
        renderHistoryPage();
    } catch (err) { showToast(err.message, 'error', 'History Load Failed'); }
}

function renderHistoryPage() {
    const trades = _historyAllTrades;
    const tbody = document.getElementById('history-body');
    if (!trades.length) { tbody.innerHTML = '<tr><td colspan="9" class="empty-state">No trades found</td></tr>'; document.getElementById('history-pagination-top').innerHTML = ''; document.getElementById('history-pagination-bottom').innerHTML = ''; return; }
    const pageSize = parseInt(document.getElementById('history-page-size')?.value || '100', 10);
    const totalPages = Math.max(1, Math.ceil(trades.length / pageSize));
    if (_historyPage > totalPages) _historyPage = totalPages;
    const start = (_historyPage - 1) * pageSize;
    const pageTrades = trades.slice(start, start + pageSize);
    tbody.innerHTML = pageTrades.map(t => {
        const dir = t.direction||'--', isLong = dir.includes('long'), conf = t.ai?.confidence||0;
        const status = t.order_status||t.status||'--', pnl = t.pnl_pct||0;
        const time = t.timestamp ? new Date(t.timestamp).toLocaleString() : '--';
        const statusClass = safeClassToken(status);
        const leverage = t.ai?.recommended_leverage ? ` / ${Number(t.ai.recommended_leverage).toFixed(1)}x` : '';
        const tpText = Array.isArray(t.take_profit_levels) && t.take_profit_levels.length
            ? formatTakeProfitLevels(t.take_profit_levels)
            : (t.take_profit ? '$'+formatNum(t.take_profit) : '--');
        return `<tr><td>${escapeHtml(time)}</td><td><strong>${escapeHtml(t.ticker||'--')}</strong></td><td><span class="badge ${isLong?'badge-long':'badge-short'}">${escapeHtml(dir)}</span></td><td>${t.entry_price?'$'+formatNum(t.entry_price):'--'}</td><td>${t.stop_loss?'$'+formatNum(t.stop_loss):'--'}</td><td>${tpText}</td><td>${(conf*100).toFixed(0)}%${escapeHtml(leverage)}</td><td><span class="badge badge-${statusClass}">${escapeHtml(status)}</span></td><td class="${pnl>=0?'pnl-positive':'pnl-negative'}">${pnl?pnl.toFixed(2)+'%':'--'}</td></tr>`;
    }).join('');
    const topEl = document.getElementById('history-pagination-top');
    if (topEl) topEl.innerHTML = `<span>${trades.length} trades total</span><span>Page ${_historyPage} of ${totalPages}</span>`;
    const botEl = document.getElementById('history-pagination-bottom');
    if (!botEl) return;
    let btns = '';
    if (_historyPage > 1) btns += `<button class="btn btn-sm" onclick="_historyPage--;renderHistoryPage()"><i class="ri-arrow-left-s-line"></i> Prev</button>`;
    const maxButtons = 7;
    let startP = Math.max(1, _historyPage - 3);
    let endP = Math.min(totalPages, startP + maxButtons - 1);
    if (endP - startP < maxButtons - 1) startP = Math.max(1, endP - maxButtons + 1);
    for (let p = startP; p <= endP; p++) {
        btns += `<button class="btn btn-sm${p === _historyPage ? ' active' : ''}" onclick="_historyPage=${p};renderHistoryPage()">${p}</button>`;
    }
    if (_historyPage < totalPages) btns += `<button class="btn btn-sm" onclick="_historyPage++;renderHistoryPage()">Next <i class="ri-arrow-right-s-line"></i></button>`;
    botEl.innerHTML = btns;
}

// ─── Analytics ───
async function loadAnalytics() {
    try {
        const [perf, daily] = await Promise.all([fetchAPI('/api/performance?days=30'), fetchAPI('/api/daily-pnl?days=30')]);
        document.getElementById('an-pf').textContent = formatValue(perf.profit_factor);
        document.getElementById('an-dd').textContent = `${(perf.max_drawdown_pct||0).toFixed(2)}%`;
        document.getElementById('an-rr').textContent = formatValue(perf.risk_reward_ratio);
        document.getElementById('an-sortino').textContent = (perf.sortino_ratio||0).toFixed(2);
        renderDailyPnlChart(daily);
        renderWinLossChart(perf);
        const metrics = [['Total P&L',`${(perf.total_pnl_pct||0).toFixed(2)}%`],['Win Rate',`${(perf.win_rate||0).toFixed(1)}%`],['Total Trades',perf.total_trades||0],['Avg Win',`${(perf.avg_win_pct||0).toFixed(2)}%`],['Avg Loss',`${(perf.avg_loss_pct||0).toFixed(2)}%`],['Sharpe',(perf.sharpe_ratio||0).toFixed(2)],['Sortino',(perf.sortino_ratio||0).toFixed(2)],['Max DD',`${(perf.max_drawdown_pct||0).toFixed(2)}%`],['Profit Factor',formatValue(perf.profit_factor)],['Best Trade',`${(perf.best_trade_pct||0).toFixed(2)}%`],['Worst Trade',`${(perf.worst_trade_pct||0).toFixed(2)}%`],['Consec. Wins',perf.max_consecutive_wins||0]];
        document.getElementById('detailed-metrics').innerHTML = metrics.map(([l,v]) => `<div class="metric-item"><span class="metric-label">${l}</span><span class="metric-value">${v}</span></div>`).join('');
        const ai = perf.ai_stats || {};
        const aiStatsEl = document.getElementById('ai-stats');
        if (!ai || (ai.high_confidence_trades === 0 && ai.low_confidence_trades === 0 && ai.avg_confidence === 0)) {
            aiStatsEl.innerHTML = `
                <div style="grid-column:1/-1;text-align:center;padding:32px 16px">
                    <i class="ri-robot-2-line" style="font-size:36px;color:var(--text-muted);display:block;margin-bottom:12px"></i>
                    <p style="color:var(--text-secondary);font-size:14px;margin:0 0 8px">No AI analysis data available yet</p>
                    <p style="color:var(--text-muted);font-size:13px;margin:0">AI performance metrics will appear here once trades with AI confidence scores are recorded. Enable an AI provider in Settings to get started.</p>
                </div>`;
        } else {
            aiStatsEl.innerHTML = `
                <div class="ai-stat-card"><div class="stat-label">High-Conf Win Rate</div><div class="stat-value pnl-positive">${(ai.high_confidence_win_rate||0).toFixed(1)}%</div><div class="hint">${ai.high_confidence_trades||0} trades</div></div>
                <div class="ai-stat-card"><div class="stat-label">Low-Conf Win Rate</div><div class="stat-value pnl-negative">${(ai.low_confidence_win_rate||0).toFixed(1)}%</div><div class="hint">${ai.low_confidence_trades||0} trades</div></div>
                <div class="ai-stat-card"><div class="stat-label">Avg Confidence</div><div class="stat-value">${((ai.avg_confidence||0)*100).toFixed(1)}%</div></div>
                <div class="ai-stat-card"><div class="stat-label">AI Edge</div><div class="stat-value ${(ai.high_confidence_win_rate-ai.low_confidence_win_rate)>0?'pnl-positive':'pnl-negative'}">${((ai.high_confidence_win_rate||0)-(ai.low_confidence_win_rate||0)).toFixed(1)}%</div></div>`;
        }
    } catch (err) { showToast(err.message, 'error', 'Analytics Load Failed'); }
}

// ─── Settings ───
function setupExchangeToggle() {
    const sel = document.getElementById('set-exchange');
    sel?.addEventListener('change', toggleExchangePasswordField);
    document.getElementById('set-ai-provider')?.addEventListener('change', toggleCustomAIFields);
    document.querySelectorAll('input[name="ai-risk-profile"]').forEach(el => el.addEventListener('change', toggleRiskProfileHint));
}

function toggleExchangePasswordField() {
    const exchange = document.getElementById('set-exchange')?.value;
    const group = document.getElementById('password-group');
    if (group) group.style.display = ['okx','bitget'].includes(exchange) ? 'block' : 'none';
}

function timeoutOverridesFromInputs(prefix) {
    const units = {
        '15m': 3600,
        '30m': 3600,
        '1h': 3600,
        '4h': 3600,
        '1d': 86400,
    };
    const values = {};
    Object.entries(units).forEach(([key, scale]) => {
        const el = document.getElementById(`${prefix}-limit-timeout-${key}`);
        const raw = parseFloat(el?.value || '0');
        if (Number.isFinite(raw) && raw > 0) values[key] = Math.round(raw * scale);
    });
    return values;
}

function readNumberInput(id, fallback, parser = parseFloat) {
    const el = document.getElementById(id);
    if (!el) return fallback;
    const raw = String(el.value ?? '').trim();
    if (!raw) return fallback;
    const parsed = parser(raw);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function applyTimeoutOverrideFields(prefix, overrides) {
    const values = overrides || {};
    setFieldValue(`${prefix}-limit-timeout-15m`, Number(values['15m'] || 7200) / 3600);
    setFieldValue(`${prefix}-limit-timeout-30m`, Number(values['30m'] || 14400) / 3600);
    setFieldValue(`${prefix}-limit-timeout-1h`, Number(values['1h'] || 28800) / 3600);
    setFieldValue(`${prefix}-limit-timeout-4h`, Number(values['4h'] || 172800) / 3600);
    setFieldValue(`${prefix}-limit-timeout-1d`, Number(values['1d'] || 604800) / 86400);
}

function toggleCustomAIFields() {
    const provider = document.getElementById('set-ai-provider')?.value;
    const customFields = document.getElementById('custom-ai-fields');
    const openrouterFields = document.getElementById('openrouter-ai-fields');
    const mistralFields = document.getElementById('mistral-ai-fields');
    const openaiFields = document.getElementById('openai-ai-fields');
    const anthropicFields = document.getElementById('anthropic-ai-fields');
    const deepseekFields = document.getElementById('deepseek-ai-fields');
    if (customFields) customFields.style.display = provider === 'custom' ? 'block' : 'none';
    if (openrouterFields) openrouterFields.style.display = provider === 'openrouter' ? 'block' : 'none';
    if (mistralFields) mistralFields.style.display = provider === 'mistral' ? 'block' : 'none';
    if (openaiFields) openaiFields.style.display = provider === 'openai' ? 'block' : 'none';
    if (anthropicFields) anthropicFields.style.display = provider === 'anthropic' ? 'block' : 'none';
    if (deepseekFields) deepseekFields.style.display = provider === 'deepseek' ? 'block' : 'none';
    if (currentRuntimeStatus) {
        setSecretPlaceholder(
            'set-ai-key',
            aiKeyConfiguredForProvider(currentRuntimeStatus, provider),
            'Enter AI API Key',
            aiKeyMaskedForProvider(currentRuntimeStatus, provider)
        );
    }
}

function toggleExitModeFields() {
    const mode = document.querySelector('input[name="exit-management-mode"]:checked')?.value || 'ai';
    const aiFields = document.getElementById('ai-exit-fields');
    const customFields = document.getElementById('custom-exit-fields');
    if (aiFields) aiFields.style.display = mode === 'ai' ? 'block' : 'none';
    if (customFields) customFields.style.display = mode === 'custom' ? 'block' : 'none';
}

function toggleRiskProfileHint() {
    const profile = document.querySelector('input[name="ai-risk-profile"]:checked')?.value || 'balanced';
    const hints = {
        conservative: 'Conservative: stricter filtering, 1:2 target R/R, leverage usually 1x-5x and capped at 10x.',
        balanced: 'Balanced: clean setups, 1:1.5 target R/R, leverage usually 2x-10x and capped at 20x.',
        aggressive: 'Aggressive: more momentum opportunities, 1:1.2 target R/R, leverage usually 5x-20x and capped at 50x.',
    };
    setText('ai-risk-profile-hint', `${hints[profile]} AI will include recommended_leverage in each analysis result.`);
}

function togglePositionSizingFields() {
    const mode = document.querySelector('input[name="position-sizing-mode"]:checked')?.value || 'percentage';
    const percentageFields = document.getElementById('sizing-percentage-fields');
    const fixedFields = document.getElementById('sizing-fixed-fields');
    const riskFields = document.getElementById('sizing-risk-fields');
    
    if (percentageFields) percentageFields.style.display = mode === 'percentage' ? 'block' : 'none';
    if (fixedFields) fixedFields.style.display = mode === 'fixed' ? 'block' : 'none';
    if (riskFields) riskFields.style.display = mode === 'risk_ratio' ? 'block' : 'none';
}

async function loadSettings() {
    try {
        const [status, userSettings] = await Promise.all([
            fetchAPI('/api/status'),
            fetchAPI('/api/settings').catch(() => ({})),
        ]);
        currentRuntimeStatus = status;
        const userExchange = isAdmin() ? {} : (userSettings.exchange || {});
        if (document.getElementById('set-exchange') && status.exchange) document.getElementById('set-exchange').value = status.exchange;
        setFieldValue('set-live-trading', String(Boolean(status.live_trading)));
        const sandbox = document.getElementById('set-exchange-sandbox');
        if (sandbox) sandbox.checked = Boolean(status.exchange_sandbox_mode);
        if (document.getElementById('set-exchange') && userExchange.name) document.getElementById('set-exchange').value = userExchange.name;
        setFieldValue('set-live-trading', String(Boolean(userExchange.live_trading ?? status.live_trading)));
        if (sandbox) sandbox.checked = Boolean(userExchange.sandbox_mode ?? status.exchange_sandbox_mode);
        setFieldValue('set-exchange-market-type', userExchange.market_type || status.exchange_market_type || 'contract');
        setFieldValue('set-default-order-type', userExchange.default_order_type || status.exchange_default_order_type || 'limit');
        setFieldValue('set-stop-loss-order-type', userExchange.stop_loss_order_type || status.exchange_stop_loss_order_type || 'market');
        applyTimeoutOverrideFields('set', userExchange.limit_timeout_overrides || status.exchange_limit_timeout_overrides || {});
        toggleExchangePasswordField();
        setSecretPlaceholder(
            'set-api-key',
            Boolean(userExchange.api_configured) || status.exchange_api_configured,
            'Enter API Key',
            userExchange.api_key_masked || status.exchange_api_key_masked
        );
        setSecretPlaceholder(
            'set-api-secret',
            Boolean(userExchange.api_configured) || status.exchange_api_configured,
            'Enter API Secret',
            userExchange.api_secret_masked || status.exchange_api_secret_masked
        );
        setSecretPlaceholder(
            'set-password',
            Boolean(userExchange.password_masked) || status.exchange_password_configured,
            'Enter Passphrase',
            userExchange.password_masked || status.exchange_password_masked
        );
        if (document.getElementById('set-ai-provider') && status.ai_provider) document.getElementById('set-ai-provider').value = status.ai_provider;
        const aiKeyConfigured = aiKeyConfiguredForProvider(status, status.ai_provider);
        setSecretPlaceholder('set-ai-key', aiKeyConfigured, 'Enter AI API Key', aiKeyMaskedForProvider(status, status.ai_provider));
        if (document.getElementById('set-custom-provider-enabled')) document.getElementById('set-custom-provider-enabled').checked = Boolean(status.custom_provider_enabled);
        if (document.getElementById('set-custom-provider-name')) document.getElementById('set-custom-provider-name').value = status.custom_provider_name || 'custom';
        if (document.getElementById('set-custom-provider-model')) document.getElementById('set-custom-provider-model').value = status.custom_provider_model || '';
        if (document.getElementById('set-custom-provider-url')) document.getElementById('set-custom-provider-url').value = status.custom_provider_url || '';
        if (document.getElementById('set-openrouter-model')) document.getElementById('set-openrouter-model').value = status.openrouter_model || 'openai/gpt-5.5';
        if (document.getElementById('set-mistral-model')) document.getElementById('set-mistral-model').value = status.mistral_model || 'mistral-large-latest';
        if (document.getElementById('set-openai-model')) document.getElementById('set-openai-model').value = status.openai_model || 'gpt-5.5';
        if (document.getElementById('set-anthropic-model')) document.getElementById('set-anthropic-model').value = status.anthropic_model || 'claude-opus-4-7';
        if (document.getElementById('set-deepseek-model')) document.getElementById('set-deepseek-model').value = status.deepseek_model || 'deepseek-v4-pro';
        setFieldValue('set-ai-temp', status.ai_temperature ?? 0.3);
        setFieldValue('set-ai-tokens', status.ai_max_tokens ?? 1000);
        setFieldValue('set-ai-prompt', status.ai_custom_system_prompt || '');
        setFieldValue('set-tg-chat', status.telegram?.chat_id || '');
        setSecretPlaceholder('set-tg-token', status.telegram?.bot_configured, 'Enter Telegram Bot Token', status.telegram?.bot_token_masked);
        toggleCustomAIFields();
        const tp = status.take_profit || {};
        setFieldValue('set-tp-levels', tp.num_levels ?? status.tp_levels ?? 1);
        setFieldValue('set-tp1-pct', tp.tp1_pct ?? 2);
        setFieldValue('set-tp2-pct', tp.tp2_pct ?? 4);
        setFieldValue('set-tp3-pct', tp.tp3_pct ?? 6);
        setFieldValue('set-tp4-pct', tp.tp4_pct ?? 10);
        setFieldValue('set-tp1-qty', tp.tp1_qty ?? 25);
        setFieldValue('set-tp2-qty', tp.tp2_qty ?? 25);
        setFieldValue('set-tp3-qty', tp.tp3_qty ?? 25);
        setFieldValue('set-tp4-qty', tp.tp4_qty ?? 25);
        toggleTPLevels();
        const ts = status.trailing_stop || {};
        setFieldValue('set-ts-mode', ts.mode ?? status.trailing_stop_mode ?? 'none');
        setFieldValue('set-ts-trail-pct', ts.trail_pct ?? 1.0);
        setFieldValue('set-ts-activation', ts.activation_profit_pct ?? 1.0);
        setFieldValue('set-ts-step', ts.trailing_step_pct ?? 0.5);
        toggleTSFields();
        const risk = status.risk || {};
        setFieldValue('set-max-pos', risk.max_position_pct ?? 10);
        setFieldValue('set-max-trades', risk.max_daily_trades ?? 10);
        setFieldValue('set-max-loss', risk.max_daily_loss_pct ?? 5);
        setFieldValue('set-margin-mode', risk.margin_mode || 'cross');
        setFieldValue('set-custom-sl', risk.custom_stop_loss_pct ?? 1.5);
        setFieldValue('set-ai-exit-prompt', risk.ai_exit_system_prompt || '');
        setFieldValue('set-equity', risk.account_equity_usdt ?? 10000);
        setFieldValue('set-fixed-size', risk.fixed_position_size_usdt ?? 100);
        setFieldValue('set-risk-per-trade', risk.risk_per_trade_pct ?? 1);
        setFieldValue('set-equity-risk', risk.account_equity_usdt ?? 10000);
        const mode = risk.exit_management_mode === 'custom' ? 'custom' : 'ai';
        const modeEl = document.getElementById(`exit-mode-${mode}`);
        if (modeEl) modeEl.checked = true;
        const profile = ['conservative','balanced','aggressive'].includes(risk.ai_risk_profile) ? risk.ai_risk_profile : 'balanced';
        const profileEl = document.getElementById(`ai-risk-${profile}`);
        if (profileEl) profileEl.checked = true;
        const sizingMode = ['percentage','fixed','risk_ratio'].includes(risk.position_sizing_mode) ? risk.position_sizing_mode : 'percentage';
        const sizingEl = document.getElementById(`sizing-${sizingMode === 'risk_ratio' ? 'risk' : sizingMode}`);
        if (sizingEl) sizingEl.checked = true;
        toggleExitModeFields();
        toggleRiskProfileHint();
        togglePositionSizingFields();
        if (isAdmin()) {
            loadAdminWebhookConfig();
            loadVotingConfig();
        }
        // Load 2FA status
        load2FAStatus();
    } catch (e) { console.error('Settings load error:', e); }
}

async function loadAdminWebhookConfig() {
    setText('webhook-url', `${window.location.origin}/webhook`);
    setText('admin-webhook-secret', 'Loading...');
    setText('admin-webhook-template', 'Loading...');
    try {
        const webhookConfig = await fetchAPI('/api/admin/webhook-config');
        setText('webhook-url', webhookConfig.webhook_url || `${window.location.origin}/webhook`);
        setText('admin-webhook-secret', webhookConfig.secret || '');
        try {
            const tpl = JSON.parse(webhookConfig.template || '{}');
            if (tpl && webhookConfig.secret) tpl.secret = webhookConfig.secret;
            const templateEl = document.getElementById('admin-webhook-template');
            if (templateEl) templateEl.textContent = JSON.stringify(tpl, null, 2);
        } catch (e) {
            setText('admin-webhook-template', webhookConfig.template || '');
        }
    } catch (e) {
        console.error('Webhook config load error:', e);
        setText('admin-webhook-secret', 'Unable to load. Make sure you are logged in as admin and the backend image is updated.');
        setText('admin-webhook-template', `Unable to load template: ${e.message}`);
        showToast(e.message, 'error', 'Webhook Config Load Failed');
    }
}

async function testConnection() {
    const btn = document.getElementById('btn-test-conn');
    const result = document.getElementById('conn-result');
    btn.disabled = true; btn.innerHTML = '<i class="ri-loader-4-line"></i> Testing...';
    try {
        const resp = await fetchAPI('/api/test-connection', { method:'POST', body:JSON.stringify({
            exchange: document.getElementById('set-exchange').value,
            api_key: document.getElementById('set-api-key').value,
            api_secret: document.getElementById('set-api-secret').value,
            password: document.getElementById('set-password').value,
            sandbox_mode: document.getElementById('set-exchange-sandbox')?.checked || false,
            market_type: document.getElementById('set-exchange-market-type')?.value || 'contract',
            default_order_type: document.getElementById('set-default-order-type')?.value || 'limit',
            stop_loss_order_type: document.getElementById('set-stop-loss-order-type')?.value || 'market',
            limit_timeout_overrides: timeoutOverridesFromInputs('set')
        })});
        result.className = `conn-result ${resp.success?'success':'error'}`;
        result.textContent = resp.message;
    } catch (e) { result.className = 'conn-result error'; result.textContent = `Failed: ${e.message}`; }
    btn.disabled = false; btn.innerHTML = '<i class="ri-link"></i> Test Connection';
}

async function saveExchangeSettings() { await saveSettings('/api/settings/exchange', {
    exchange: document.getElementById('set-exchange').value,
    api_key: document.getElementById('set-api-key').value,
    api_secret: document.getElementById('set-api-secret').value,
    password: document.getElementById('set-password').value,
    live_trading: document.getElementById('set-live-trading')?.value === 'true',
    sandbox_mode: document.getElementById('set-exchange-sandbox')?.checked || false,
    market_type: document.getElementById('set-exchange-market-type')?.value || 'contract',
    default_order_type: document.getElementById('set-default-order-type')?.value || 'limit',
    stop_loss_order_type: document.getElementById('set-stop-loss-order-type')?.value || 'market',
    limit_timeout_overrides: timeoutOverridesFromInputs('set')
}, 'btn-save-exchange'); }
async function saveAISettings() {
    const provider = document.getElementById('set-ai-provider').value;
    const customProviderName = document.getElementById('set-custom-provider-name')?.value || 'custom';
    const isCustomProvider = provider === 'custom' || provider === customProviderName.toLowerCase();

    const votingModelsText = document.getElementById('voting-models')?.value || '';
    const votingModels = votingModelsText.split('\n').map(line => line.trim()).filter(line => line.length > 0);

    // Parse voting weights from textarea (format: model_id: weight)
    const votingWeightsText = document.getElementById('voting-weight-models')?.value || '';
    const votingWeights = {};
    votingWeightsText.split('\n').forEach(line => {
        const parts = line.split(':').map(p => p.trim());
        if (parts.length >= 2 && parts[0]) {
            votingWeights[parts[0]] = parseFloat(parts[1]) || 1.0;
        }
    });

    await saveSettings('/api/settings/ai', {
        provider,
        api_key: document.getElementById('set-ai-key').value,
        temperature: readNumberInput('set-ai-temp', 0.3),
        max_tokens: readNumberInput('set-ai-tokens', 1000, value => parseInt(value, 10)),
        custom_system_prompt: document.getElementById('set-ai-prompt').value || '',
        custom_provider_enabled: isCustomProvider || document.getElementById('set-custom-provider-enabled')?.checked || false,
        custom_provider_name: customProviderName,
        custom_provider_model: document.getElementById('set-custom-provider-model')?.value || '',
        custom_provider_api_url: document.getElementById('set-custom-provider-url')?.value || '',
        custom_provider_api_key: isCustomProvider ? document.getElementById('set-ai-key').value : '',
        openrouter_enabled: provider === 'openrouter',
        openrouter_model: document.getElementById('set-openrouter-model')?.value || 'openai/gpt-5.5',
        mistral_model: document.getElementById('set-mistral-model')?.value || 'mistral-large-latest',
        openai_model: document.getElementById('set-openai-model')?.value || 'gpt-5.5',
        anthropic_model: document.getElementById('set-anthropic-model')?.value || 'claude-opus-4-7',
        deepseek_model: document.getElementById('set-deepseek-model')?.value || 'deepseek-v4-pro',
        voting_enabled: document.getElementById('voting-enabled')?.checked || false,
        voting_models: votingModels,
        voting_weights: votingWeights,
        voting_strategy: document.getElementById('voting-strategy')?.value || 'weighted'
    }, 'btn-save-ai');
}
async function saveTelegramSettings() { await saveSettings('/api/settings/telegram', { bot_token:document.getElementById('set-tg-token').value, chat_id:document.getElementById('set-tg-chat').value }); }
async function saveRiskSettings() {
    const exitMode = document.querySelector('input[name="exit-management-mode"]:checked')?.value || 'ai';
    const profile = document.querySelector('input[name="ai-risk-profile"]:checked')?.value || 'balanced';
    const sizingMode = document.querySelector('input[name="position-sizing-mode"]:checked')?.value || 'percentage';
    let accountEquity = 10000;
    if (sizingMode === 'percentage') {
        accountEquity = readNumberInput('set-equity', 10000);
    } else if (sizingMode === 'risk_ratio') {
        accountEquity = readNumberInput('set-equity-risk', 10000);
    } else {
        accountEquity = readNumberInput('set-equity', 10000);
    }
    await saveSettings('/api/settings/risk', {
        max_position_pct: readNumberInput('set-max-pos', 10),
        max_daily_trades: readNumberInput('set-max-trades', 10, value => parseInt(value, 10)),
        max_daily_loss_pct: readNumberInput('set-max-loss', 5),
        margin_mode: document.getElementById('set-margin-mode')?.value || 'cross',
        exit_management_mode: exitMode,
        ai_risk_profile: profile,
        custom_stop_loss_pct: readNumberInput('set-custom-sl', 1.5),
        ai_exit_system_prompt: document.getElementById('set-ai-exit-prompt').value || '',
        position_sizing_mode: sizingMode,
        account_equity_usdt: accountEquity,
        fixed_position_size_usdt: readNumberInput('set-fixed-size', 100),
        risk_per_trade_pct: readNumberInput('set-risk-per-trade', 1),
    });
}

// ─── Take-Profit ───
function toggleTPLevels() { const num = parseInt(document.getElementById('set-tp-levels').value)||1; for(let i=1;i<=4;i++){const r=document.getElementById(`tp-row-${i}`);if(r)r.style.display=i<=num?'block':'none';} }
async function saveTPSettings() {
    const data = { num_levels:readNumberInput('set-tp-levels', 1, value => parseInt(value, 10)), tp1_pct:readNumberInput('set-tp1-pct', 2.0), tp2_pct:readNumberInput('set-tp2-pct', 4.0), tp3_pct:readNumberInput('set-tp3-pct', 6.0), tp4_pct:readNumberInput('set-tp4-pct', 10.0), tp1_qty:readNumberInput('set-tp1-qty', 25.0), tp2_qty:readNumberInput('set-tp2-qty', 25.0), tp3_qty:readNumberInput('set-tp3-qty', 25.0), tp4_qty:readNumberInput('set-tp4-qty', 25.0) };
    const total = [data.tp1_qty,data.tp2_qty,data.tp3_qty,data.tp4_qty].slice(0,data.num_levels).reduce((a,b)=>a+b,0);
    if (total > 100) { showToast(`Total close % is ${total}%. Must be ≤ 100%.`,'warning','Invalid TP Config'); return; }
    await saveSettings('/api/settings/take-profit', data);
    showToast(`${data.num_levels} TP levels saved.`,'success','Take-Profit Updated');
}

// ─── Trailing Stop ───
function toggleTSFields() {
    const mode = document.getElementById('set-ts-mode').value;
    const m = document.getElementById('ts-moving-fields'), p = document.getElementById('ts-profit-fields'), d = document.getElementById('ts-description'), dt = document.getElementById('ts-description-text');
    m.style.display = 'none'; p.style.display = 'none'; d.style.display = 'none';
    const descs = { none:'', auto:'AI automatically selects optimal trailing stop mode based on market conditions, confidence, and trend strength.', moving:'The stop-loss will trail behind the price by the specified percentage.', breakeven_on_tp1:'When TP1 is reached, the stop-loss moves to the entry price (breakeven).', step_trailing:'As each TP is reached, SL moves to the previous TP price.', profit_pct_trailing:'The trailing stop activates after unrealized profit reaches the threshold.' };
    if (mode === 'moving') m.style.display = 'block';
    else if (mode === 'profit_pct_trailing') p.style.display = 'block';
    if (descs[mode]) { dt.textContent = descs[mode]; d.style.display = 'flex'; }
}
async function saveTSSettings() {
    const data = {
        mode: document.getElementById('set-ts-mode').value,
        trail_pct: readNumberInput('set-ts-trail-pct', 1.0),
        activation_profit_pct: readNumberInput('set-ts-activation', 1.0),
        trailing_step_pct: readNumberInput('set-ts-step', 0.5)
    };
    await saveSettings('/api/settings/trailing-stop', data);
    showToast(`Trailing stop: ${data.mode}`, 'success', 'Trailing Stop Updated');
}

// ─── Two-Factor Authentication (2FA) ───
let _2faSecret = '';
let _2faRecoveryCodes = [];

async function load2FAStatus() {
    try {
        const data = await fetchAPI('/api/auth/2fa/status');
        const badge = document.getElementById('twofa-status-badge');
        const disabledView = document.getElementById('twofa-disabled-view');
        const enabledView = document.getElementById('twofa-enabled-view');
        const setupView = document.getElementById('twofa-setup-view');
        const recoveryView = document.getElementById('twofa-recovery-view');

        if (!badge) return;

        setupView.style.display = 'none';
        recoveryView.style.display = 'none';

        if (data.enabled) {
            badge.style.display = 'inline-block';
            badge.textContent = 'Enabled';
            badge.style.background = 'rgba(34,197,94,0.15)';
            badge.style.color = '#22c55e';
            disabledView.style.display = 'none';
            enabledView.style.display = 'block';
            document.getElementById('twofa-recovery-remaining').textContent =
                `Recovery codes remaining: ${data.recovery_codes_remaining}`;
        } else {
            badge.style.display = 'none';
            disabledView.style.display = 'block';
            enabledView.style.display = 'none';
        }
    } catch (e) { console.error('2FA status error:', e); }
}

async function setup2FA() {
    const btn = document.getElementById('btn-setup-2fa');
    btn.disabled = true;
    btn.innerHTML = '<i class="ri-loader-4-line"></i> Setting up...';
    try {
        const data = await fetchAPI('/api/auth/2fa/setup', { method: 'POST' });
        _2faSecret = data.secret;

        // Show QR code
        const container = document.getElementById('twofa-qr-container');
        container.innerHTML = `<img src="data:image/png;base64,${data.qr_code}" alt="2FA QR Code" style="width:180px;height:180px;image-rendering:pixelated">`;
        document.getElementById('twofa-manual-key').textContent = data.secret;

        document.getElementById('twofa-disabled-view').style.display = 'none';
        document.getElementById('twofa-setup-view').style.display = 'block';
        document.getElementById('twofa-confirm-code').value = '';
        document.getElementById('twofa-confirm-code').focus();
    } catch (e) {
        showToast(e.message || 'Failed to set up 2FA', 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="ri-shield-keyhole-line"></i> Set Up 2FA';
    }
}

function cancel2FASetup() {
    document.getElementById('twofa-setup-view').style.display = 'none';
    document.getElementById('twofa-disabled-view').style.display = 'block';
    _2faSecret = '';
}

async function confirm2FA() {
    const code = document.getElementById('twofa-confirm-code').value.trim();
    if (!code || code.length !== 6) {
        showToast('Enter a valid 6-digit code', 'error');
        return;
    }
    const btn = document.getElementById('btn-confirm-2fa');
    btn.disabled = true;
    btn.innerHTML = '<i class="ri-loader-4-line"></i> Verifying...';
    try {
        const data = await fetchAPI('/api/auth/2fa/enable', {
            method: 'POST',
            body: JSON.stringify({ code }),
        });
        _2faRecoveryCodes = data.recovery_codes || [];

        // Show recovery codes
        const codesEl = document.getElementById('twofa-recovery-codes');
        codesEl.innerHTML = _2faRecoveryCodes.map(c =>
            `<div style="padding:8px 12px;background:var(--bg-input);border:1px solid var(--border);border-radius:6px;font-family:monospace;font-size:14px;text-align:center;letter-spacing:1px;user-select:all">${escapeHtml(c)}</div>`
        ).join('');

        document.getElementById('twofa-setup-view').style.display = 'none';
        document.getElementById('twofa-recovery-view').style.display = 'block';
        showToast('2FA enabled successfully!', 'success');
    } catch (e) {
        showToast(e.message || 'Invalid code', 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="ri-check-line"></i> Enable 2FA';
    }
}

function copyRecoveryCodes() {
    const text = _2faRecoveryCodes.join('\n');
    copyText(text, 'Recovery codes copied');
}

function finish2FASetup() {
    _2faRecoveryCodes = [];
    _2faSecret = '';
    load2FAStatus();
}

async function disable2FA() {
    const pw = document.getElementById('twofa-disable-pw').value;
    if (!pw) {
        showToast('Enter your password to disable 2FA', 'error');
        return;
    }
    const btn = document.getElementById('btn-disable-2fa');
    btn.disabled = true;
    btn.innerHTML = '<i class="ri-loader-4-line"></i> Disabling...';
    try {
        await fetchAPI('/api/auth/2fa/disable', {
            method: 'POST',
            body: JSON.stringify({ password: pw }),
        });
        document.getElementById('twofa-disable-pw').value = '';
        showToast('2FA has been disabled', 'success');
        load2FAStatus();
    } catch (e) {
        showToast(e.message || 'Failed to disable 2FA', 'error');
    } finally {
        btn.disabled = false;
        btn.innerHTML = '<i class="ri-shield-cross-line"></i> Disable 2FA';
    }
}

// ─── Subscription Page ───
async function loadUserPortal() {
    try {
        const [userSettings, perf, sub] = await Promise.all([
            fetchAPI('/api/user/settings'),
            fetchAPI('/api/user/performance?days=30'),
            fetchAPI('/api/my-subscription'),
        ]);
        currentUserSettings = userSettings;
        renderUserSettings(userSettings);
        renderUserPerformance(perf, sub);
        await loadUserSubscriptionPanel();
    } catch (err) {
        showToast(err.message, 'error', 'User Portal Load Failed');
    }
}

function renderUserSettings(data) {
    const ex = data.exchange || {}, tp = data.take_profit || {}, wh = data.webhook || {}, controls = data.trade_controls || {};
    setFieldValue('user-exchange', ex.exchange || ex.name || 'binance');
    setFieldValue('user-live-trading', String(Boolean(ex.live_trading)));
    setFieldValue('user-market-type', ex.market_type || 'contract');
    setFieldValue('user-default-order-type', ex.default_order_type || 'limit');
    setFieldValue('user-stop-loss-order-type', ex.stop_loss_order_type || 'market');
    applyTimeoutOverrideFields('user', ex.limit_timeout_overrides || {});
    const sandbox = document.getElementById('user-sandbox-mode');
    if (sandbox) sandbox.checked = Boolean(ex.sandbox_mode);
    const liveSelect = document.getElementById('user-live-trading');
    if (liveSelect) liveSelect.disabled = !controls.live_trading_allowed;
    const savedKey = ex.api_key_masked ? ` · Key ${ex.api_key_masked}` : '';
    setText('user-api-configured', `${ex.api_configured ? 'Configured' : 'Not configured'}${savedKey} · Live ${controls.live_trading_allowed ? 'allowed' : 'disabled'} · ${ex.sandbox_mode ? 'Sandbox' : 'Live endpoints'} · Max ${controls.max_leverage || 20}x`);
    setSecretPlaceholder('user-api-key', Boolean(ex.api_configured), 'Leave blank to keep saved key', ex.api_key_masked);
    setSecretPlaceholder('user-api-secret', Boolean(ex.api_configured), 'Leave blank to keep saved secret', ex.api_secret_masked);
    setSecretPlaceholder('user-api-password', Boolean(ex.password_masked), 'OKX / Bitget optional', ex.password_masked);
    setFieldValue('user-tp-levels', tp.num_levels || 1);
    ['tp1_pct','tp2_pct','tp3_pct','tp4_pct','tp1_qty','tp2_qty','tp3_qty','tp4_qty'].forEach(key => {
        const id = `user-${key.replace('_','-')}`;
        if (document.getElementById(id)) setFieldValue(id, tp[key] ?? '');
    });
    toggleUserTPLevels();
    setText('user-webhook-url', wh.url || `${window.location.origin}/webhook`);
    const secretDisplay = wh.secret || wh.secret_masked || '';
    setText('user-webhook-secret', secretDisplay);
    const templateStr = wh.template || '';
    const templateEl = document.getElementById('user-webhook-template');
    if (templateEl) {
        try {
            const tpl = JSON.parse(templateStr);
            if (tpl && wh.secret && !wh.secret_masked) {
                tpl.secret = wh.secret;
            } else if (tpl && wh.secret_masked) {
                tpl.secret = wh.secret_masked;
            }
            templateEl.textContent = JSON.stringify(tpl, null, 2);
        } catch (e) {
            templateEl.textContent = templateStr;
        }
    }
    // Add reveal button for webhook secret
    const secretBox = document.getElementById('user-webhook-secret');
    if (secretBox && wh.secret_masked && !wh.secret) {
        const revealBtn = secretBox.parentElement.querySelector('.btn-reveal-secret');
        if (!revealBtn) {
            const btn = document.createElement('button');
            btn.className = 'btn btn-sm btn-reveal-secret';
            btn.style.cssText = 'margin-left:8px;font-size:11px;padding:2px 8px';
            btn.innerHTML = '<i class="ri-eye-line"></i> Reveal';
            btn.onclick = async () => {
                try {
                    const data = await fetchAPI('/api/webhook-secret');
                    if (data.secret) {
                        setText('user-webhook-secret', data.secret);
                        btn.innerHTML = '<i class="ri-eye-off-line"></i> Hidden';
                        btn.disabled = true;
                        setTimeout(() => {
                            setText('user-webhook-secret', wh.secret_masked || '***');
                            btn.innerHTML = '<i class="ri-eye-line"></i> Reveal';
                            btn.disabled = false;
                        }, 30000);
                    }
                } catch (e) {
                    showToast('Failed to reveal webhook secret', 'error');
                }
            };
            secretBox.parentElement.appendChild(btn);
        }
    }
}

function renderUserPerformance(perf, sub) {
    const pnl = Number(perf.total_pnl_pct || 0);
    const pnlEl = document.getElementById('user-kpi-pnl');
    if (pnlEl) {
        pnlEl.textContent = `${pnl >= 0 ? '+' : ''}${pnl.toFixed(2)}%`;
        pnlEl.className = `kpi-value ${pnl >= 0 ? 'pnl-positive' : 'pnl-negative'}`;
    }
    setText('user-kpi-trades', perf.total_trades || 0);
    setText('user-kpi-winrate', `${Number(perf.win_rate || 0).toFixed(1)}%`);
    setText('user-kpi-sub', sub && sub.status === 'active' ? 'Active' : 'None');
    renderUserEquityChart(perf.equity_curve || []);
}

function renderUserEquityChart(curve) {
    if (typeof window.userEquityChart !== 'undefined' && window.userEquityChart) {
        window.userEquityChart.destroy();
    }
    const ctx = document.getElementById('user-equity-chart')?.getContext('2d');
    if (!ctx) return;
    const labels = curve.map(c => {
        if (c.date) return c.date;
        if (c.timestamp) return new Date(c.timestamp).toLocaleDateString(undefined, {month:'short', day:'numeric'});
        return '';
    });
    window.userEquityChart = new Chart(ctx, {
        type:'line',
        data:{ labels, datasets:[{ label:'My P&L %', data:curve.map(c=>c.cumulative_pnl), borderColor:'#10b981', backgroundColor:'rgba(16,185,129,.12)', borderWidth:2, fill:true, tension:.35, pointRadius:0 }] },
        options:typeof chartOptions === 'function' ? chartOptions('P&L %') : {responsive:true, maintainAspectRatio:false}
    });
}

async function loadUserSubscriptionPanel() {
    const panel = document.getElementById('user-subscription-panel');
    if (!panel) return;
    const [plans, mySub, payments, me] = await Promise.all([fetchAPI('/api/plans'), fetchAPI('/api/my-subscription'), fetchAPI('/api/my-payments'), fetchAPI('/api/auth/me')]);
    const balance = Number(me.balance_usdt || 0);
    const status = mySub && mySub.status === 'active'
        ? `<div class="sub-active"><i class="ri-checkbox-circle-fill"></i><div><strong>${escapeHtml(mySub.plan_name)}</strong><br><span class="hint">Until ${escapeHtml(formatDateTime(mySub.end_date))} · Balance ${formatNum(balance)} USDT</span></div></div>`
        : `<div class="sub-inactive"><i class="ri-close-circle-line"></i><span>No active subscription · Balance ${formatNum(balance)} USDT</span></div>`;
    const planCards = plans.map(p => {
        const price = Number(p.price_usdt || 0);
        const buttonText = price <= 0 ? 'Activate Free' : (balance >= price ? 'Pay With Balance' : 'Pay USDT');
        return `<div class="plan-card"><h3>${escapeHtml(p.name)}</h3><div class="plan-price">${price > 0 ? '$' + formatNum(price) : 'Free'}</div><p class="plan-desc">${escapeHtml(p.description || '')}</p><button class="btn-plan" onclick="subscribeToPlan('${escapeJsSingle(p.id)}',${price})">${buttonText}</button></div>`;
    }).join('');
    const rows = payments.length ? `<table class="data-table"><thead><tr><th>Date</th><th>Amount</th><th>Network</th><th>Status</th></tr></thead><tbody>${payments.slice(0,8).map(p => `<tr><td>${escapeHtml(formatDateTime(p.created_at))}</td><td>${escapeHtml(p.amount)} ${escapeHtml(p.currency)}</td><td>${escapeHtml(p.network)}</td><td><span class="badge badge-${safeClassToken(p.status)}">${escapeHtml(p.status)}</span></td></tr>`).join('')}</tbody></table>` : '<p class="empty-state">No payments yet</p>';
    panel.innerHTML = `${status}<div class="plans-grid" style="margin-top:20px">${planCards}</div><div class="form-row" style="margin-top:20px"><div class="form-group"><label for="user-redeem-code">Card Code</label><input id="user-redeem-code" class="text-input" placeholder="CARD-XXXXXXXXXXXX"></div><div class="form-group" style="display:flex;align-items:flex-end"><button class="btn btn-primary" onclick="redeemUserCardCode()"><i class="ri-gift-line"></i> Redeem</button></div></div><div class="mt-4">${rows}</div>`;
}

async function saveUserExchangeSettings() {
    const data = {
        exchange: document.getElementById('user-exchange')?.value || 'binance',
        live_trading: document.getElementById('user-live-trading')?.value === 'true',
        sandbox_mode: document.getElementById('user-sandbox-mode')?.checked || false,
        api_key: document.getElementById('user-api-key')?.value || '',
        api_secret: document.getElementById('user-api-secret')?.value || '',
        password: document.getElementById('user-api-password')?.value || '',
        market_type: document.getElementById('user-market-type')?.value || 'contract',
        default_order_type: document.getElementById('user-default-order-type')?.value || 'limit',
        stop_loss_order_type: document.getElementById('user-stop-loss-order-type')?.value || 'market',
        limit_timeout_overrides: timeoutOverridesFromInputs('user'),
    };
    await fetchAPI('/api/user/settings/exchange', { method:'POST', body:JSON.stringify(data) });
    ['user-api-key','user-api-secret','user-api-password'].forEach(id => { const el = document.getElementById(id); if (el) el.value = ''; });
    showToast('Exchange settings saved.','success','Saved');
    loadUserPortal();
}

async function saveUserTPSettings() {
    const data = {
        num_levels: readNumberInput('user-tp-levels', 1, value => parseInt(value, 10)),
        tp1_pct: readNumberInput('user-tp1-pct', 2),
        tp2_pct: readNumberInput('user-tp2-pct', 4),
        tp3_pct: readNumberInput('user-tp3-pct', 6),
        tp4_pct: readNumberInput('user-tp4-pct', 10),
        tp1_qty: readNumberInput('user-tp1-qty', 25),
        tp2_qty: readNumberInput('user-tp2-qty', 25),
        tp3_qty: readNumberInput('user-tp3-qty', 25),
        tp4_qty: readNumberInput('user-tp4-qty', 25),
    };
    await fetchAPI('/api/user/settings/take-profit', { method:'POST', body:JSON.stringify(data) });
    showToast('Take-profit settings saved.','success','Saved');
    loadUserPortal();
}

async function redeemUserCardCode() {
    const input = document.getElementById('user-redeem-code');
    const code = input?.value.trim();
    if (!code) return showToast('Please enter a card code.','warning','Missing Code');
    await fetchAPI('/api/redeem-code', { method:'POST', body:JSON.stringify({ code }) });
    input.value = '';
    showToast('Code redeemed.','success','Redeemed');
    loadUserPortal();
}

async function loadSubscription() {
    try {
        const [plans, mySub, myPayments, me] = await Promise.all([
            fetchAPI('/api/plans'),
            fetchAPI('/api/my-subscription'),
            fetchAPI('/api/my-payments'),
            fetchAPI('/api/auth/me'),
        ]);
        // Merge latest profile into the in-memory cache
        _cachedUser = { ..._cachedUser, ...me };
        updateUserUI();
        const balance = Number(me.balance_usdt || 0);
        // Current subscription status
        const statusEl = document.getElementById('sub-status');
        if (mySub && mySub.status === 'active') {
            const endDate = new Date(mySub.end_date).toLocaleDateString();
            statusEl.innerHTML = `<div class="sub-active"><i class="ri-checkbox-circle-fill"></i><div><strong>${escapeHtml(mySub.plan_name)}</strong><br><span style="color:var(--text-muted);font-size:13px">Active until ${endDate} · Balance ${formatNum(balance)} USDT</span></div></div>`;
        } else {
            statusEl.innerHTML = `<div class="sub-inactive"><i class="ri-close-circle-line"></i><span>No active subscription · Balance ${formatNum(balance)} USDT</span></div>`;
        }

        // Available plans
        const plansEl = document.getElementById('plans-grid');
        plansEl.innerHTML = plans.map(p => {
            const features = Array.isArray(p.features) ? p.features : JSON.parse(p.features_json || '[]');
            const price = Number(p.price_usdt || 0);
            const buttonText = price <= 0 ? 'Activate Free' : (balance >= price ? 'Pay With Balance' : 'Pay USDT');
            return `<div class="plan-card"><h3>${escapeHtml(p.name)}</h3><div class="plan-price">${price > 0 ? '$' + formatNum(price) : 'Free'}</div><p class="plan-desc">${escapeHtml(p.description)}</p><ul class="plan-features">${features.map(f=>`<li><i class="ri-check-line"></i>${escapeHtml(f)}</li>`).join('')}</ul><button class="btn-plan" onclick="subscribeToPlan('${escapeJsSingle(p.id)}',${price})">${buttonText}</button></div>`;
        }).join('');

        // Payment history
        const payEl = document.getElementById('payment-history');
        if (myPayments.length) {
            payEl.innerHTML = `<table class="data-table"><thead><tr><th>Date</th><th>Amount</th><th>Network</th><th>Status</th><th>TX</th></tr></thead><tbody>${myPayments.map(p => `<tr><td>${escapeHtml(new Date(p.created_at).toLocaleDateString())}</td><td>${escapeHtml(p.amount)} ${escapeHtml(p.currency)}</td><td>${escapeHtml(p.network)}</td><td><span class="badge badge-${safeClassToken(p.status)}">${escapeHtml(p.status)}</span></td><td>${p.tx_hash ? escapeHtml(p.tx_hash.slice(0,12))+'...' : '--'}</td></tr>`).join('')}</tbody></table>`;
        } else {
            payEl.innerHTML = '<p style="color:var(--text-muted);text-align:center;padding:20px">No payments yet</p>';
        }
    } catch (err) { showToast(err.message, 'error', 'Subscription Load Failed'); }
}

async function subscribeToPlan(planId, price) {
    try {
        const sub = await fetchAPI('/api/subscribe', { method:'POST', body:JSON.stringify({ plan_id:planId }) });
        if (price <= 0 || sub.status === 'active') {
            showToast(sub.paid_from_balance ? 'Subscription paid from account balance.' : 'Subscription activated.','success','Subscribed');
            reloadBillingViews();
            return;
        }
        // Show payment modal
        showPaymentModal(sub.id, price);
    } catch (err) { showToast(err.message,'error','Subscribe Failed'); }
}

async function redeemCardCode() {
    const input = document.getElementById('redeem-code-input');
    const code = input?.value.trim();
    if (!code) {
        showToast('Please enter a card code.','warning','Missing Code');
        return;
    }
    try {
        const result = await fetchAPI('/api/redeem-code', { method:'POST', body:JSON.stringify({ code }) });
        input.value = '';
        const pieces = [];
        if (Number(result.balance_usdt || 0) > 0) pieces.push(`${formatNum(result.balance_usdt)} USDT balance`);
        if (result.subscription) pieces.push('subscription activated');
        showToast(pieces.length ? pieces.join(' + ') : 'Code redeemed.', 'success', 'Redeemed');
        reloadBillingViews();
    } catch (err) {
        showToast(err.message, 'error', 'Redeem Failed');
    }
}

async function loadSubscriptionHistory() {
    try {
        const subscriptions = await fetchAPI('/api/subscriptions');
        const cardEl = document.getElementById('subscription-history-card');
        const listEl = document.getElementById('subscription-history');
        if (cardEl) cardEl.style.display = 'block';
        if (!subscriptions.length) {
            if (listEl) listEl.innerHTML = '<div class="empty-state">No subscription history</div>';
            return;
        }
        if (listEl) {
            listEl.innerHTML = `<div class="table-wrapper"><table class="data-table"><thead><tr><th>Plan</th><th>Status</th><th>Start Date</th><th>End Date</th><th>Auto Renew</th></tr></thead><tbody>${subscriptions.map(s => `<tr><td><strong>${escapeHtml(s.plan_name || s.plan_id)}</strong></td><td><span class="badge badge-${s.status === 'active' ? 'active' : 'pending'}">${escapeHtml(s.status)}</span></td><td>${escapeHtml(formatDateTime(s.start_date))}</td><td>${escapeHtml(formatDateTime(s.end_date))}</td><td>${s.auto_renew ? 'Yes' : 'No'}</td></tr>`).join('')}</tbody></table></div>`;
        }
    } catch (err) {
        showToast(err.message, 'error', 'History Load Failed');
    }
}

function hideSubscriptionHistory() {
    const cardEl = document.getElementById('subscription-history-card');
    if (cardEl) cardEl.style.display = 'none';
}

async function showPaymentModal(subscriptionId, amount) {
    const options = await fetchAPI('/api/payment-options');
    const modal = document.getElementById('payment-modal');
    const body = document.getElementById('payment-modal-body');

    if (!options.networks.length) {
        showToast('No payment address has been configured yet. Please contact the admin.','warning','Payment Unavailable');
        return;
    }
    let network = options.networks.length > 0 ? options.networks[0].network : 'TRC20';
    body.innerHTML = `
        <h3 style="margin-bottom:16px">Pay ${amount} USDT</h3>
        <div class="form-group"><label>Payment Network</label>
            <div id="pay-network" class="payment-network-grid">
                ${options.networks.map((n, idx) => `<button type="button" class="payment-network-option ${idx === 0 ? 'active' : ''}" data-network="${escapeHtml(n.network)}" onclick="selectPaymentNetwork(this,'${subscriptionId}',${amount})">${escapeHtml(n.name)}<span>${escapeHtml(n.fee)}</span></button>`).join('')}
            </div>
        </div>
        <div id="pay-address-info" style="margin-top:16px"></div>
        <div class="form-group" style="margin-top:16px"><label>Transaction Hash (TX ID)</label><input type="text" id="pay-tx-hash" class="form-input" placeholder="Paste your TX hash after sending"></div>
        <div style="display:flex;gap:12px;margin-top:16px">
            <button class="btn btn-primary" onclick="submitPayment('${subscriptionId}')"><i class="ri-check-line"></i> Submit Payment</button>
            <button class="btn btn-secondary" onclick="closePaymentModal()">Cancel</button>
        </div>`;
    modal.style.display = 'flex';
    updatePaymentAddress(subscriptionId, amount);
}

async function updatePaymentAddress(subscriptionId, amount) {
    const network = document.querySelector('#pay-network .payment-network-option.active')?.dataset.network || 'TRC20';
    try {
        const payment = await fetchAPI('/api/payment/create', { method:'POST', body:JSON.stringify({ subscription_id:subscriptionId, currency:'USDT', network:network }) });
        const infoEl = document.getElementById('pay-address-info');
        if (payment.status === 'activated') {
            closePaymentModal();
            showToast('Free plan activated!','success');
            reloadBillingViews();
            return;
        }
        infoEl.innerHTML = `<div class="payment-address-box"><label>Send to this address:</label><div class="address-display"><code>${escapeHtml(payment.address)}</code><button class="btn-copy" onclick="copyText('${escapeJsSingle(payment.address)}','Address copied!')"><i class="ri-file-copy-line"></i></button></div><p style="color:var(--text-muted);font-size:12px;margin-top:8px">Network: ${escapeHtml(payment.network_name)} · Confirmation: ${escapeHtml(payment.confirmation_time)}</p></div>`;
        // Store payment_id for submission
        document.getElementById('pay-tx-hash').dataset.paymentId = payment.id;
    } catch (err) { showToast(err.message,'error'); }
}

async function submitPayment(subscriptionId) {
    const txHash = document.getElementById('pay-tx-hash').value;
    const paymentId = document.getElementById('pay-tx-hash').dataset.paymentId;
    if (!txHash) { showToast('Please enter the TX hash','warning'); return; }
    try {
        await fetchAPI('/api/payment/submit-tx', { method:'POST', body:JSON.stringify({ payment_id:paymentId, tx_hash:txHash }) });
        showToast('Payment submitted for review!','success');
        closePaymentModal();
        reloadBillingViews();
    } catch (err) { showToast(err.message,'error'); }
}

function closePaymentModal() {
    document.getElementById('payment-modal').style.display = 'none';
}

function reloadBillingViews() {
    const page = document.querySelector('.page.active')?.id?.replace('page-', '');
    if (page === 'user') loadUserPortal();
    else loadSubscription();
}

// ─── Admin Panel ───
async function loadAdminLegacyUnused() {
    return loadAdmin();
    if (!isAdmin()) { showToast('Admin access required','error'); return; }
    try {
        const [users, payments] = await Promise.all([fetchAPI('/api/admin/users'), fetchAPI('/api/admin/payments')]);
        // Users table
        const usersEl = document.getElementById('admin-users');
        usersEl.innerHTML = `<table class="data-table"><thead><tr><th>Username</th><th>Email</th><th>Role</th><th>Subscription</th><th>Status</th><th>Actions</th></tr></thead><tbody>${users.map(u => `<tr><td><strong>${escapeHtml(u.username)}</strong></td><td>${escapeHtml(u.email)}</td><td><span class="role-badge ${safeClassToken(u.role)}">${escapeHtml(u.role)}</span></td><td>${u.subscription ? escapeHtml(u.subscription.plan_name) : '<span style="color:var(--text-muted)">None</span>'}</td><td>${u.is_active ? '<span class="badge badge-active">Active</span>' : '<span class="badge badge-inactive">Disabled</span>'}</td><td>${u.role !== 'admin' ? `<button class="btn-sm" onclick="toggleUser('${u.id}')">${u.is_active ? 'Disable' : 'Enable'}</button>` : ''}</td></tr>`).join('')}</tbody></table>`;

        // Pending payments
        const pendingPayments = payments.filter(p => p.status === 'submitted');
        const payEl = document.getElementById('admin-payments');
        if (pendingPayments.length) {
            payEl.innerHTML = `<table class="data-table"><thead><tr><th>User</th><th>Amount</th><th>Network</th><th>TX Hash</th><th>Date</th><th>Actions</th></tr></thead><tbody>${pendingPayments.map(p => `<tr><td>${escapeHtml(p.username||'--')}</td><td>${escapeHtml(p.amount)} ${escapeHtml(p.currency)}</td><td>${escapeHtml(p.network)}</td><td><code style="font-size:11px">${p.tx_hash?escapeHtml(p.tx_hash.slice(0,20))+'...':'--'}</code></td><td>${escapeHtml(new Date(p.created_at).toLocaleDateString())}</td><td><div style="display:flex;gap:6px"><button class="btn-sm btn-success" onclick="adminConfirmPayment('${p.id}')">✓ Confirm</button><button class="btn-sm btn-danger" onclick="adminRejectPayment('${p.id}')">✕ Reject</button></div></td></tr>`).join('')}</tbody></table>`;
        } else {
            payEl.innerHTML = '<p style="color:var(--text-muted);text-align:center;padding:20px">No pending payments</p>';
        }
    } catch (err) { showToast(err.message, 'error', 'Admin Load Failed'); }
}

async function toggleUser(userId) {
    try { await fetchAPI(`/api/admin/user/${userId}/toggle`, {method:'POST'}); loadAdmin(); showToast('User status updated','success'); }
    catch (err) { showToast(err.message,'error'); }
}
async function adminConfirmPayment(paymentId) {
    try { await fetchAPI(`/api/admin/payment/${paymentId}/confirm`, {method:'POST'}); loadAdmin(); showToast('Payment confirmed & subscription activated!','success'); }
    catch (err) { showToast(err.message,'error'); }
}
async function adminRejectPayment(paymentId) {
    if (!confirm('Reject this payment? This action cannot be undone.')) return;
    try { await fetchAPI(`/api/admin/payment/${paymentId}/reject`, {method:'POST'}); loadAdmin(); showToast('Payment rejected','warning'); }
    catch (err) { showToast(err.message,'error'); }
}
async function adminVerifyPayment(paymentId) {
    try {
        const result = await fetchAPI(`/api/admin/payment/${paymentId}/verify`, {method:'POST'});
        loadAdmin();
        const msg = result.verification?.reason || result.status;
        showToast(msg, result.status === 'confirmed' ? 'success' : 'warning', 'Verification Result');
    } catch (err) { showToast(err.message,'error','Verification Failed'); }
}

// ─── Helpers ───
async function loadAdmin() {
    if (!isAdmin()) { showToast('Admin access required','error'); return; }
    try {
        const [users, payments, plans, addresses, registration, invites, redeemCodes, system, auditLogs, webhookEvents, backups, monitorState, filterThresholds, filterStats, externalKeys, enhancedFilters, updateStatus, aiCosts] = await Promise.all([
            fetchAPI('/api/admin/users'),
            fetchAPI('/api/admin/payments'),
            fetchAPI('/api/admin/plans'),
            fetchAPI('/api/admin/payment-addresses'),
            fetchAPI('/api/admin/registration'),
            fetchAPI('/api/admin/invite-codes'),
            fetchAPI('/api/admin/redeem-codes'),
            fetchAPI('/api/admin/system'),
            fetchAPI('/api/admin/audit-logs?limit=8'),
            fetchAPI('/api/admin/webhook-events?limit=30'),
            fetchAPI('/api/admin/backups'),
            fetchAPI('/api/admin/position-monitor'),
            fetchAPI('/api/admin/filter-thresholds'),
            fetchAPI('/api/admin/filter-stats'),
            fetchAPI('/api/admin/external-api-keys'),
            fetchAPI('/api/admin/enhanced-filters'),
            fetchAPI('/api/admin/update-status'),
            fetchAPI('/api/admin/ai-costs').catch(() => ({ costs: {} })),
        ]);

        renderAdminUsers(users, plans);
        renderAdminPlans(plans);
        window._adminPlans = plans;
        renderAdminPaymentAddresses(addresses || {});
        renderAdminRegistration(registration || {}, invites || []);
        renderAdminRedeemCodes(redeemCodes || [], plans || []);
        renderAdminPendingPayments(payments || []);
        renderAdminUpdate(updateStatus || {});
        renderAdminSystem(system || {}, auditLogs || [], aiCosts || {});
        renderAdminWebhookEvents(webhookEvents || []);
        renderAdminBackups(backups || []);
        renderAdminPositionMonitor(monitorState || {});
        renderAdminFilterThresholds(filterThresholds || {});
        renderAdminFilterStats(filterStats || {});
        loadRiskThresholds();
        renderAdminExternalAPIKeys(externalKeys || {});
        renderAdminEnhancedFilters(enhancedFilters || {});
        loadAdminRiskConsole();
        loadAIProviderConfig();
    } catch (err) { showToast(err.message, 'error', 'Admin Load Failed'); }
}

async function loadAIProviderConfig() {
    try {
        const [config, models] = await Promise.all([
            fetchAPI('/api/admin/ai/provider-config'),
            fetchAPI('/api/admin/ai/models-list'),
        ]);
        renderAIProviderConfig(config, models);
    } catch (err) {
        const el = document.getElementById('admin-ai-provider');
        if (el) el.innerHTML = `<div class="empty-state">AI provider config unavailable: ${escapeHtml(err.message)}</div>`;
    }
}

function renderAIProviderConfig(config, models) {
    const el = document.getElementById('admin-ai-provider');
    if (!el) return;

    const providers = models.providers || {};
    const defaultProvider = config.default_provider || 'openai';

    let providerHtml = '';
    for (const [name, providerModels] of Object.entries(providers)) {
        const isDefault = name === defaultProvider;
        providerHtml += `
            <div style="margin-bottom:16px;padding:12px;border:1px solid rgba(255,255,255,0.08);border-radius:8px">
                <div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">
                    <strong>${escapeHtml(name.toUpperCase())}</strong>
                    ${isDefault ? '<span class="badge badge-active">Default</span>' : ''}
                </div>
                <div style="font-size:12px;color:var(--text-secondary)">
                    Available Models: ${providerModels.length || 0}
                </div>
                ${providerModels.length ? `
                    <div style="margin-top:8px;max-height:120px;overflow-y:auto">
                        ${providerModels.slice(0, 10).map(m => `<div style="font-size:11px;color:var(--text-muted);padding:2px 0">• ${escapeHtml(m)}</div>`).join('')}
                        ${providerModels.length > 10 ? `<div style="font-size:11px;color:var(--text-muted);padding:2px 0">... and ${providerModels.length - 10} more</div>` : ''}
                    </div>
                ` : ''}
            </div>
        `;
    }

    el.innerHTML = `
        <div class="settings-form">
            <div class="form-group">
                <label>Default AI Provider</label>
                <select id="admin-default-provider" class="select-input">
                    ${Object.keys(providers).map(p => `<option value="${escapeHtml(p)}" ${p === defaultProvider ? 'selected' : ''}>${escapeHtml(p.toUpperCase())}</option>`).join('')}
                </select>
            </div>
            <div class="form-row">
                <button class="btn btn-primary" onclick="saveAIProviderConfig()"><i class="ri-save-line"></i> Save</button>
            </div>
        </div>
        <h4 style="margin:24px 0 12px;font-size:14px">Available Providers & Models</h4>
        ${providerHtml}
        <div style="margin-top:16px;padding:12px;background:rgba(59,130,246,0.06);border-radius:8px;border:1px solid rgba(59,130,246,0.14)">
            <p style="font-size:12px;color:var(--text-secondary);margin:0">
                <i class="ri-information-line"></i>
                Provider models are fetched dynamically from AI service. Configure API keys in Settings page to enable providers.
            </p>
        </div>
    `;
}

async function saveAIProviderConfig() {
    const provider = document.getElementById('admin-default-provider')?.value || 'openai';
    try {
        await fetchAPI('/api/admin/ai/provider-config', {
            method: 'POST',
            body: JSON.stringify({ provider }),
        });
        showToast('Default provider updated.', 'success', 'Saved');
        await loadAIProviderConfig();
    } catch (err) {
        showToast(err.message, 'error', 'Save Failed');
    }
}

async function loadAdminRiskConsole() {
    if (!isAdmin()) return;
    try {
        const [controls, orderData] = await Promise.all([
            fetchAPI('/api/admin/trading-controls'),
            fetchAPI('/api/admin/order-events?limit=8').catch(() => ({ events: [] })),
        ]);
        renderTradingControls(controls || {});
        renderOrderEvents(orderData.events || []);
        loadOrderExecutionSettings();
    } catch (err) {
        setText('admin-control-mode', 'Unavailable');
        setText('admin-control-reason', err.message);
        const el = document.getElementById('admin-order-events');
        if (el) el.innerHTML = `<div class="empty-state">Risk console unavailable: ${escapeHtml(err.message)}</div>`;
    }
}

function renderTradingControls(state = {}) {
    const mode = state.mode || 'enabled';
    const allowed = state.allowed !== false;
    setText('admin-control-mode', mode.replace(/_/g, ' ').toUpperCase());
    setText('admin-control-reason', state.reason || (allowed ? 'New trade execution is allowed.' : 'New trade execution is blocked.'));
    const badge = document.getElementById('admin-control-badge');
    if (badge) {
        badge.textContent = allowed ? 'Enabled' : mode.replace(/_/g, ' ');
        badge.className = `badge badge-${allowed ? 'active' : 'error'}`;
    }
}

function renderOrderEvents(events = []) {
    const el = document.getElementById('admin-order-events');
    if (!el) return;
    if (!events.length) {
        el.innerHTML = '<div class="empty-state">No recent order events</div>';
        return;
    }
    el.innerHTML = events.map(event => {
        const status = event.status || 'unknown';
        const title = `${event.ticker || '--'} · ${(event.direction || '--').toUpperCase()}`;
        const meta = `${status} · attempts ${event.attempt_count || 0} · ${formatDateTime(event.updated_at || event.created_at)}`;
        let actions = '';
        if (status === 'manual_review') {
            const jsId = escapeJsSingle(event.id);
            actions = `<div class="order-event-actions">
                <button class="btn btn-sm btn-success" onclick="approveOrderEvent('${jsId}')"><i class="ri-check-line"></i> Approve</button>
                <button class="btn btn-sm btn-warning" onclick="retryOrderEvent('${jsId}')"><i class="ri-loop-left-line"></i> Retry</button>
                <button class="btn btn-sm btn-danger" onclick="rejectOrderEvent('${jsId}')"><i class="ri-close-line"></i> Reject</button>
            </div>`;
        }
        return `<div class="order-event"><div><strong>${escapeHtml(title)}</strong><span>${escapeHtml(meta)}</span>${event.last_error ? `<span>${escapeHtml(event.last_error)}</span>` : ''}</div><span class="badge badge-${safeClassToken(status)}">${escapeHtml(status)}</span>${actions}</div>`;
    }).join('');
}

async function setTradingControlMode(mode) {
    const reason = mode === 'enabled' ? 'Trading enabled from admin console' : `Trading set to ${mode.replace(/_/g, ' ')} from admin console`;
    try {
        const state = await fetchAPI('/api/admin/trading-controls', {
            method: 'POST',
            body: JSON.stringify({ mode, reason }),
        });
        renderTradingControls(state);
        showToast(`Trading mode: ${state.mode}`, 'success', 'Risk Control Updated');
    } catch (err) {
        showToast(err.message, 'error', 'Risk Control Failed');
    }
}

async function emergencyStopTrading() {
    if (!confirm('Activate emergency stop and block all new trade execution?')) return;
    try {
        const state = await fetchAPI('/api/admin/trading-controls/emergency-stop', { method: 'POST' });
        renderTradingControls(state);
        showToast('All new trade execution is blocked.', 'warning', 'Emergency Stop Active');
    } catch (err) {
        showToast(err.message, 'error', 'Emergency Stop Failed');
    }
}

async function runOrderReconciliation() {
    try {
        const result = await fetchAPI('/api/admin/order-events/reconcile', { method: 'POST' });
        await loadAdminRiskConsole();
        showToast(`Checked ${result.checked || 0}, manual review ${result.marked_manual_review || 0}.`, 'success', 'Reconciliation Complete');
    } catch (err) {
        showToast(err.message, 'error', 'Reconciliation Failed');
    }
}

async function approveOrderEvent(eventId) {
    if (!confirm('Approve this order for re-execution?')) return;
    try {
        const result = await fetchAPI(`/api/admin/order-events/${eventId}/approve`, {
            method: 'POST',
            body: JSON.stringify({ admin_notes: 'Approved from admin console' }),
        });
        await loadAdminRiskConsole();
        showToast('Order approved for re-execution', 'success', 'Order Approved');
    } catch (err) {
        showToast(err.message, 'error', 'Approval Failed');
    }
}

async function retryOrderEvent(eventId) {
    if (!confirm('Queue this order for retry?')) return;
    try {
        const result = await fetchAPI(`/api/admin/order-events/${eventId}/retry`, {
            method: 'POST',
            body: JSON.stringify({ admin_notes: 'Retried from admin console' }),
        });
        await loadAdminRiskConsole();
        showToast('Order queued for retry', 'success', 'Order Retried');
    } catch (err) {
        showToast(err.message, 'error', 'Retry Failed');
    }
}

async function rejectOrderEvent(eventId) {
    if (!confirm('Reject this order permanently?')) return;
    try {
        const result = await fetchAPI(`/api/admin/order-events/${eventId}/reject`, {
            method: 'POST',
            body: JSON.stringify({ admin_notes: 'Rejected from admin console' }),
        });
        await loadAdminRiskConsole();
        showToast('Order rejected', 'warning', 'Order Rejected');
    } catch (err) {
        showToast(err.message, 'error', 'Rejection Failed');
    }
}

function renderOrderExecutionSettings(settings = {}) {
    const el = document.getElementById('admin-order-execution');
    if (!el) return;
    const autoApprove = settings.auto_approve_failed_orders || false;
    const autoReject = settings.auto_reject_failed_orders || false;
    const autoRetryLeverage = settings.auto_retry_leverage_errors || false;
    const maxRetryAttempts = settings.max_leverage_retry_attempts || 3;
    const retryDelay = settings.leverage_retry_delay_secs || 5;
    el.innerHTML = `<div class="settings-form">
        <div class="form-row">
            <div class="form-group">
                <label class="checkbox-label">
                    <input type="checkbox" id="auto-approve-failed" ${autoApprove ? 'checked' : ''}>
                    <span>Auto-approve failed orders for manual review</span>
                </label>
                <span class="hint">Automatically approve orders that fail execution and move them to retry queue</span>
            </div>
        </div>
        <div class="form-row">
            <div class="form-group">
                <label class="checkbox-label">
                    <input type="checkbox" id="auto-reject-failed" ${autoReject ? 'checked' : ''}>
                    <span>Auto-reject failed orders permanently</span>
                </label>
                <span class="hint">Automatically reject orders that fail execution without manual review</span>
            </div>
        </div>
        <div class="form-row">
            <div class="form-group">
                <label class="checkbox-label">
                    <input type="checkbox" id="auto-retry-leverage" ${autoRetryLeverage ? 'checked' : ''}>
                    <span>Auto-retry leverage setup failures (OKX error 11045, etc.)</span>
                </label>
                <span class="hint">Automatically retry orders when leverage setup fails due to exchange errors</span>
            </div>
        </div>
        <div class="form-row two-col">
            <div class="form-group">
                <label for="max-leverage-retry">Max leverage retry attempts</label>
                <input type="number" id="max-leverage-retry" class="text-input" value="${maxRetryAttempts}" min="1" max="10">
            </div>
            <div class="form-group">
                <label for="leverage-retry-delay">Retry delay (seconds)</label>
                <input type="number" id="leverage-retry-delay" class="text-input" value="${retryDelay}" min="1" max="60">
            </div>
        </div>
        <div class="form-row">
            <button class="btn btn-primary" onclick="saveOrderExecutionSettings()"><i class="ri-save-line"></i> Save Automation Settings</button>
        </div>
    </div>`;
}

async function loadOrderExecutionSettings() {
    try {
        const settings = await fetchAPI('/api/admin/order-execution-settings');
        renderOrderExecutionSettings(settings);
    } catch (err) {
        console.error('Failed to load order execution settings:', err);
    }
}

async function saveOrderExecutionSettings() {
    try {
        const settings = {
            auto_approve_failed_orders: document.getElementById('auto-approve-failed')?.checked || false,
            auto_reject_failed_orders: document.getElementById('auto-reject-failed')?.checked || false,
            auto_retry_leverage_errors: document.getElementById('auto-retry-leverage')?.checked || false,
            max_leverage_retry_attempts: parseInt(document.getElementById('max-leverage-retry')?.value || '3'),
            leverage_retry_delay_secs: parseInt(document.getElementById('leverage-retry-delay')?.value || '5'),
        };
        const result = await fetchAPI('/api/admin/order-execution-settings', {
            method: 'POST',
            body: JSON.stringify(settings),
        });
        renderOrderExecutionSettings(result);
        showToast('Order execution settings saved', 'success', 'Settings Updated');
    } catch (err) {
        showToast(err.message, 'error', 'Settings Save Failed');
    }
}

function renderAdminUsers(users, plans) {
    const usersEl = document.getElementById('admin-users');
    if (!usersEl) return;
    if (!users.length) {
        usersEl.innerHTML = '<p style="color:var(--text-muted);text-align:center;padding:20px">No users found</p>';
        return;
    }
    const createForm = `<div class="settings-form admin-create-user">
        <div class="form-row three-col">
            <div class="form-group"><label for="new-user-username">Username</label><input id="new-user-username" class="text-input" placeholder="username"></div>
            <div class="form-group"><label for="new-user-email">Email</label><input id="new-user-email" class="text-input" placeholder="user@example.com"></div>
            <div class="form-group"><label for="new-user-password">Password</label><input id="new-user-password" type="password" class="text-input" placeholder="Upper/lower/number/symbol"></div>
        </div>
        <div class="form-row three-col">
            <div class="form-group"><label for="new-user-role">Role</label><select id="new-user-role" class="select-input"><option value="user">User</option><option value="admin">Admin</option></select></div>
            <div class="form-group"><label for="new-user-balance">Balance USDT</label><input id="new-user-balance" type="number" min="0" step="0.01" value="0" class="text-input"></div>
            <div class="form-group"><label for="new-user-live">Live Trading</label><select id="new-user-live" class="select-input"><option value="false">Disabled</option><option value="true">Allowed</option></select></div>
        </div>
        <div class="form-row three-col">
            <div class="form-group"><label for="new-user-max-leverage">Max Leverage</label><input id="new-user-max-leverage" type="number" min="1" max="125" step="1" value="20" class="text-input"></div>
            <div class="form-group"><label for="new-user-max-position">Max Position %</label><input id="new-user-max-position" type="number" min="0.1" max="100" step="0.1" value="10" class="text-input"></div>
            <div class="form-group admin-button-bottom"><button class="btn btn-primary" onclick="createAdminUser()"><i class="ri-user-add-line"></i> Add User</button></div>
        </div>
    </div>`;
    usersEl.innerHTML = `${createForm}<div class="table-wrapper"><table class="data-table admin-users-table"><thead><tr><th>Account</th><th>Role</th><th>Status</th><th>Balance</th><th>Live Controls</th><th>Password</th><th>Current Subscription</th><th>Grant Subscription</th><th>Actions</th></tr></thead><tbody>${users.map(u => {
        const id = escapeHtml(u.id);
        const jsId = escapeJsSingle(u.id);
        const active = Boolean(u.is_active);
        const sub = u.subscription ? `<strong>${escapeHtml(u.subscription.plan_name || u.subscription.plan_id)}</strong><br><span class="hint">Until ${escapeHtml(formatDateTime(u.subscription.end_date))}</span>` : '<span style="color:var(--text-muted)">None</span>';
        return `<tr>
            <td data-label="Account"><div class="admin-stack"><input id="admin-username-${id}" class="text-input table-input" value="${escapeHtml(u.username)}" autocomplete="off"><input id="admin-email-${id}" class="text-input table-input" value="${escapeHtml(u.email)}" autocomplete="off">${u.totp_enabled ? '<span style="display:inline-flex;align-items:center;gap:4px;font-size:11px;color:#22c55e;margin-top:2px"><i class="ri-shield-check-line"></i>2FA</span>' : ''}</div></td>
            <td data-label="Role"><select id="admin-role-${id}" class="select-input table-input"><option value="user" ${u.role === 'user' ? 'selected' : ''}>User</option><option value="admin" ${u.role === 'admin' ? 'selected' : ''}>Admin</option></select></td>
            <td data-label="Status"><select id="admin-active-${id}" class="select-input table-input"><option value="true" ${active ? 'selected' : ''}>Active</option><option value="false" ${!active ? 'selected' : ''}>Disabled</option></select></td>
            <td data-label="Balance"><input id="admin-balance-${id}" type="number" class="text-input table-input" value="${Number(u.balance_usdt || 0).toFixed(2)}" min="0" step="0.01"></td>
            <td data-label="Live Controls"><div class="admin-stack"><select id="admin-live-${id}" class="select-input table-input"><option value="false" ${!u.live_trading_allowed ? 'selected' : ''}>Paper only</option><option value="true" ${u.live_trading_allowed ? 'selected' : ''}>Live allowed</option></select><div class="admin-inline"><input id="admin-max-lev-${id}" type="number" class="text-input table-input" min="1" max="125" value="${escapeHtml(u.max_leverage || 20)}"><input id="admin-max-pos-${id}" type="number" class="text-input table-input" min="0.1" max="100" step="0.1" value="${escapeHtml(u.max_position_pct || 10)}"></div></div></td>
            <td data-label="Password"><div class="admin-stack"><input id="admin-password-${id}" type="password" class="text-input table-input" placeholder="New password" autocomplete="new-password"><button class="btn btn-sm btn-primary" onclick="resetAdminPassword('${jsId}')">Reset</button></div></td>
            <td data-label="Subscription">${sub}</td>
            <td data-label="Grant Plan"><div class="admin-stack"><select id="admin-plan-${id}" class="select-input table-input">${planOptions(plans)}</select><div class="admin-inline"><input id="admin-duration-${id}" type="number" class="text-input table-input" min="0" step="1" placeholder="Plan days"><select id="admin-substatus-${id}" class="select-input table-input"><option value="active">Active</option><option value="pending">Pending</option></select></div><button class="btn btn-sm btn-primary" onclick="grantSubscription('${jsId}')">Grant</button></div></td>
            <td data-label="Actions"><div class="admin-actions"><button class="btn btn-sm btn-success" onclick="saveAdminUser('${jsId}')">Save</button>${u.role !== 'admin' ? `<button class="btn btn-sm" onclick="toggleUser('${jsId}')">${active ? 'Disable' : 'Enable'}</button>` : ''}<button class="btn btn-sm btn-danger" onclick="deleteAdminUser('${jsId}')">Delete</button></div></td>
        </tr>`;
    }).join('')}</tbody></table></div>`;
}

function renderAdminPaymentAddresses(addresses) {
    const el = document.getElementById('admin-payment-addresses');
    if (!el) return;
    el.innerHTML = `<div class="settings-form admin-mini-form">${USDT_PAYMENT_NETWORKS.map(n => {
        const address = addresses[n.id]?.address || '';
        return `<div class="form-row admin-address-row">
            <div class="form-group"><label>${escapeHtml(n.name)}</label><input id="pay-address-${n.id}" class="text-input" value="${escapeHtml(address)}" placeholder="USDT receiving address"></div>
            <div class="form-group admin-button-bottom"><button class="btn btn-primary" onclick="savePaymentAddress('${n.id}')"><i class="ri-save-line"></i> Save</button></div>
        </div>`;
    }).join('')}</div>`;
}

function renderAdminRegistration(registration, invites) {
    const el = document.getElementById('admin-registration');
    if (!el) return;
    const rows = invites.length ? invites.map(c => {
        const active = c.is_active && Number(c.used_count || 0) < Number(c.max_uses || 0);
        const status = active ? 'active' : 'inactive';
        return `<tr><td><code>${escapeHtml(c.code)}</code></td><td>${escapeHtml(c.used_count || 0)} / ${escapeHtml(c.max_uses || 1)}</td><td>${escapeHtml(c.expires_at || '--')}</td><td>${escapeHtml(c.note || '')}</td><td><span class="badge badge-${status}">${status}</span></td><td><button class="btn btn-sm" onclick="copyText('${escapeJsSingle(c.code)}','Invite code copied')">Copy</button></td></tr>`;
    }).join('') : '<tr><td colspan="6" class="empty-state">No invite codes yet</td></tr>';
    el.innerHTML = `<div class="settings-form">
        <label class="checkbox-label"><input type="checkbox" id="admin-invite-required" ${registration.invite_required ? 'checked' : ''}><span>Require invite code for new registrations</span></label>
        <div class="form-row"><button class="btn btn-success" onclick="saveRegistrationSettings()"><i class="ri-save-line"></i> Save Registration Settings</button></div>
        <div class="form-row three-col admin-create-row">
            <div class="form-group"><label for="invite-max-uses">Max Uses</label><input type="number" id="invite-max-uses" class="text-input" value="1" min="1" max="1000"></div>
            <div class="form-group"><label for="invite-expires">Expires</label><input type="date" id="invite-expires" class="text-input"></div>
            <div class="form-group"><label for="invite-note">Note</label><input type="text" id="invite-note" class="text-input" placeholder="Optional"></div>
        </div>
        <div class="form-row"><button class="btn btn-primary" onclick="createInviteCode()"><i class="ri-key-2-line"></i> Generate Invite Code</button></div>
        <div class="table-wrapper mt-4"><table class="data-table"><thead><tr><th>Code</th><th>Uses</th><th>Expires</th><th>Note</th><th>Status</th><th>Copy</th></tr></thead><tbody>${rows}</tbody></table></div>
    </div>`;
}

function renderAdminRedeemCodes(codes, plans) {
    const el = document.getElementById('admin-redeem-codes');
    if (!el) return;
    const rows = codes.length ? codes.map(c => {
        const parts = [];
        if (c.plan_name) parts.push(escapeHtml(c.plan_name));
        if (Number(c.balance_usdt || 0) > 0) parts.push(`${formatNum(c.balance_usdt)} USDT`);
        const status = (!c.is_active || c.redeemed_by) ? 'inactive' : 'active';
        return `<tr><td><code>${escapeHtml(c.code)}</code></td><td>${parts.length ? parts.join(' + ') : '--'}</td><td>${escapeHtml(c.redeemed_by_username || '--')}</td><td>${escapeHtml(c.expires_at || '--')}</td><td><span class="badge badge-${status}">${status === 'active' ? 'Active' : 'Used'}</span></td><td><button class="btn btn-sm" onclick="copyText('${escapeJsSingle(c.code)}','Card code copied')">Copy</button></td></tr>`;
    }).join('') : '<tr><td colspan="6" class="empty-state">No card codes yet</td></tr>';
    el.innerHTML = `<div class="settings-form">
        <div class="form-row three-col admin-create-row">
            <div class="form-group"><label for="redeem-plan">Subscription Plan</label><select id="redeem-plan" class="select-input">${planOptions(plans, '', 'No subscription')}</select></div>
            <div class="form-group"><label for="redeem-duration">Duration Override</label><input type="number" id="redeem-duration" class="text-input" value="0" min="0" step="1"><p class="hint">0 uses the plan duration</p></div>
            <div class="form-group"><label for="redeem-balance">Balance USDT</label><input type="number" id="redeem-balance" class="text-input" value="0" min="0" step="0.01"></div>
        </div>
        <div class="form-row two-col admin-create-row">
            <div class="form-group"><label for="redeem-expires">Expires</label><input type="date" id="redeem-expires" class="text-input"></div>
            <div class="form-group"><label for="redeem-note">Note</label><input type="text" id="redeem-note" class="text-input" placeholder="Optional"></div>
        </div>
        <div class="form-row"><button class="btn btn-primary" onclick="createRedeemCode()"><i class="ri-coupon-3-line"></i> Generate Card Code</button></div>
        <div class="table-wrapper mt-4"><table class="data-table"><thead><tr><th>Code</th><th>Benefit</th><th>Redeemed By</th><th>Expires</th><th>Status</th><th>Copy</th></tr></thead><tbody>${rows}</tbody></table></div>
    </div>`;
}

function renderAdminPendingPayments(payments) {
    const pendingPayments = payments.filter(p => p.status === 'submitted');
    const payEl = document.getElementById('admin-payments');
    if (!payEl) return;
    if (pendingPayments.length) {
        payEl.innerHTML = `<div class="table-wrapper"><table class="data-table"><thead><tr><th>User</th><th>Amount</th><th>Network</th><th>TX Hash</th><th>Date</th><th>Actions</th></tr></thead><tbody>${pendingPayments.map(p => `<tr><td>${escapeHtml(p.username||'--')}</td><td>${escapeHtml(p.amount)} ${escapeHtml(p.currency)}</td><td>${escapeHtml(p.network)}</td><td><code style="font-size:11px">${p.tx_hash?escapeHtml(p.tx_hash.slice(0,20))+'...':'--'}</code></td><td>${escapeHtml(formatDateTime(p.created_at))}</td><td><div class="admin-actions"><button class="btn btn-sm btn-primary" onclick="adminVerifyPayment('${escapeJsSingle(p.id)}')">Verify</button><button class="btn btn-sm btn-success" onclick="adminConfirmPayment('${escapeJsSingle(p.id)}')">Confirm</button><button class="btn btn-sm btn-danger" onclick="adminRejectPayment('${escapeJsSingle(p.id)}')">Reject</button></div></td></tr>`).join('')}</tbody></table></div>`;
    } else {
        payEl.innerHTML = '<p style="color:var(--text-muted);text-align:center;padding:20px">No pending payments</p>';
    }
}

function renderAdminPlans(plans) {
    const el = document.getElementById('admin-plans');
    if (!el) return;
    const rows = plans.length ? plans.map(p => {
        const features = (p.features && p.features.length) ? p.features.map(f => `<span class="badge badge-feature">${escapeHtml(f)}</span>`).join(' ') : '<span class="hint">No features</span>';
        const active = Boolean(p.is_active);
        return `<tr>
            <td><strong>${escapeHtml(p.name)}</strong></td>
            <td>${formatNum(p.price_usdt)} USDT</td>
            <td>${escapeHtml(p.duration_days)} days</td>
            <td>${escapeHtml(p.max_signals_per_day || 'Unlimited')}</td>
            <td><div class="feature-tags">${features}</div></td>
            <td><span class="badge badge-${active ? 'active' : 'inactive'}">${active ? 'Active' : 'Inactive'}</span></td>
            <td><div class="admin-actions">
                <button class="btn btn-sm btn-primary" onclick="editPlan('${escapeJsSingle(p.id)}')">Edit</button>
                <button class="btn btn-sm ${active ? 'btn-warning' : 'btn-success'}" onclick="togglePlanActive('${escapeJsSingle(p.id)}', ${!active})">${active ? 'Disable' : 'Enable'}</button>
                <button class="btn btn-sm btn-danger" onclick="deletePlan('${escapeJsSingle(p.id)}', '${escapeJsSingle(p.name)}')">Delete</button>
            </div></td>
        </tr>`;
    }).join('') : '<tr><td colspan="7" class="empty-state">No plans yet. Create one below.</td></tr>';

    const formFeatures = '<input type="text" id="plan-features-input" class="text-input" placeholder="Feature1, Feature2, Feature3">';
    el.innerHTML = `<div class="settings-form">
        <div class="form-row" style="font-size:13px;color:var(--text-secondary);margin-bottom:4px"><i class="ri-add-circle-line"></i> <strong>Create / Edit Plan</strong> <span id="plan-edit-title"></span></div>
        <div class="form-row three-col admin-create-row">
            <div class="form-group"><label for="plan-name">Plan Name</label><input id="plan-name" class="text-input" placeholder="e.g. Pro Monthly"></div>
            <div class="form-group"><label for="plan-price">Price USDT</label><input id="plan-price" type="number" class="text-input" min="0" step="0.01" value="0"></div>
            <div class="form-group"><label for="plan-duration">Duration Days</label><input id="plan-duration" type="number" class="text-input" min="1" step="1" value="30"></div>
        </div>
        <div class="form-row three-col admin-create-row">
            <div class="form-group"><label for="plan-signals">Max Signals / Day</label><input id="plan-signals" type="number" class="text-input" min="0" step="1" value="0" placeholder="0 = unlimited"></div>
            <div class="form-group"><label for="plan-features-input">Features</label>${formFeatures}<p class="hint">Comma-separated feature list</p></div>
            <div class="form-group admin-button-bottom"><label>&nbsp;</label>
                <input type="hidden" id="plan-edit-id" value="">
                <div style="display:flex;gap:8px">
                    <button class="btn btn-primary" onclick="savePlan()"><i class="ri-save-line"></i> <span id="plan-save-label">Create Plan</span></button>
                    <button class="btn btn-secondary" id="plan-cancel-btn" style="display:none" onclick="cancelEditPlan()">Cancel</button>
                </div>
            </div>
        </div>
        <div class="table-wrapper mt-4"><table class="data-table"><thead><tr><th>Plan</th><th>Price</th><th>Duration</th><th>Signals/Day</th><th>Features</th><th>Status</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table></div>
    </div>`;
}

function resetPlanForm() {
    document.getElementById('plan-edit-id').value = '';
    document.getElementById('plan-name').value = '';
    document.getElementById('plan-price').value = '0';
    document.getElementById('plan-duration').value = '30';
    document.getElementById('plan-signals').value = '0';
    document.getElementById('plan-features-input').value = '';
    document.getElementById('plan-save-label').textContent = 'Create Plan';
    document.getElementById('plan-cancel-btn').style.display = 'none';
    document.getElementById('plan-edit-title').textContent = '';
}

function editPlan(planId) {
    const plans = window._adminPlans || [];
    const plan = plans.find(p => p.id === planId);
    if (!plan) return;
    document.getElementById('plan-edit-id').value = planId;
    document.getElementById('plan-name').value = plan.name || '';
    document.getElementById('plan-price').value = Number(plan.price_usdt || 0).toFixed(2);
    document.getElementById('plan-duration').value = plan.duration_days || 30;
    document.getElementById('plan-signals').value = plan.max_signals_per_day || 0;
    document.getElementById('plan-features-input').value = (plan.features || []).join(', ');
    document.getElementById('plan-save-label').textContent = 'Update Plan';
    document.getElementById('plan-cancel-btn').style.display = '';
    document.getElementById('plan-edit-title').textContent = `— Editing: ${plan.name}`;
    document.getElementById('plan-name').focus();
}

function cancelEditPlan() {
    resetPlanForm();
}

async function savePlan() {
    const editId = document.getElementById('plan-edit-id').value;
    const name = document.getElementById('plan-name').value.trim();
    const price = parseFloat(document.getElementById('plan-price').value) || 0;
    const duration = parseInt(document.getElementById('plan-duration').value) || 30;
    const signals = parseInt(document.getElementById('plan-signals').value) || 0;
    const featuresRaw = document.getElementById('plan-features-input').value;
    const features = featuresRaw ? featuresRaw.split(',').map(s => s.trim()).filter(Boolean) : [];

    if (!name) {
        showToast('Plan name is required.', 'warning', 'Missing Name');
        return;
    }
    if (duration < 1) {
        showToast('Duration must be at least 1 day.', 'warning', 'Invalid Duration');
        return;
    }

    const body = JSON.stringify({ name, description: '', price_usdt: price, duration_days: duration, features, max_signals_per_day: signals, is_active: true });

    try {
        if (editId) {
            await fetchAPI(`/api/admin/plans/${encodeURIComponent(editId)}`, { method: 'PUT', body });
            showToast('Plan updated successfully.', 'success', 'Plan Updated');
        } else {
            await fetchAPI('/api/admin/plans', { method: 'POST', body });
            showToast('Plan created successfully.', 'success', 'Plan Created');
        }
        resetPlanForm();
        await loadAdmin();
    } catch (err) {
        showToast(err.message, 'error', 'Plan Save Failed');
    }
}

async function togglePlanActive(planId, activate) {
    const plans = window._adminPlans || [];
    const plan = plans.find(p => p.id === planId);
    if (!plan) return;
    const action = activate ? 'enable' : 'disable';
    if (!confirm(`${activate ? 'Enable' : 'Disable'} plan "${plan.name}"? ${!activate ? 'It will no longer appear to users.' : ''}`)) return;
    try {
        const body = JSON.stringify({
            name: plan.name,
            description: plan.description || '',
            price_usdt: plan.price_usdt,
            duration_days: plan.duration_days,
            features: plan.features || [],
            max_signals_per_day: plan.max_signals_per_day || 0,
            is_active: activate,
        });
        await fetchAPI(`/api/admin/plans/${encodeURIComponent(planId)}`, { method: 'PUT', body });
        showToast(`Plan ${activate ? 'enabled' : 'disabled'} successfully.`, 'success', 'Plan Updated');
        await loadAdmin();
    } catch (err) {
        showToast(err.message, 'error', 'Plan Update Failed');
    }
}

async function deletePlan(planId, planName) {
    if (!confirm(`Delete or deactivate plan "${planName}"? Deactivation occurs if the plan has existing subscriptions or card codes.`)) return;
    try {
        const result = await fetchAPI(`/api/admin/plans/${encodeURIComponent(planId)}`, { method: 'DELETE' });
        showToast(result.status === 'deleted' ? 'Plan deleted.' : 'Plan deactivated (has references).', result.status === 'deleted' ? 'success' : 'warning', 'Plan Removed');
        await loadAdmin();
    } catch (err) {
        showToast(err.message, 'error', 'Plan Delete Failed');
    }
}

let currentUpdateInfo = null;
let currentUpdateTaskId = null;
let updatePollTimer = null;

function renderAdminUpdate(status) {
    const el = document.getElementById('admin-update');
    const badge = document.getElementById('admin-update-badge');
    if (!el) return;

    const currentVersion = status.current_version || '--';
    const deploymentMode = status.deployment_mode || 'manual';
    const updateSupported = status.update_supported || false;
    const updaterHealthy = status.updater_healthy || false;
    const latestTask = status.latest_task || null;
    const updaterMessage = status.updater_message || 'Updater unavailable';

    if (latestTask && ['queued', 'running'].includes(latestTask.status)) {
        currentUpdateTaskId = latestTask.task_id || currentUpdateTaskId;
        startUpdatePolling(currentUpdateTaskId);
    }

    if (badge) {
        if (latestTask && ['queued', 'running'].includes(latestTask.status)) {
            badge.textContent = latestTask.status === 'running' ? 'Updating' : 'Queued';
            badge.className = 'badge badge-pending';
        } else if (updateSupported) {
            badge.textContent = 'Ready';
            badge.className = 'badge badge-active';
        } else {
            badge.textContent = 'Manual';
            badge.className = 'badge badge-warning';
        }
    }

    const latestTaskHtml = latestTask
        ? `<div style="margin-top:16px;padding:12px;background:rgba(148,163,184,0.06);border-radius:8px;border:1px solid rgba(148,163,184,0.12)">
            <p style="margin:0 0 6px"><strong>Latest Task:</strong> ${escapeHtml(latestTask.task_id || '--')}</p>
            <p style="margin:0;font-size:12px;color:var(--text-secondary)">Status: <span class="badge badge-${safeClassToken(latestTask.status || 'unknown')}">${escapeHtml(latestTask.status || 'unknown')}</span> ${latestTask.target_version ? `→ v${escapeHtml(latestTask.target_version)}` : ''}</p>
        </div>`
        : '';

    el.innerHTML = `
        <div class="settings-form">
            <div class="form-row three-col">
                <div class="form-group">
                    <label>Current Version</label>
                    <div class="metric-value" style="font-size:18px;color:var(--accent-cyan)">v${escapeHtml(currentVersion)}</div>
                </div>
                <div class="form-group">
                    <label>Environment</label>
                    <div class="metric-value">${deploymentMode === 'docker-compose' ? '<i class="ri-box-3-line"></i> Docker Compose' : '<i class="ri-code-line"></i> Manual / Source'}</div>
                </div>
                <div class="form-group">
                    <label>Updater</label>
                    <div class="metric-value">${updaterHealthy ? '<span class="badge badge-active">Online</span>' : `<span class="badge badge-warning">${escapeHtml(updaterMessage)}</span>`}</div>
                </div>
            </div>
            <div class="form-row">
                <button class="btn btn-primary" onclick="checkForUpdate()" id="btn-check-update">
                    <i class="ri-refresh-line"></i> Check for Updates
                </button>
                <button class="btn btn-success" onclick="showUpdateModal()" id="btn-one-click-update" ${updateSupported ? 'style="display:none"' : 'disabled'}>
                    <i class="ri-download-cloud-2-line"></i> One-Click Update
                </button>
                <a href="https://github.com/${escapeHtml(status.github_repo || 'ikun52012/QuantPilot-AI')}/releases" target="_blank" class="btn btn-secondary">
                    <i class="ri-external-link-line"></i> View Releases
                </a>
            </div>
            <p class="hint">One-click update requires the updater sidecar and GHCR image deployment. It queues a rollout task instead of restarting the current HTTP request.</p>
            ${latestTaskHtml}
            <div id="update-result" style="margin-top:16px"></div>
        </div>
    `;

    if (latestTask) {
        renderUpdateTaskResult(latestTask);
    }
}

async function checkForUpdate() {
    const btn = document.getElementById('btn-check-update');
    const resultEl = document.getElementById('update-result');
    const badge = document.getElementById('admin-update-badge');

    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="ri-loader-4-line ri-spin"></i> Checking...';
    }

    try {
        const info = await fetchAPI('/api/admin/check-update');
        currentUpdateInfo = info;

        const hasUpdate = info.has_update;
        const latestVersion = info.latest_version || info.current_version;
        const releaseUrl = info.release_url || '';
        const releaseBody = info.release_body || '';
        const oneClickSupported = info.one_click_supported || false;

        if (badge) {
            if (hasUpdate) {
                badge.textContent = 'Update Available';
                badge.className = 'badge badge-active';
            } else if (info.status === 'error') {
                badge.textContent = 'Error';
                badge.className = 'badge badge-error';
            } else {
                badge.textContent = 'Latest';
                badge.className = 'badge badge-active';
            }
        }

        const oneClickBtn = document.getElementById('btn-one-click-update');
        if (oneClickBtn && hasUpdate && oneClickSupported) {
            oneClickBtn.style.display = '';
        } else if (oneClickBtn) {
            oneClickBtn.style.display = 'none';
        }

        if (resultEl) {
            if (info.status === 'error') {
                resultEl.innerHTML = `
                    <div style="padding:12px;background:rgba(239,68,68,0.06);border-radius:8px;border:1px solid rgba(239,68,68,0.14)">
                        <p style="color:var(--accent-red);margin:0"><i class="ri-error-warning-line"></i> ${escapeHtml(info.message || 'Check failed')}</p>
                    </div>
                `;
            } else if (hasUpdate) {
                const notesHtml = releaseBody
                    ? `<div style="margin-top:12px;padding:12px;background:rgba(16,185,129,0.06);border-radius:8px;border:1px solid rgba(16,185,129,0.14);max-height:200px;overflow-y:auto">
                        <h4 style="margin:0 0 8px;color:var(--accent-green)">Release Notes</h4>
                        <pre style="white-space:pre-wrap;font-size:12px;color:var(--text-secondary);margin:0">${escapeHtml(releaseBody.slice(0, 1000))}${releaseBody.length > 1000 ? '...' : ''}</pre>
                    </div>`
                    : '';
                resultEl.innerHTML = `
                    <div style="padding:16px;background:rgba(16,185,129,0.06);border-radius:8px;border:1px solid rgba(16,185,129,0.14)">
                        <p style="font-size:14px;color:var(--accent-green);margin:0 0 8px"><i class="ri-arrow-up-circle-line"></i> New version available: <strong>v${escapeHtml(latestVersion)}</strong></p>
                        <p style="font-size:12px;color:var(--text-secondary);margin:0">Current: v${escapeHtml(info.current_version)} → Latest: v${escapeHtml(latestVersion)}</p>
                        ${oneClickSupported ? '<p style="font-size:12px;color:var(--accent-cyan);margin:8px 0 0"><i class="ri-information-line"></i> Click "One-Click Update" to queue a rollout task</p>' : '<p style="font-size:12px;color:var(--accent-orange);margin:8px 0 0"><i class="ri-information-line"></i> Automatic rollout is not available for this deployment</p>'}
                    </div>
                    ${notesHtml}
                    <div style="margin-top:12px">
                        <a href="${escapeHtml(releaseUrl)}" target="_blank" class="btn btn-sm btn-secondary">
                            <i class="ri-external-link-line"></i> Full Release Notes
                        </a>
                    </div>
                `;
            } else {
                resultEl.innerHTML = `
                    <div style="padding:12px;background:rgba(59,130,246,0.06);border-radius:8px;border:1px solid rgba(59,130,246,0.14)">
                        <p style="color:var(--accent-indigo);margin:0"><i class="ri-checkbox-circle-line"></i> You are running the latest version v${escapeHtml(latestVersion)}</p>
                    </div>
                `;
            }
        }

    } catch (err) {
        if (resultEl) {
            resultEl.innerHTML = `
                <div style="padding:12px;background:rgba(239,68,68,0.06);border-radius:8px;border:1px solid rgba(239,68,68,0.14)">
                    <p style="color:var(--accent-red);margin:0"><i class="ri-error-warning-line"></i> ${escapeHtml(err.message)}</p>
                </div>
            `;
        }
        showToast(err.message, 'error', 'Check Update Failed');
    }

    if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="ri-refresh-line"></i> Check for Updates';
    }
}

function showUpdateModal() {
    if (!currentUpdateInfo || !currentUpdateInfo.has_update) {
        showToast('No update available', 'warning');
        return;
    }
    if (!currentUpdateInfo.one_click_supported) {
        showToast('One-click update is not available for this deployment', 'warning');
        return;
    }

    const latestVersion = currentUpdateInfo.latest_version;
    const currentVersion = currentUpdateInfo.current_version;

    const modalHtml = `
        <div style="text-align:center">
            <div style="font-size:48px;color:var(--accent-orange);margin-bottom:16px"><i class="ri-download-cloud-2-line"></i></div>
            <h3 style="margin:0 0 8px">Update Confirmation</h3>
            <p style="color:var(--text-secondary);margin:0 0 16px">This will update from <strong>v${escapeHtml(currentVersion)}</strong> to <strong>v${escapeHtml(latestVersion)}</strong></p>
        </div>
        <div style="padding:16px;background:rgba(245,158,11,0.06);border-radius:8px;border:1px solid rgba(245,158,11,0.14);margin-bottom:16px">
            <p style="font-size:13px;color:var(--accent-orange);margin:0"><i class="ri-alert-line"></i> <strong>Warning:</strong></p>
            <ul style="font-size:12px;color:var(--text-secondary);margin:8px 0 0;padding-left:20px">
                <li>The update will be queued and applied by the updater sidecar</li>
                <li>signal-server will restart when the rollout begins</li>
                <li>Active positions may be affected</li>
                <li>Recommend pausing trading before update</li>
                <li>Database backup will be created automatically</li>
            </ul>
        </div>
        <div class="form-group">
            <label class="checkbox-label">
                <input type="checkbox" id="update-backup-checkbox" checked>
                <span>Create backup before update</span>
            </label>
        </div>
        <div class="form-row" style="justify-content:center">
            <button class="btn btn-danger" onclick="performUpdate()" id="btn-confirm-update">
                <i class="ri-download-cloud-2-line"></i> Confirm Update
            </button>
            <button class="btn btn-secondary" onclick="closeUpdateModal()">
                <i class="ri-close-line"></i> Cancel
            </button>
        </div>
    `;

    const modal = document.getElementById('payment-modal');
    const body = document.getElementById('payment-modal-body');
    if (modal && body) {
        body.innerHTML = modalHtml;
        modal.style.display = 'flex';
    }
}

function closeUpdateModal() {
    const modal = document.getElementById('payment-modal');
    if (modal) modal.style.display = 'none';
}

async function performUpdate() {
    const backupCheckbox = document.getElementById('update-backup-checkbox');
    const resultEl = document.getElementById('update-result');

    closeUpdateModal();

    if (resultEl) {
        resultEl.innerHTML = `
            <div style="padding:16px;background:rgba(245,158,11,0.06);border-radius:8px;border:1px solid rgba(245,158,11,0.14)">
                <p style="color:var(--accent-orange);margin:0"><i class="ri-loader-4-line ri-spin"></i> Queueing update...</p>
                <p style="font-size:12px;color:var(--text-secondary);margin:8px 0 0">The updater sidecar will pull the new image and restart the application.</p>
            </div>
        `;
    }

    try {
        const result = await fetchAPI('/api/admin/perform-update', {
            method: 'POST',
            body: JSON.stringify({
                confirm: true,
                backup_before_update: backupCheckbox ? backupCheckbox.checked : true,
            }),
        });
        currentUpdateTaskId = result.task_id || null;
        if (resultEl) {
            resultEl.innerHTML = `
                <div style="padding:16px;background:rgba(59,130,246,0.06);border-radius:8px;border:1px solid rgba(59,130,246,0.14)">
                    <p style="color:var(--accent-blue);margin:0 0 8px"><i class="ri-time-line"></i> Update queued</p>
                    <p style="font-size:12px;color:var(--text-secondary);margin:0">Task ID: ${escapeHtml(result.task_id || '--')}. Polling updater status now.</p>
                </div>
            `;
        }
        startUpdatePolling(currentUpdateTaskId);
        showToast('Update queued successfully', 'success');

    } catch (err) {
        if (resultEl) {
            resultEl.innerHTML = `
                <div style="padding:16px;background:rgba(239,68,68,0.06);border-radius:8px;border:1px solid rgba(239,68,68,0.14)">
                    <p style="color:var(--accent-red);margin:0"><i class="ri-error-warning-line"></i> Update failed: ${escapeHtml(err.message)}</p>
                </div>
            `;
        }
        showToast(err.message, 'error', 'Update Failed');
    }
}

async function pollUpdateTask(taskId) {
    if (!taskId) return;
    try {
        const task = await fetchAPI(`/api/admin/update-task/${encodeURIComponent(taskId)}`);
        renderUpdateTaskResult(task);
        const status = String(task.status || '').toLowerCase();
        if (['completed', 'failed'].includes(status)) {
            stopUpdatePolling();
            await refreshAdminUpdatePanel();
        }
    } catch (err) {
        const resultEl = document.getElementById('update-result');
        if (resultEl) {
            resultEl.innerHTML = `<div style="padding:12px;background:rgba(239,68,68,0.06);border-radius:8px;border:1px solid rgba(239,68,68,0.14)"><p style="color:var(--accent-red);margin:0"><i class="ri-error-warning-line"></i> Failed to poll update task: ${escapeHtml(err.message)}</p></div>`;
        }
        stopUpdatePolling();
    }
}

function startUpdatePolling(taskId) {
    if (!taskId) return;
    currentUpdateTaskId = taskId;
    stopUpdatePolling();
    pollUpdateTask(taskId);
    updatePollTimer = window.setInterval(() => pollUpdateTask(taskId), 5000);
}

function stopUpdatePolling() {
    if (updatePollTimer) {
        window.clearInterval(updatePollTimer);
        updatePollTimer = null;
    }
}

async function refreshAdminUpdatePanel() {
    try {
        const status = await fetchAPI('/api/admin/update-status');
        renderAdminUpdate(status || {});
    } catch (err) {
        console.error('Failed to refresh update panel', err);
    }
}

function renderUpdateTaskResult(task) {
    const resultEl = document.getElementById('update-result');
    if (!resultEl || !task) return;

    const logs = Array.isArray(task.log) ? task.log : [];
    const status = String(task.status || 'unknown').toLowerCase();
    const statusClass = status === 'completed' ? 'active' : status === 'failed' ? 'error' : 'pending';
    const title = status === 'completed'
        ? 'Update completed'
        : status === 'failed'
            ? 'Update failed'
            : status === 'running'
                ? 'Update running'
                : 'Update queued';
    const summary = task.message || '';
    const logHtml = logs.length
        ? logs.map(log => `<div style="font-size:12px;padding:4px 0">${escapeHtml(log)}</div>`).join('')
        : '<div style="font-size:12px;color:var(--text-secondary)">No logs yet.</div>';

    resultEl.innerHTML = `
        <div style="padding:16px;background:rgba(148,163,184,0.06);border-radius:8px;border:1px solid rgba(148,163,184,0.12)">
            <p style="margin:0 0 8px"><span class="badge badge-${statusClass}">${escapeHtml(status)}</span> <strong>${escapeHtml(title)}</strong></p>
            <p style="font-size:12px;color:var(--text-secondary);margin:0">${escapeHtml(summary)}</p>
            <p style="font-size:11px;color:var(--text-muted);margin:8px 0 0">Task: ${escapeHtml(task.task_id || '--')} ${task.target_version ? `· target v${escapeHtml(task.target_version)}` : ''}</p>
        </div>
        <div style="margin-top:12px;padding:12px;background:var(--card-bg);border-radius:8px;border:1px solid rgba(255,255,255,0.08)">
            <h4 style="margin:0 0 8px;font-size:13px">Update Log</h4>
            ${logHtml}
        </div>
        ${status === 'completed' ? '<div style="margin-top:12px"><button class="btn btn-primary" onclick="location.reload()"><i class="ri-refresh-line"></i> Refresh Page</button></div>' : ''}
    `;
}

function renderAdminSystem(system, auditLogs, aiCosts = {}) {
    const el = document.getElementById('admin-system');
    if (!el) return;
    const storage = system.storage || {};
    const storageRows = Object.entries(storage)
        .map(([name, item]) => `<tr><td>${escapeHtml(name)}</td><td>${escapeHtml(item.path || '')}</td><td><span class="badge badge-${item.writable === false ? 'error' : 'active'}">${item.writable === false ? 'Blocked' : 'OK'}</span></td></tr>`)
        .join('');
    const auditRows = auditLogs.length ? auditLogs.map(a => `<tr><td>${escapeHtml(formatDateTime(a.created_at))}</td><td>${escapeHtml(a.admin_username || '--')}</td><td>${escapeHtml(a.action)}</td><td>${escapeHtml(a.target_type || '')}:${escapeHtml(a.target_id || '')}</td><td>${escapeHtml(a.summary || '')}</td></tr>`).join('') : '<tr><td colspan="5" class="empty-state">No audit events yet</td></tr>';
    
    const costs = aiCosts.costs || {};
    const totalCost = costs.total_cost_usd || 0;
    const totalRequests = costs.total_requests || 0;
    const byProvider = costs.by_provider || {};
    const providerRows = Object.entries(byProvider).map(([p, c]) => `<div class="metric-item"><span class="metric-label">${escapeHtml(p.toUpperCase())}</span><span class="metric-value">$${formatNum(c.cost || 0)} (${c.requests || 0} req)</span></div>`).join('');

    el.innerHTML = `
        <div class="metrics-table">
            <div class="metric-item"><span class="metric-label">Version</span><span class="metric-value">${escapeHtml(system.version || '--')}</span></div>
            <div class="metric-item"><span class="metric-label">Commit</span><span class="metric-value">${escapeHtml(system.commit || '--')}</span></div>
            <div class="metric-item"><span class="metric-label">Webhook</span><span class="metric-value">${escapeHtml(system.webhook_url || '--')}</span></div>
            <div class="metric-item"><span class="metric-label">Live Trading</span><span class="metric-value">${system.live_trading ? 'YES' : 'NO'}</span></div>
        </div>
        <div class="metrics-table mt-4" style="background:rgba(59,130,246,0.06);padding:12px;border-radius:8px">
            <div style="font-size:13px;font-weight:600;margin-bottom:8px"><i class="ri-robot-line"></i> AI Costs Summary</div>
            <div class="metric-item"><span class="metric-label">Total Cost</span><span class="metric-value">$${formatNum(totalCost)}</span></div>
            <div class="metric-item"><span class="metric-label">Total Requests</span><span class="metric-value">${totalRequests}</span></div>
            ${providerRows}
        </div>
        <div class="table-wrapper mt-4"><table class="data-table"><thead><tr><th>Storage</th><th>Path</th><th>Status</th></tr></thead><tbody>${storageRows}</tbody></table></div>
        <div class="table-wrapper mt-4"><table class="data-table"><thead><tr><th>Time</th><th>Admin</th><th>Action</th><th>Target</th><th>Summary</th></tr></thead><tbody>${auditRows}</tbody></table></div>
    `;
}

function renderAdminWebhookEvents(events) {
    const el = document.getElementById('admin-webhook-events');
    if (!el) return;
    const rows = events.length ? events.map(e => {
        const payload = e.payload || {};
        return `<tr><td>${escapeHtml(formatDateTime(e.created_at))}</td><td>${escapeHtml(e.username || 'admin/global')}</td><td>${escapeHtml(e.ticker || payload.ticker || '--')}</td><td>${escapeHtml(e.direction || payload.direction || '--')}</td><td><span class="badge badge-${safeClassToken(e.status)}">${escapeHtml(e.status)}</span></td><td>${escapeHtml(e.reason || '')}</td><td>${escapeHtml(e.client_ip || '')}</td></tr>`;
    }).join('') : '<tr><td colspan="7" class="empty-state">No webhook events yet</td></tr>';
    el.innerHTML = `<div class="table-wrapper"><table class="data-table"><thead><tr><th>Time</th><th>User</th><th>Ticker</th><th>Direction</th><th>Status</th><th>Reason</th><th>IP</th></tr></thead><tbody>${rows}</tbody></table></div>`;
}

function renderAdminPositionMonitor(state) {
    const el = document.getElementById('admin-position-monitor');
    if (!el) return;
    const keys = Object.keys(state || {}).filter(k => k !== 'last_run_at');
    const rows = keys.length ? keys.slice(-20).reverse().map(k => {
        const item = state[k] || {};
        return `<tr><td>${escapeHtml(k)}</td><td>${escapeHtml(item.stop_price || '--')}</td><td>${escapeHtml(item.paper ? 'paper' : 'live')}</td><td>${escapeHtml(formatDateTime(item.updated_at))}</td></tr>`;
    }).join('') : '<tr><td colspan="4" class="empty-state">No trailing-stop adjustments yet</td></tr>';
    el.innerHTML = `<div class="settings-form"><div class="form-row"><button class="btn btn-primary" onclick="runPositionMonitor()"><i class="ri-play-line"></i> Run Monitor Now</button><span class="hint">Last run: ${escapeHtml(formatDateTime(state.last_run_at))}</span></div><div class="table-wrapper mt-4"><table class="data-table"><thead><tr><th>Trade Rule</th><th>Stop</th><th>Mode</th><th>Updated</th></tr></thead><tbody>${rows}</tbody></table></div></div>`;
}

function renderAdminFilterThresholds(thresholds) {
    const el = document.getElementById('admin-filter-thresholds');
    if (!el) return;

    const current = thresholds.thresholds || thresholds.current || {};
    const defaults = thresholds.weights || thresholds.defaults || {};

    const thresholdFields = [
        ['atr_pct_max', 'ATR% Max', 'Extreme volatility threshold'],
        ['spread_pct_max', 'Spread% Max', 'Maximum acceptable spread'],
        ['volume_24h_min', 'Min Volume (USD)', 'Minimum 24h volume'],
        ['price_change_1h_max', '1h Price Change Max', 'Large sudden move threshold'],
        ['rsi_long_max', 'RSI Long Max', 'Block longs above this RSI'],
        ['rsi_short_min', 'RSI Short Min', 'Block shorts below this RSI'],
        ['funding_rate_threshold', 'Funding Rate Threshold', 'Extreme funding rate'],
        ['orderbook_long_min', 'Orderbook Long Min', 'Min bid/ask ratio for long'],
        ['orderbook_short_max', 'Orderbook Short Max', 'Max bid/ask ratio for short'],
        ['signal_saturation_max', 'Signal Saturation', 'Max same-direction signals/h'],
        ['ema_diff_pct_min', 'EMA Diff Min', 'Min EMA divergence pct'],
        ['consecutive_loss_max', 'Max Consecutive Losses', 'Cool-off after N losses'],
        ['cooldown_seconds', 'Cooldown Seconds', 'Duplicate signal cooldown'],
        ['cooldown_win_multiplier', 'Cooldown Win Mult', 'Reduce cooldown after win'],
        ['cooldown_loss_multiplier', 'Cooldown Loss Mult', 'Increase cooldown after loss'],
        ['price_deviation_pct_max', 'Price Deviation Max', 'Max signal/market price diff'],
        ['oi_change_pct_max', 'OI Change Max', 'Max open interest change pct'],
        ['correlated_asset_change_max', 'Correlated Asset Max', 'Max BTC/ETH 1h change'],
        ['whale_threshold_usd', 'Whale Threshold', 'Min whale transfer USD'],
        ['liquidation_distance_pct_min', 'Liq Distance Min', 'Min distance to liquidation'],
        ['long_short_ratio_extreme_high', 'L/S Ratio High', 'Extreme long ratio'],
        ['long_short_ratio_extreme_low', 'L/S Ratio Low', 'Extreme short ratio'],
        ['basis_pct_max', 'Basis Max', 'Max spot/futures difference'],
        ['fear_greed_extreme_threshold', 'F&G Extreme', 'Fear & Greed extreme level'],
        ['cvd_divergence_threshold', 'CVD Divergence', 'Min CVD divergence strength'],
        ['volatility_regime_multiplier', 'Vol Regime Mult', 'Volatility regime threshold'],
        ['position_reduce_on_loss_pct', 'Reduce on Loss', 'Position reduction after loss'],
        ['min_pass_score', 'Min Pass Score', 'Minimum score for scoring mode'],
        ['data_completeness_soft_fail_count', 'Missing Data Limit', 'Max missing data checks'],
        ['max_same_direction_positions', 'Max Same-Dir Positions', 'Correlation risk: max positions same direction'],
        ['max_correlated_exposure_pct', 'Max Correlated Exp', 'Correlation risk: max exposure pct'],
        ['max_live_missing_data_checks', 'Max Missing Checks', 'Live mode: max missing data'],
        ['margin_mode', 'Margin Mode', 'cross (全仓) or isolated (逐仓) - affects position margin'],
    ];

    const inputs = thresholdFields.map(([key, label, hint]) => {
        const value = current[key] ?? defaults[key] ?? '';
        // Special handling for margin_mode (select dropdown)
        if (key === 'margin_mode') {
            const crossSel = value === 'cross' ? 'selected' : '';
            const isolatedSel = value === 'isolated' ? 'selected' : '';
            return `<div class="form-group">
                <label for="threshold-${key}">${escapeHtml(label)}</label>
                <select id="threshold-${key}" class="text-input">
                    <option value="cross" ${crossSel}>Cross (全仓)</option>
                    <option value="isolated" ${isolatedSel}>Isolated (逐仓)</option>
                </select>
                <p class="hint">${escapeHtml(hint)}</p>
            </div>`;
        }
        // Special handling for boolean fields
        if (key === 'dynamic_cooldown_enabled' || key === 'block_live_on_risk_check_error') {
            const trueSel = value === true || value === 'true' ? 'selected' : '';
            const falseSel = value === false || value === 'false' ? 'selected' : '';
            return `<div class="form-group">
                <label for="threshold-${key}">${escapeHtml(label)}</label>
                <select id="threshold-${key}" class="text-input">
                    <option value="true" ${trueSel}>Enabled</option>
                    <option value="false" ${falseSel}>Disabled</option>
                </select>
                <p class="hint">${escapeHtml(hint)}</p>
            </div>`;
        }
        return `<div class="form-group">
            <label for="threshold-${key}">${escapeHtml(label)}</label>
            <input type="number" id="threshold-${key}" class="text-input" value="${value}" step="0.1">
            <p class="hint">${escapeHtml(hint)}</p>
        </div>`;
    }).join('');

    el.innerHTML = `
        <div class="settings-form">
            <div class="form-row three-col">${inputs}</div>
            <div class="form-row">
                <button class="btn btn-primary" onclick="saveFilterThresholds()"><i class="ri-save-line"></i> Save Thresholds</button>
                <button class="btn btn-secondary" onclick="loadFilterThresholds()"><i class="ri-refresh-line"></i> Refresh</button>
                <button class="btn btn-warning" onclick="resetFilterThresholds()"><i class="ri-restart-line"></i> Reset to Defaults</button>
            </div>
            <div style="margin-top:16px;padding:12px;background:rgba(59,130,246,0.06);border-radius:8px;border:1px solid rgba(59,130,246,0.14)">
                <p style="font-size:12px;color:var(--text-secondary);margin:0">
                    <i class="ri-information-line" style="color:var(--accent-indigo)"></i>
                    <strong>Dynamic thresholds:</strong> BTC/ETH have stricter thresholds. High-volatility assets use relaxed thresholds.
                </p>
            </div>
        </div>`;
}

function renderAdminFilterStats(stats) {
    const el = document.getElementById('admin-filter-stats');
    if (!el) return;

    const summary = stats.summary || {};
    const detailed = stats.statistics || {};

    const summaryRows = Object.entries(summary).map(([check, data]) => {
        const topTickers = (data.top_tickers || []).map(([t, c]) => `${escapeHtml(t)}:${escapeHtml(c)}`).join(', ');
        return `<tr>
            <td><strong>${escapeHtml(check)}</strong></td>
            <td>${escapeHtml(data.total_blocks || 0)}</td>
            <td style="font-size:11px;color:var(--text-muted)">${escapeHtml(topTickers || '--')}</td>
        </tr>`;
    }).join('');

    el.innerHTML = `
        <div class="settings-form">
            <div class="form-row">
                <button class="btn btn-warning" onclick="resetFilterStats()"><i class="ri-delete-bin-line"></i> Reset Statistics</button>
                <button class="btn btn-secondary" onclick="loadFilterStats()"><i class="ri-refresh-line"></i> Refresh</button>
            </div>
            <div class="table-wrapper mt-4">
                <table class="data-table">
                    <thead><tr><th>Filter Check</th><th>Total Blocks</th><th>Top Tickers</th></tr></thead>
                    <tbody>${summaryRows || '<tr><td colspan="3" class="empty-state">No blocking statistics yet</td></tr>'}</tbody>
                </table>
            </div>
        </div>`;
}

async function loadFilterThresholds() {
    try {
        const thresholds = await fetchAPI('/api/admin/filter-thresholds');
        renderAdminFilterThresholds(thresholds);
    } catch (err) {
        showToast(err.message, 'error', 'Load Failed');
    }
}

async function saveFilterThresholds() {
    const thresholdFields = [
        'atr_pct_max', 'spread_pct_max', 'volume_24h_min', 'price_change_1h_max',
        'rsi_long_max', 'rsi_short_min', 'funding_rate_threshold', 'orderbook_long_min',
        'orderbook_short_max', 'signal_saturation_max', 'ema_diff_pct_min',
        'consecutive_loss_max', 'cooldown_seconds', 'cooldown_win_multiplier',
        'cooldown_loss_multiplier', 'price_deviation_pct_max', 'oi_change_pct_max',
        'correlated_asset_change_max', 'whale_threshold_usd', 'liquidation_distance_pct_min',
        'long_short_ratio_extreme_high', 'long_short_ratio_extreme_low', 'basis_pct_max',
        'fear_greed_extreme_threshold', 'cvd_divergence_threshold', 'volatility_regime_multiplier',
        'position_reduce_on_loss_pct', 'min_pass_score', 'data_completeness_soft_fail_count',
        'max_same_direction_positions', 'max_correlated_exposure_pct', 'max_live_missing_data_checks',
        'margin_mode', 'dynamic_cooldown_enabled', 'block_live_on_risk_check_error'
    ];

    const data = {};
    thresholdFields.forEach(key => {
        const el = document.getElementById(`threshold-${key}`);
        if (!el) return;
        const raw = String(el.value ?? '').trim();
        // Special handling for string/boolean fields
        if (key === 'margin_mode') {
            data[key] = raw === '' ? null : raw;
        } else if (key === 'dynamic_cooldown_enabled' || key === 'block_live_on_risk_check_error') {
            data[key] = raw === 'true';
        } else {
            data[key] = raw === '' ? null : readNumberInput(`threshold-${key}`, null);
        }
    });

    try {
        await fetchAPI('/api/admin/filter-thresholds', { method: 'POST', body: JSON.stringify(data) });
        showToast('Filter thresholds saved.', 'success', 'Saved');
    } catch (err) {
        showToast(err.message, 'error', 'Save Failed');
    }
}

async function resetFilterThresholds() {
    if (!confirm('Reset all thresholds to default values?')) return;

    const thresholdFields = [
        'atr_pct_max', 'spread_pct_max', 'volume_24h_min', 'price_change_1h_max',
        'rsi_long_max', 'rsi_short_min', 'funding_rate_threshold', 'orderbook_long_min',
        'orderbook_short_max', 'signal_saturation_max', 'ema_diff_pct_min',
        'consecutive_loss_max', 'cooldown_seconds', 'cooldown_win_multiplier',
        'cooldown_loss_multiplier', 'price_deviation_pct_max', 'oi_change_pct_max',
        'correlated_asset_change_max', 'whale_threshold_usd', 'liquidation_distance_pct_min',
        'long_short_ratio_extreme_high', 'long_short_ratio_extreme_low', 'basis_pct_max',
        'fear_greed_extreme_threshold', 'cvd_divergence_threshold', 'volatility_regime_multiplier',
        'position_reduce_on_loss_pct', 'min_pass_score', 'data_completeness_soft_fail_count',
        'max_same_direction_positions', 'max_correlated_exposure_pct', 'max_live_missing_data_checks'
    ];

    const defaults = {
        atr_pct_max: 15.0, spread_pct_max: 0.1, volume_24h_min: 1000000, price_change_1h_max: 8.0,
        rsi_long_max: 80, rsi_short_min: 20, funding_rate_threshold: 0.0005,
        orderbook_long_min: 0.4, orderbook_short_max: 2.5, signal_saturation_max: 3,
        ema_diff_pct_min: 1.0, consecutive_loss_max: 3, cooldown_seconds: 300,
        cooldown_win_multiplier: 0.5, cooldown_loss_multiplier: 2.0, price_deviation_pct_max: 2.0,
        oi_change_pct_max: 15.0, correlated_asset_change_max: 5.0, whale_threshold_usd: 1000000,
        liquidation_distance_pct_min: 1.0, long_short_ratio_extreme_high: 2.5,
        long_short_ratio_extreme_low: 0.4, basis_pct_max: 0.5, fear_greed_extreme_threshold: 20,
        cvd_divergence_threshold: 15.0, volatility_regime_multiplier: 1.5,
        position_reduce_on_loss_pct: 50.0, min_pass_score: 0.0, data_completeness_soft_fail_count: 5,
        max_same_direction_positions: 5, max_correlated_exposure_pct: 50.0,
        max_live_missing_data_checks: 0, margin_mode: 'cross',
        dynamic_cooldown_enabled: true, block_live_on_risk_check_error: true
    };

    thresholdFields.forEach(key => {
        const el = document.getElementById(`threshold-${key}`);
        if (el) el.value = defaults[key] ?? '';
    });

    showToast('Thresholds reset to defaults. Click Save to apply.', 'info', 'Reset');
}

function renderAdminRiskThresholds(thresholds) {
    const el = document.getElementById('admin-risk-thresholds');
    if (!el) return;

    const fields = [
        ['max_daily_trades', 'Max Daily Trades', 'Maximum total trades per day (0=unlimited)'],
        ['max_daily_loss_pct', 'Max Daily Loss %', 'Stop trading if daily loss exceeds this % of equity'],
        ['max_position_pct', 'Max Position %', 'Maximum single position size as % of equity'],
        ['risk_per_trade_pct', 'Risk Per Trade %', 'Amount to risk per trade as % of equity'],
        ['live_data_quality_mode', 'Live Data Quality', 'How strictly to validate market data in live mode'],
    ];

    const inputs = fields.map(([key, label, hint]) => {
        const value = thresholds[key] ?? '';
        if (key === 'live_data_quality_mode') {
            const strictSel = value === 'strict' ? 'selected' : '';
            const normalSel = value === 'normal' || !value ? 'selected' : '';
            const lenientSel = value === 'lenient' ? 'selected' : '';
            return `<div class="form-group">
                <label for="risk-${key}">${escapeHtml(label)}</label>
                <select id="risk-${key}" class="text-input">
                    <option value="strict" ${strictSel}>Strict (strict)</option>
                    <option value="normal" ${normalSel}>Normal (normal)</option>
                    <option value="lenient" ${lenientSel}>Lenient (lenient)</option>
                </select>
                <p class="hint">${escapeHtml(hint)}</p>
            </div>`;
        }
        const step = key.includes('pct') ? '0.1' : '1';
        return `<div class="form-group">
            <label for="risk-${key}">${escapeHtml(label)}</label>
            <input type="number" id="risk-${key}" class="text-input" value="${value}" step="${step}" min="0">
            <p class="hint">${escapeHtml(hint)}</p>
        </div>`;
    }).join('');

    el.innerHTML = `<div class="settings-form">
        <div class="form-row" style="font-size:13px;color:var(--text-secondary);margin-bottom:8px"><i class="ri-information-line"></i> These thresholds directly control trade execution safety limits. Changes take effect immediately.</div>
        <div class="form-row three-col">${inputs}</div>
        <div class="form-row">
            <button class="btn btn-primary" onclick="saveRiskThresholds()"><i class="ri-save-line"></i> Save Risk Thresholds</button>
        </div>
    </div>`;
}

async function saveRiskThresholds() {
    const fields = ['max_daily_trades', 'max_daily_loss_pct', 'max_position_pct', 'risk_per_trade_pct', 'live_data_quality_mode'];
    const data = {};
    fields.forEach(key => {
        const el = document.getElementById(`risk-${key}`);
        if (!el) return;
        const raw = String(el.value ?? '').trim();
        if (key === 'live_data_quality_mode') {
            data[key] = raw || null;
        } else {
            data[key] = raw === '' ? null : readNumberInput(`risk-${key}`, null);
        }
    });
    try {
        await fetchAPI('/api/admin/risk-thresholds', { method: 'POST', body: JSON.stringify(data) });
        showToast('Risk thresholds saved.', 'success', 'Saved');
    } catch (err) {
        showToast(err.message, 'error', 'Save Failed');
    }
}

async function loadRiskThresholds() {
    try {
        const thresholds = await fetchAPI('/api/admin/risk-thresholds');
        renderAdminRiskThresholds(thresholds);
    } catch (err) {
        showToast(err.message, 'error', 'Load Failed');
    }
}

async function loadFilterStats() {
    try {
        const stats = await fetchAPI('/api/admin/filter-stats');
        renderAdminFilterStats(stats);
    } catch (err) {
        showToast(err.message, 'error', 'Load Failed');
    }
}

async function resetFilterStats() {
    if (!confirm('Clear all filter blocking statistics?')) return;
    try {
        await fetchAPI('/api/admin/filter-stats/reset', { method: 'POST' });
        showToast('Filter statistics reset.', 'success', 'Reset');
        loadFilterStats();
    } catch (err) {
        showToast(err.message, 'error', 'Reset Failed');
    }
}

function renderAdminExternalAPIKeys(keys) {
    const el = document.getElementById('admin-external-api-keys');
    if (!el) return;

    const descriptions = keys.description || {};

    const keyConfigs = [
        {
            name: 'whale_alert_api_key',
            label: 'Whale Alert API Key',
            configured: keys.whale_alert_configured,
            masked: keys.whale_alert_api_key,
            desc: descriptions.whale_alert,
            free: true,
        },
        {
            name: 'etherscan_api_key',
            label: 'Etherscan API Key',
            configured: keys.etherscan_configured,
            masked: keys.etherscan_api_key,
            desc: descriptions.etherscan,
            free: true,
        },
        {
            name: 'glassnode_api_key',
            label: 'Glassnode API Key',
            configured: keys.glassnode_configured,
            masked: keys.glassnode_api_key,
            desc: descriptions.glassnode,
            free: false,
        },
        {
            name: 'cryptoquant_api_key',
            label: 'CryptoQuant API Key',
            configured: keys.cryptoquant_configured,
            masked: keys.cryptoquant_api_key,
            desc: descriptions.cryptoquant,
            free: false,
        },
    ];

    const rows = keyConfigs.map(cfg => {
        const statusBadge = cfg.configured
            ? `<span class="badge badge-active">Configured</span>`
            : `<span class="badge badge-inactive">Not Set</span>`;

        const freeBadge = cfg.free
            ? `<span class="badge badge-success" style="font-size:10px">FREE</span>`
            : `<span class="badge badge-warning" style="font-size:10px">Paid</span>`;

        const deleteBtn = cfg.configured
            ? `<button class="btn btn-sm btn-danger" onclick="deleteExternalAPIKey('${cfg.name}')"><i class="ri-delete-bin-line"></i></button>`
            : '';

        return `<tr>
            <td>
                <strong>${escapeHtml(cfg.label)}</strong>
                ${freeBadge}
            </td>
            <td>${statusBadge}</td>
            <td style="font-family:monospace;font-size:11px">${escapeHtml(cfg.masked || '--')}</td>
            <td style="font-size:11px;color:var(--text-muted)">${escapeHtml(cfg.desc || '')}</td>
            <td>
                <input type="password" id="api-key-${cfg.name}" class="text-input" placeholder="Enter new key" style="width:150px">
                <button class="btn btn-sm btn-primary" onclick="saveExternalAPIKey('${cfg.name}')"><i class="ri-save-line"></i></button>
                ${deleteBtn}
            </td>
        </tr>`;
    }).join('');

    el.innerHTML = `
        <div class="settings-form">
            <div class="table-wrapper">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>API Service</th>
                            <th>Status</th>
                            <th>Key Preview</th>
                            <th>Description</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
            <div style="margin-top:16px;padding:12px;background:rgba(16,185,129,0.06);border-radius:8px;border:1px solid rgba(16,185,129,0.14)">
                <h4 style="margin:0 0 8px;font-size:13px;color:var(--accent-green)"><i class="ri-information-line"></i> Free API Keys Guide</h4>
                <ul style="margin:0;padding-left:20px;font-size:12px;color:var(--text-secondary)">
                    <li><strong>Whale Alert:</strong> <a href="https://whale-alert.io" target="_blank">whale-alert.io</a> → Free tier 500 requests/day</li>
                    <li><strong>Etherscan:</strong> <a href="https://etherscan.io/apis" target="_blank">etherscan.io/apis</a> → Free tier 5 calls/sec</li>
                    <li>Keys are encrypted and stored securely in database</li>
                    <li>Glassnode/CryptoQuant require paid subscriptions for full data</li>
                </ul>
            </div>
        </div>`;
}

function renderAdminEnhancedFilters(settings) {
    const el = document.getElementById('admin-enhanced-filters');
    if (!el) return;

    const desc = settings.description || {};

    el.innerHTML = `
        <div class="settings-form">
            <div class="form-group">
                <label class="checkbox-label">
                    <input type="checkbox" id="enhanced-filters-enabled" ${settings.enhanced_filters_enabled ? 'checked' : ''}>
                    <span>Enable Enhanced Whale & On-chain Filters</span>
                </label>
                <p class="hint">${escapeHtml(desc.enhanced_filters_enabled || 'Activates whale activity, correlated assets, and OI change checks')}</p>
            </div>

            <div class="form-row three-col">
                <div class="form-group">
                    <label for="whale-threshold">Whale Threshold (USD)</label>
                    <input type="number" id="whale-threshold" class="text-input" value="${settings.whale_threshold_usd || 1000000}" min="100000" step="100000">
                    <p class="hint">${escapeHtml(desc.whale_threshold_usd || 'Min USD for whale transfer')}</p>
                </div>
                <div class="form-group">
                    <label for="correlated-threshold">Correlated Change %</label>
                    <input type="number" id="correlated-threshold" class="text-input" value="${settings.correlated_threshold_pct || 5}" min="1" max="20" step="0.5">
                    <p class="hint">${escapeHtml(desc.correlated_threshold_pct || 'BTC/ETH change threshold')}</p>
                </div>
                <div class="form-group">
                    <label for="oi-threshold">OI Change %</label>
                    <input type="number" id="oi-threshold" class="text-input" value="${settings.oi_change_threshold_pct || 15}" min="5" max="50" step="1">
                    <p class="hint">${escapeHtml(desc.oi_change_threshold_pct || 'Open Interest change threshold')}</p>
                </div>
            </div>

            <div class="form-row">
                <button class="btn btn-primary" onclick="saveEnhancedFilters()"><i class="ri-save-line"></i> Save Enhanced Filters</button>
                <button class="btn btn-secondary" onclick="loadEnhancedFilters()"><i class="ri-refresh-line"></i> Refresh</button>
            </div>

            <div style="margin-top:16px;padding:12px;background:rgba(59,130,246,0.06);border-radius:8px;border:1px solid rgba(59,130,246,0.14)">
                <p style="font-size:12px;color:var(--text-secondary);margin:0">
                    <i class="ri-information-line" style="color:var(--accent-indigo)"></i>
                    <strong>Enhanced filters require external API keys:</strong>
                    Configure Whale Alert (free) or Etherscan (free) keys above to enable whale tracking.
                    OI and correlated asset checks use exchange public data (no API key required).
                </p>
            </div>
        </div>`;
}

async function loadExternalAPIKeys() {
    try {
        const keys = await fetchAPI('/api/admin/external-api-keys');
        renderAdminExternalAPIKeys(keys);
    } catch (err) {
        showToast(err.message, 'error', 'Load Failed');
    }
}

async function saveExternalAPIKey(keyName) {
    const input = document.getElementById(`api-key-${keyName}`);
    const value = input?.value?.trim() || '';

    if (!value) {
        showToast('Please enter an API key.', 'warning', 'Missing Key');
        return;
    }

    const data = { [keyName]: value };

    try {
        await fetchAPI('/api/admin/external-api-keys', { method: 'POST', body: JSON.stringify(data) });
        showToast(`${keyName.replace('_api_key', '')} key saved.`, 'success', 'Saved');
        input.value = '';
        loadExternalAPIKeys();
    } catch (err) {
        showToast(err.message, 'error', 'Save Failed');
    }
}

async function deleteExternalAPIKey(keyName) {
    if (!confirm(`Delete ${keyName}? This will disable related features.`)) return;

    try {
        await fetchAPI(`/api/admin/external-api-keys/${keyName}`, { method: 'DELETE' });
        showToast(`${keyName.replace('_api_key', '')} key deleted.`, 'warning', 'Deleted');
        loadExternalAPIKeys();
    } catch (err) {
        showToast(err.message, 'error', 'Delete Failed');
    }
}

async function loadEnhancedFilters() {
    try {
        const settings = await fetchAPI('/api/admin/enhanced-filters');
        renderAdminEnhancedFilters(settings);
    } catch (err) {
        showToast(err.message, 'error', 'Load Failed');
    }
}

async function saveEnhancedFilters() {
    const enabled = document.getElementById('enhanced-filters-enabled')?.checked || false;
    const whaleThreshold = parseFloat(document.getElementById('whale-threshold')?.value) || 1000000;
    const correlatedThreshold = parseFloat(document.getElementById('correlated-threshold')?.value) || 5;
    const oiThreshold = parseFloat(document.getElementById('oi-threshold')?.value) || 15;

    const data = {
        enhanced_filters_enabled: enabled,
        whale_threshold_usd: whaleThreshold,
        correlated_threshold_pct: correlatedThreshold,
        oi_change_threshold_pct: oiThreshold,
    };

    try {
        await fetchAPI('/api/admin/enhanced-filters', { method: 'POST', body: JSON.stringify(data) });
        showToast('Enhanced filter settings saved.', 'success', 'Saved');
    } catch (err) {
        showToast(err.message, 'error', 'Save Failed');
    }
}

function renderAdminBackups(backups) {
    const el = document.getElementById('admin-backups');
    if (!el) return;
    const rows = backups.length ? backups.map(b => `<tr><td>${escapeHtml(b.filename)}</td><td>${formatNum(Number(b.size || 0) / 1024)} KB</td><td>${escapeHtml(formatDateTime(b.created_at))}</td><td><div class="admin-actions"><button class="btn btn-sm" onclick="downloadBackup('${escapeJsSingle(b.filename)}')">Download</button><button class="btn btn-sm btn-warning" onclick="stageRestore('${escapeJsSingle(b.filename)}')">Stage Restore</button><button class="btn btn-sm btn-danger" onclick="restoreBackupPG('${escapeJsSingle(b.filename)}')">Restore PG</button></div></td></tr>`).join('') : '<tr><td colspan="4" class="empty-state">No backups yet</td></tr>';
    el.innerHTML = `<div class="settings-form"><div class="form-row"><button class="btn btn-primary" onclick="createBackup()"><i class="ri-archive-line"></i> Create Backup</button><span class="hint">Restore PG requires PostgreSQL service restart. Stop QuantPilot first.</span></div><div class="table-wrapper mt-4"><table class="data-table"><thead><tr><th>Backup</th><th>Size</th><th>Created</th><th>Actions</th></tr></thead><tbody>${rows}</tbody></table></div></div>`;
}

async function restoreBackupPG(filename) {
    if (!confirm('Restore PostgreSQL database from backup? This will REPLACE the current database. Stop the service first!')) return;
    try {
        await fetchAPI(`/api/admin/backups/${encodeURIComponent(filename)}/restore-pg`, { method: 'POST' });
        showToast('PostgreSQL restore initiated. Check logs for result.', 'success', 'Restore Started');
    } catch (err) {
        showToast(err.message, 'error', 'Restore Failed');
    }
}

function planOptions(plans, selected = '', emptyLabel = 'Select plan...') {
    return `<option value="">${escapeHtml(emptyLabel)}</option>${plans.map(p => `<option value="${escapeHtml(p.id)}" ${p.id === selected ? 'selected' : ''}>${escapeHtml(p.name)} (${formatNum(p.price_usdt)} USDT)</option>`).join('')}`;
}

async function saveAdminUser(userId) {
    const data = {
        username: document.getElementById(`admin-username-${userId}`)?.value || '',
        email: document.getElementById(`admin-email-${userId}`)?.value || '',
        role: document.getElementById(`admin-role-${userId}`)?.value || 'user',
        is_active: document.getElementById(`admin-active-${userId}`)?.value === 'true',
        balance_usdt: parseFloat(document.getElementById(`admin-balance-${userId}`)?.value) || 0,
        live_trading_allowed: document.getElementById(`admin-live-${userId}`)?.value === 'true',
        max_leverage: parseInt(document.getElementById(`admin-max-lev-${userId}`)?.value) || 20,
        max_position_pct: parseFloat(document.getElementById(`admin-max-pos-${userId}`)?.value) || 10,
    };
    try {
        const result = await fetchAPI(`/api/admin/user/${encodeURIComponent(userId)}`, { method:'PUT', body:JSON.stringify(data) });
        if (getUser().id === userId && result.user) {
            _cachedUser = { ..._cachedUser, ...result.user };
            updateUserUI();
        }
        showToast('User account updated.','success','Saved');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Save Failed'); }
}

async function createAdminUser() {
    const data = {
        username: document.getElementById('new-user-username')?.value || '',
        email: document.getElementById('new-user-email')?.value || '',
        password: document.getElementById('new-user-password')?.value || '',
        role: document.getElementById('new-user-role')?.value || 'user',
        is_active: true,
        balance_usdt: parseFloat(document.getElementById('new-user-balance')?.value) || 0,
        live_trading_allowed: document.getElementById('new-user-live')?.value === 'true',
        max_leverage: parseInt(document.getElementById('new-user-max-leverage')?.value) || 20,
        max_position_pct: parseFloat(document.getElementById('new-user-max-position')?.value) || 10,
    };
    try {
        await fetchAPI('/api/admin/users', { method:'POST', body:JSON.stringify(data) });
        showToast('User created.','success','Created');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Create Failed'); }
}

async function deleteAdminUser(userId) {
    if (!confirm('Delete this user and their subscriptions/payments?')) return;
    try {
        await fetchAPI(`/api/admin/user/${encodeURIComponent(userId)}`, { method:'DELETE' });
        showToast('User deleted.','success','Deleted');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Delete Failed'); }
}

async function resetAdminPassword(userId) {
    const input = document.getElementById(`admin-password-${userId}`);
    const password = input?.value || '';
    if (password.length < 8) {
        showToast('Password must be at least 8 characters.','warning','Invalid Password');
        return;
    }
    try {
        await fetchAPI(`/api/admin/user/${encodeURIComponent(userId)}/password`, { method:'POST', body:JSON.stringify({ password }) });
        input.value = '';
        showToast('Password updated.','success','Saved');
    } catch (err) {
        showToast(err.message,'error','Password Reset Failed');
    }
}

async function changePassword() {
    const currentPw = document.getElementById('change-pw-current')?.value || '';
    const newPw = document.getElementById('change-pw-new')?.value || '';
    const confirmPw = document.getElementById('change-pw-confirm')?.value || '';

    if (!currentPw || !newPw || !confirmPw) {
        showToast('All fields are required.', 'warning', 'Missing Fields');
        return;
    }

    if (newPw.length < 8) {
        showToast('New password must be at least 8 characters.', 'warning', 'Weak Password');
        return;
    }

    if (newPw !== confirmPw) {
        showToast('New password and confirmation do not match.', 'warning', 'Mismatch');
        return;
    }

    try {
        await fetchAPI('/api/auth/change-password', {
            method: 'POST',
            body: JSON.stringify({
                current_password: currentPw,
                new_password: newPw,
            }),
        });
        document.getElementById('change-pw-current').value = '';
        document.getElementById('change-pw-new').value = '';
        document.getElementById('change-pw-confirm').value = '';
        showToast('Password updated successfully.', 'success', 'Password Changed');
    } catch (err) {
        showToast(err.message, 'error', 'Password Change Failed');
    }
}

async function grantSubscription(userId) {
    const planId = document.getElementById(`admin-plan-${userId}`)?.value;
    if (!planId) {
        showToast('Choose a subscription plan first.','warning','Missing Plan');
        return;
    }
    const data = {
        plan_id: planId,
        duration_days: parseInt(document.getElementById(`admin-duration-${userId}`)?.value) || 0,
        status: document.getElementById(`admin-substatus-${userId}`)?.value || 'active',
    };
    try {
        await fetchAPI(`/api/admin/user/${encodeURIComponent(userId)}/subscription`, { method:'POST', body:JSON.stringify(data) });
        showToast('Subscription updated.','success','Saved');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Grant Failed'); }
}

async function runPositionMonitor() {
    try {
        const result = await fetchAPI('/api/admin/position-monitor/run', { method:'POST' });
        showToast(`Checked ${result.checked || 0}, adjusted ${result.adjusted || 0}.`, 'success', 'Monitor Complete');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Monitor Failed'); }
}

async function createBackup() {
    try {
        const backup = await fetchAPI('/api/admin/backups', { method:'POST' });
        showToast(backup.filename, 'success', 'Backup Created');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Backup Failed'); }
}

function downloadBackup(filename) {
    window.location.href = `/api/admin/backups/${encodeURIComponent(filename)}`;
}

async function stageRestore(filename) {
    if (!confirm('Stage this backup for restore? You must stop the service before replacing live files.')) return;
    try {
        const result = await fetchAPI(`/api/admin/backups/${encodeURIComponent(filename)}/restore`, { method:'POST' });
        showToast(result.message || result.status, 'warning', 'Restore Staged');
    } catch (err) { showToast(err.message,'error','Restore Failed'); }
}

async function savePaymentAddress(network) {
    const address = document.getElementById(`pay-address-${network}`)?.value.trim();
    if (!address) {
        showToast('Payment address cannot be empty.','warning','Missing Address');
        return;
    }
    try {
        await fetchAPI('/api/admin/payment-addresses', { method:'POST', body:JSON.stringify({ network, address }) });
        showToast(`${network} address saved.`, 'success', 'Saved');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Save Failed'); }
}

async function saveRegistrationSettings() {
    const inviteRequired = document.getElementById('admin-invite-required')?.checked || false;
    try {
        await fetchAPI('/api/admin/registration', { method:'POST', body:JSON.stringify({ invite_required: inviteRequired }) });
        showToast('Registration settings saved.','success','Saved');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Save Failed'); }
}

async function createInviteCode() {
    const data = {
        max_uses: parseInt(document.getElementById('invite-max-uses')?.value) || 1,
        expires_at: document.getElementById('invite-expires')?.value || '',
        note: document.getElementById('invite-note')?.value || '',
    };
    try {
        const created = await fetchAPI('/api/admin/invite-codes', { method:'POST', body:JSON.stringify(data) });
        showToast(created.code, 'success', 'Invite Code Generated');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Generate Failed'); }
}

async function createRedeemCode() {
    const data = {
        plan_id: document.getElementById('redeem-plan')?.value || '',
        duration_days: parseInt(document.getElementById('redeem-duration')?.value) || 0,
        balance_usdt: parseFloat(document.getElementById('redeem-balance')?.value) || 0,
        expires_at: document.getElementById('redeem-expires')?.value || '',
        note: document.getElementById('redeem-note')?.value || '',
    };
    if (!data.plan_id && data.balance_usdt <= 0) {
        showToast('Choose a plan or enter a balance amount.','warning','Missing Benefit');
        return;
    }
    try {
        const created = await fetchAPI('/api/admin/redeem-codes', { method:'POST', body:JSON.stringify(data) });
        showToast(created.code, 'success', 'Card Code Generated');
        loadAdmin();
    } catch (err) { showToast(err.message,'error','Generate Failed'); }
}

async function saveSettings(endpoint, data, btnId) {
    const btn = btnId ? document.getElementById(btnId) : null;
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="ri-loader-4-line"></i> Saving...'; }
    try {
        await fetchAPI(endpoint, { method:'POST', body:JSON.stringify(data) });
        showToast('Settings saved.','success','Saved');
        if (btn) { btn.innerHTML = '<i class="ri-check-line"></i> Saved!'; }
        setTimeout(() => { if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-save-line"></i> Save'; } }, 2000);
    } catch (e) {
        showToast(e.message,'error','Save Failed');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-save-line"></i> Save'; }
    }
}

async function testTelegram() {
    try { await fetchAPI('/api/test-telegram',{method:'POST'}); showToast('Check your Telegram.','success','Test Sent'); }
    catch (e) { showToast(e.message,'error','Test Failed'); }
}

function detectWebhookUrl() {
    const url = `${window.location.origin}/webhook`;
    const el = document.getElementById('webhook-url');
    if (el) el.textContent = url;
}

function copyWebhookUrl(evt) {
    const url = document.getElementById('webhook-url')?.textContent;
    if (url) {
        navigator.clipboard.writeText(url).then(() => showToast(url,'success','Webhook URL copied'));
        const btn = evt?.target?.closest('.btn');
        if (btn) { btn.innerHTML = '<i class="ri-check-line"></i>'; setTimeout(() => { btn.innerHTML = '<i class="ri-file-copy-line"></i>'; }, 1500); }
    }
}
function copyAdminWebhookSecret() {
    const value = document.getElementById('admin-webhook-secret')?.textContent;
    if (value) copyText(value, 'Webhook secret copied');
}
function copyUserWebhookUrl() {
    const value = document.getElementById('user-webhook-url')?.textContent;
    if (value) copyText(value, 'Webhook URL copied');
}
function copyUserWebhookSecret() {
    const value = document.getElementById('user-webhook-secret')?.textContent;
    if (value) copyText(value, 'Webhook secret copied');
}

async function fetchAPI(path, options = {}) {
    const headers = { 'Content-Type': 'application/json', ...(options.headers || {}) };
    const method = String(options.method || 'GET').toUpperCase();
    const csrf = getCookie('tvss_csrf');
    if (!['GET','HEAD','OPTIONS'].includes(method) && csrf) headers['X-CSRF-Token'] = decodeURIComponent(csrf);
    const resp = await fetch(`${API}${path}`, { credentials: 'include', cache: 'no-store', ...options, headers });
    if (resp.status === 401) { redirectToLogin('expired'); throw new Error('Session expired'); }
    if (!resp.ok) {
        const data = await resp.json().catch(()=>({}));
        throw new Error(data.detail || `API error: ${resp.status}`);
    }
    return resp.json();
}

function firstDefined(...values) { return values.find(v => v !== undefined && v !== null); }
function setFieldValue(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value ?? '';
}
function aiKeyConfiguredForProvider(status, provider) {
    const normalized = String(provider || '').toLowerCase();
    if (normalized === 'openai') return Boolean(status.openai_api_configured);
    if (normalized === 'anthropic') return Boolean(status.anthropic_api_configured);
    if (normalized === 'deepseek') return Boolean(status.deepseek_api_configured);
    if (normalized === 'mistral') return Boolean(status.mistral_api_configured);
    if (normalized === 'openrouter') return Boolean(status.openrouter_api_configured);
    if (normalized === 'custom') return Boolean(status.custom_provider_api_configured);
    return Boolean(status.ai_api_configured);
}
function aiKeyMaskedForProvider(status, provider) {
    const normalized = String(provider || '').toLowerCase();
    if (normalized === 'openai') return status.openai_api_key_masked || '';
    if (normalized === 'anthropic') return status.anthropic_api_key_masked || '';
    if (normalized === 'deepseek') return status.deepseek_api_key_masked || '';
    if (normalized === 'mistral') return status.mistral_api_key_masked || '';
    if (normalized === 'openrouter') return status.openrouter_api_key_masked || '';
    if (normalized === 'custom') return status.custom_provider_api_key_masked || '';
    return '';
}
function setSecretPlaceholder(id, configured, emptyText, maskedValue = '') {
    const el = document.getElementById(id);
    if (!el) return;
    el.value = '';
    const masked = String(maskedValue || '').trim();
    el.placeholder = configured ? (masked || '********') : emptyText;
    el.title = configured ? `Saved: ${el.placeholder}` : '';
}

function selectPaymentNetwork(button, subscriptionId, amount) {
    document.querySelectorAll('#pay-network .payment-network-option').forEach(btn => btn.classList.remove('active'));
    button.classList.add('active');
    updatePaymentAddress(subscriptionId, amount);
}
function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value ?? '';
}
function formatTakeProfitLevels(levels, fallback = '--') {
    if (!Array.isArray(levels) || !levels.length) return fallback;
    const parts = levels
        .map(level => Number(level?.price))
        .filter(value => Number.isFinite(value) && value > 0)
        .map(value => `$${formatNum(value)}`);
    return parts.length ? parts.join(' / ') : fallback;
}
function pickBalance(section, quote = 'USDT') {
    if (!section || typeof section !== 'object') return 0;
    return firstDefined(section[quote], section.USDT, section.USD, section.USDC, 0);
}
function formatNum(n) {
    if (n == null || n === '') return '--';
    const value = Number(n);
    if (!Number.isFinite(value)) return '--';
    return value.toLocaleString(undefined,{minimumFractionDigits:2,maximumFractionDigits:2});
}
function formatValue(v) { if (v==='∞'||v===Infinity) return '∞'; if (typeof v==='number') return v.toFixed(2); return v||'--'; }
function formatDateTime(value) {
    if (!value) return '--';
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return String(value);
    return date.toLocaleString();
}
async function refreshAll() {
    const p = document.querySelector('.page.active')?.id?.replace('page-','');
    if (p==='dashboard') await loadDashboard(); else if (p==='positions') await loadPositions();
    else if (p==='user') await loadUserPortal(); else if (p==='history') await loadHistory(); else if (p==='analytics') await loadAnalytics();
    else if (p==='charts') await loadChartPage(); else if (p==='social') await loadSocialPage(); else if (p==='strategy-editor') await loadStrategyEditorPage();
    else if (p==='subscription') await loadSubscription(); else if (p==='admin') await loadAdmin();
    else if (p==='strategies') { await loadStrategiesOverview(); await loadDCAList(); await loadGridList(); await loadStrategyHistory(); }
}

function toggleVotingFields() {
    const enabled = document.getElementById('voting-enabled')?.checked || false;
    const fields = document.getElementById('voting-config-fields');
    if (fields) fields.style.display = enabled ? 'block' : 'none';
}

async function loadVotingConfig() {
    try {
        const config = await fetchAPI('/api/admin/ai/voting-config');
        const enabledEl = document.getElementById('voting-enabled');
        if (enabledEl) enabledEl.checked = Boolean(config.enabled);

        const modelsEl = document.getElementById('voting-models');
        if (modelsEl && config.models) {
            modelsEl.value = config.models.join('\n');
        }

        const strategyEl = document.getElementById('voting-strategy');
        if (strategyEl && config.strategy) {
            strategyEl.value = config.strategy;
        }

        const weightsEl = document.getElementById('voting-weight-models');
        if (weightsEl && config.weights) {
            const weightLines = Object.entries(config.weights).map(([k, v]) => `${k}: ${v}`);
            weightsEl.value = weightLines.join('\n');
        }

        toggleVotingFields();
    } catch (err) {
        console.warn('Could not load voting config:', err);
    }
}

async function saveVotingConfig() {
    const enabled = document.getElementById('voting-enabled')?.checked || false;

    const modelsText = document.getElementById('voting-models')?.value || '';
    const models = modelsText.split('\n').map(m => m.trim()).filter(m => m);

    const strategy = document.getElementById('voting-strategy')?.value || 'weighted';

    const weightsText = document.getElementById('voting-weight-models')?.value || '';
    const weights = {};
    weightsText.split('\n').forEach(line => {
        const parts = line.split(':').map(p => p.trim());
        if (parts.length >= 2 && parts[0]) {
            weights[parts[0]] = parseFloat(parts[1]) || 1.0;
        }
    });

    if (!models.length && enabled) {
        showToast('Please add at least one voting model.', 'warning', 'No Models');
        return;
    }

    const btn = document.getElementById('btn-save-voting');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="ri-loader-4-line"></i> Saving...'; }

    try {
        await fetchAPI('/api/admin/ai/voting-config', {
            method: 'POST',
            body: JSON.stringify({
                enabled,
                models,
                weights,
                strategy,
            })
        });
        showToast(`Voting ${enabled ? 'enabled' : 'disabled'} with ${models.length} models.`, 'success', 'Voting Config Saved');
        if (btn) { btn.innerHTML = '<i class="ri-check-line"></i> Saved!'; }
        setTimeout(() => { if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-save-line"></i> Save Voting Config'; } }, 2000);
    } catch (err) {
        showToast(err.message, 'error', 'Save Failed');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-save-line"></i> Save Voting Config'; }
    }
}

function toggleUserTPLevels() {
    const num = parseInt(document.getElementById('user-tp-levels')?.value) || 1;
    for (let i = 1; i <= 4; i++) {
        const row = document.getElementById(`user-tp-row-${i}`);
        if (row) row.style.display = i <= num ? 'flex' : 'none';
    }
}

let btEquityChart = null;

async function runBacktest() {
    const btn = document.getElementById('btn-run-backtest');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="ri-loader-4-line"></i> Running...'; }

    const request = {
        ticker: document.getElementById('bt-ticker')?.value || 'BTCUSDT',
        timeframe: document.getElementById('bt-timeframe')?.value || '1h',
        days: parseInt(document.getElementById('bt-days')?.value) || 30,
        strategy: document.getElementById('bt-strategy')?.value || 'simple_trend',
        initial_capital: parseFloat(document.getElementById('bt-capital')?.value) || 10000,
        position_size_pct: parseFloat(document.getElementById('bt-position-size')?.value) || 10,
        leverage: parseFloat(document.getElementById('bt-leverage')?.value) || 1,
        stop_loss_pct: parseFloat(document.getElementById('bt-stop-loss')?.value) || 2,
        trailing_mode: document.getElementById('bt-trailing-mode')?.value || 'none',
        fee_pct: parseFloat(document.getElementById('bt-fee')?.value) || 0.04,
        slippage_pct: parseFloat(document.getElementById('bt-slippage')?.value) || 0.01,
    };

    try {
        const result = await fetchAPI('/api/backtest/run', {
            method: 'POST',
            body: JSON.stringify(request)
        });

        displayBacktestResults(result, request);

        if (btn) { btn.innerHTML = '<i class="ri-check-line"></i> Completed!'; }
        setTimeout(() => { if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-play-line"></i> Run Backtest'; } }, 2000);
    } catch (err) {
        showToast(err.message, 'error', 'Backtest Failed');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-play-line"></i> Run Backtest'; }
    }
}

let _backtestAsyncTaskId = null;
let _backtestAsyncPollTimer = null;

async function runBacktestAsync() {
    const btn = document.getElementById('btn-run-backtest-async');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="ri-loader-line"></i> Starting...'; }

    const request = {
        ticker: document.getElementById('bt-ticker')?.value || 'BTCUSDT',
        timeframe: document.getElementById('bt-timeframe')?.value || '1h',
        days: parseInt(document.getElementById('bt-days')?.value) || 30,
        strategy: document.getElementById('bt-strategy')?.value || 'simple_trend',
        initial_capital: parseFloat(document.getElementById('bt-capital')?.value) || 10000,
        position_size_pct: parseFloat(document.getElementById('bt-position-size')?.value) || 10,
        leverage: parseFloat(document.getElementById('bt-leverage')?.value) || 1,
        fee_pct: parseFloat(document.getElementById('bt-fee')?.value) || 0.04,
        slippage_pct: parseFloat(document.getElementById('bt-slippage')?.value) || 0.01,
        async: true,
    };

    try {
        const result = await fetchAPI('/api/backtest/run', {
            method: 'POST',
            body: JSON.stringify(request)
        });

        if (result.task_id) {
            _backtestAsyncTaskId = result.task_id;
            showBacktestProgress(result.task_id, 'Running...');
            startBacktestPolling(result.task_id, request);
            showToast('Async backtest started. Polling for results...', 'success', 'Task Created');
            if (btn) { btn.innerHTML = '<i class="ri-loader-line"></i> Running...'; }
            const cancelBtn = document.getElementById('btn-cancel-backtest-async');
            if (cancelBtn) cancelBtn.style.display = 'inline-block';
        } else {
            displayBacktestResults(result, request);
            if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-loader-line"></i> Run Async (Long)'; }
        }
    } catch (err) {
        showToast(err.message, 'error', 'Async Backtest Failed');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-loader-line"></i> Run Async (Long)'; }
        hideBacktestProgress();
    }
}

function showBacktestProgress(taskId, status) {
    const progressEl = document.getElementById('backtest-progress');
    const taskIdEl = document.getElementById('backtest-task-id');
    const statusEl = document.getElementById('backtest-progress-status');
    const barEl = document.getElementById('backtest-progress-bar');

    if (progressEl) progressEl.style.display = 'block';
    if (taskIdEl) taskIdEl.textContent = taskId;
    if (statusEl) statusEl.textContent = status;
    if (barEl) barEl.style.width = '10%';
}

function hideBacktestProgress() {
    const progressEl = document.getElementById('backtest-progress');
    if (progressEl) progressEl.style.display = 'none';
    const cancelBtn = document.getElementById('btn-cancel-backtest-async');
    if (cancelBtn) cancelBtn.style.display = 'none';
    _backtestAsyncTaskId = null;
    if (_backtestAsyncPollTimer) {
        clearTimeout(_backtestAsyncPollTimer);
        _backtestAsyncPollTimer = null;
    }
}

function startBacktestPolling(taskId, config) {
    let pollCount = 0;
    const maxPolls = 120; // 120 * 2s = 240s = 4 minutes max

    const poll = async () => {
        pollCount++;
        if (pollCount > maxPolls) {
            showToast('Backtest timeout - took too long', 'warning', 'Timeout');
            hideBacktestProgress();
            const btn = document.getElementById('btn-run-backtest-async');
            if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-loader-line"></i> Run Async (Long)'; }
            return;
        }

        try {
            const result = await fetchAPI(`/api/backtest/result/${encodeURIComponent(taskId)}`);

            const barEl = document.getElementById('backtest-progress-bar');
            const statusEl = document.getElementById('backtest-progress-status');

            if (result.status === 'completed') {
                if (barEl) barEl.style.width = '100%';
                if (statusEl) statusEl.textContent = 'Completed!';
                displayBacktestResults(result, config);
                showToast('Async backtest completed!', 'success', 'Results Ready');
                hideBacktestProgress();
                const btn = document.getElementById('btn-run-backtest-async');
                if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-loader-line"></i> Run Async (Long)'; }
                return;
            } else if (result.status === 'failed' || result.status === 'error') {
                showToast(result.error || 'Backtest failed', 'error', 'Task Failed');
                hideBacktestProgress();
                const btn = document.getElementById('btn-run-backtest-async');
                if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-loader-line"></i> Run Async (Long)'; }
                return;
            } else if (result.status === 'running' || result.status === 'pending') {
                const progress = Math.min(90, 10 + (pollCount / maxPolls) * 80);
                if (barEl) barEl.style.width = `${progress}%`;
                if (statusEl) statusEl.textContent = `Running... (${pollCount * 2}s)`;
                _backtestAsyncPollTimer = setTimeout(poll, 2000);
            }
        } catch (err) {
            console.warn('Backtest poll error:', err);
            _backtestAsyncPollTimer = setTimeout(poll, 2000);
        }
    };

    poll();
}

async function cancelBacktestAsync() {
    if (!_backtestAsyncTaskId) return;

    try {
        await fetchAPI(`/api/backtest/result/${encodeURIComponent(_backtestAsyncTaskId)}`, { method: 'DELETE' });
        showToast('Backtest task cancelled.', 'warning', 'Cancelled');
        hideBacktestProgress();
        const btn = document.getElementById('btn-run-backtest-async');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-loader-line"></i> Run Async (Long)'; }
    } catch (err) {
        showToast(err.message, 'error', 'Cancel Failed');
    }
}

function displayBacktestResults(result, config) {
    const resultsCard = document.getElementById('bt-results-card');
    if (resultsCard) resultsCard.style.display = 'block';

    const metrics = result.metrics || {};

    document.getElementById('bt-total-return').textContent = formatPercent(metrics.total_return_pct || 0);
    document.getElementById('bt-win-rate').textContent = formatPercent(metrics.win_rate || 0);
    document.getElementById('bt-max-dd').textContent = formatPercent(metrics.max_drawdown_pct || 0);
    document.getElementById('bt-sharpe').textContent = metrics.sharpe_ratio?.toFixed(2) || '--';
    document.getElementById('bt-pf').textContent = metrics.profit_factor === 'inf' ? '∞' : (metrics.profit_factor?.toFixed(2) || '--');
    document.getElementById('bt-total-trades').textContent = metrics.total_trades || 0;

    document.getElementById('bt-avg-win').textContent = formatPercent(metrics.avg_win_pct || 0);
    document.getElementById('bt-avg-loss').textContent = formatPercent(metrics.avg_loss_pct || 0);
    document.getElementById('bt-largest-win').textContent = formatPercent(metrics.largest_win_pct || 0);
    document.getElementById('bt-largest-loss').textContent = formatPercent(metrics.largest_loss_pct || 0);
    document.getElementById('bt-expectancy').textContent = metrics.expectancy?.toFixed(4) || '--';
    document.getElementById('bt-rr').textContent = metrics.risk_reward_ratio?.toFixed(2) || '--';
    document.getElementById('bt-max-wins').textContent = metrics.max_consecutive_wins || 0;
    document.getElementById('bt-max-losses').textContent = metrics.max_consecutive_losses || 0;
    document.getElementById('bt-cagr').textContent = formatPercent(metrics.cagr_pct || 0);
    document.getElementById('bt-kelly').textContent = metrics.kelly_fraction?.toFixed(4) || '--';

    document.getElementById('bt-execution-time').textContent = `${result.execution_time_ms?.toFixed(0) || 0}ms`;

    renderEquityCurve(result.equity_curve || [], config.initial_capital);
    renderTradeTable(result.trades || []);

    const signals = result.signals || {};
    showToast(`Backtest completed: ${metrics.total_trades} trades, ${formatPercent(metrics.total_return_pct)} return`,
        metrics.total_return_pct >= 0 ? 'success' : 'warning', 'Backtest Results');
}

function renderEquityCurve(equityCurve, initialCapital) {
    const canvas = document.getElementById('bt-equity-chart');
    if (!canvas) return;

    const ctx = canvas.getContext('2d');

    if (btEquityChart) {
        btEquityChart.destroy();
    }

    const labels = equityCurve.map(e => {
        const ts = new Date(e.timestamp);
        return ts.toLocaleDateString();
    }).filter((_, i) => i % Math.max(1, Math.floor(equityCurve.length / 20)) === 0);

    const data = equityCurve.map(e => e.equity).filter((_, i) => i % Math.max(1, Math.floor(equityCurve.length / 20)) === 0);

    btEquityChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: 'Equity (USDT)',
                data: data,
                borderColor: data[data.length - 1] >= initialCapital ? '#10b981' : '#ef4444',
                backgroundColor: 'transparent',
                borderWidth: 2,
                pointRadius: 0,
                pointHoverRadius: 4,
                tension: 0.1,
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: true, position: 'top' },
            },
            scales: {
                x: { display: true, grid: { display: false } },
                y: { display: true, grid: { color: '#1f2937' }, ticks: { callback: v => `$${v.toFixed(0)}` } }
            }
        }
    });
}

function renderTradeTable(trades) {
    const tbody = document.getElementById('bt-trade-list');
    if (!tbody) return;

    tbody.innerHTML = '';

    if (!trades.length) {
        tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:#6b7280">No trades executed</td></tr>';
        return;
    }

    trades.slice(0, 50).forEach((trade, i) => {
        const pnlClass = trade.pnl_pct >= 0 ? 'text-success' : 'text-danger';
        const row = document.createElement('tr');
        row.innerHTML = `
            <td>${i + 1}</td>
            <td><span class="badge ${trade.direction === 'buy' ? 'badge-green' : 'badge-red'}">${trade.direction.toUpperCase()}</span></td>
            <td>${trade.entry_price.toFixed(4)}</td>
            <td>${trade.exit_price.toFixed(4)}</td>
            <td class="${pnlClass}">${formatPercent(trade.pnl_pct)}</td>
            <td class="${pnlClass}">$${trade.pnl_usdt.toFixed(2)}</td>
            <td>$${trade.fees_usdt.toFixed(4)}</td>
            <td>${trade.holding_bars}</td>
            <td>${trade.exit_reason}</td>
        `;
        tbody.appendChild(row);
    });
}

async function compareStrategies() {
    const ticker = document.getElementById('bt-ticker')?.value || 'BTCUSDT';
    const timeframe = document.getElementById('bt-timeframe')?.value || '1h';
    const days = parseInt(document.getElementById('bt-days')?.value) || 30;

    try {
        const result = await fetchAPI(`/api/backtest/compare?ticker=${ticker}&timeframe=${timeframe}&days=${days}`);

        const comparisonCard = document.getElementById('bt-comparison-card');
        if (comparisonCard) comparisonCard.style.display = 'block';

        const table = document.getElementById('bt-comparison-table');
        if (!table) return;

        let html = '<table class="table"><thead><tr><th>Strategy</th><th>Return</th><th>Win Rate</th><th>Max DD</th><th>Sharpe</th><th>Trades</th></tr></thead><tbody>';

        result.comparison.forEach(c => {
            const m = c.metrics || {};
            const returnClass = (m.total_return_pct || 0) >= 0 ? 'text-success' : 'text-danger';
            html += `<tr>
                <td><strong>${c.strategy}</strong></td>
                <td class="${returnClass}">${formatPercent(m.total_return_pct || 0)}</td>
                <td>${formatPercent(m.win_rate || 0)}</td>
                <td>${formatPercent(m.max_drawdown_pct || 0)}</td>
                <td>${m.sharpe_ratio?.toFixed(2) || '--'}</td>
                <td>${c.trades_count || 0}</td>
            </tr>`;
        });

        html += '</tbody></table>';
        table.innerHTML = html;

        showToast('Strategy comparison completed', 'success', 'Comparison');
    } catch (err) {
        showToast(err.message, 'error', 'Comparison Failed');
    }
}

function formatPercent(value) {
    if (typeof value !== 'number') return '--';
    const sign = value >= 0 ? '+' : '';
    return `${sign}${value.toFixed(2)}%`;
}

async function loadStrategiesOverview() {
    try {
        const overview = await fetchAPI('/api/strategies/overview');

        document.getElementById('st-dca-active').textContent = overview.dca?.active_count || 0;
        document.getElementById('st-grid-active').textContent = overview.grid?.active_count || 0;

        const totalPnl = (overview.dca?.total_pnl_usdt || 0) + (overview.grid?.total_pnl_usdt || 0);
        document.getElementById('st-total-pnl').textContent = `$${totalPnl.toFixed(2)}`;
    } catch (err) {
        console.warn('Failed to load strategies overview:', err);
    }
}

async function createDCA() {
    const btn = document.querySelector('button[onclick="createDCA()"]');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="ri-loader-4-line"></i> Creating...'; }

    const request = {
        ticker: document.getElementById('dca-ticker')?.value || 'BTCUSDT',
        direction: document.getElementById('dca-direction')?.value || 'long',
        initial_capital_usdt: parseFloat(document.getElementById('dca-initial-capital')?.value) || 1000,
        max_entries: parseInt(document.getElementById('dca-max-entries')?.value) || 5,
        entry_spacing_pct: parseFloat(document.getElementById('dca-spacing')?.value) || 2,
        sizing_method: document.getElementById('dca-sizing')?.value || 'fixed',
        stop_loss_pct: parseFloat(document.getElementById('dca-sl')?.value) || 10,
        take_profit_pct: parseFloat(document.getElementById('dca-tp')?.value) || 5,
        activation_loss_pct: parseFloat(document.getElementById('dca-activation')?.value) || 1,
        max_total_capital_usdt: parseFloat(document.getElementById('dca-max-capital')?.value) || 5000,
        mode: document.getElementById('dca-mode')?.value || 'average_down',
    };

    try {
        const result = await fetchAPI('/api/strategies/dca/create', {
            method: 'POST',
            body: JSON.stringify(request)
        });

        showToast(`DCA created for ${result.ticker}, entry=${result.initial_entry_price}`, 'success', 'DCA Created');
        loadDCAList();
        loadStrategiesOverview();

        if (btn) { btn.innerHTML = '<i class="ri-check-line"></i> Created!'; }
        setTimeout(() => { if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-add-line"></i> Create DCA'; } }, 2000);
    } catch (err) {
        showToast(err.message, 'error', 'DCA Failed');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-add-line"></i> Create DCA'; }
    }
}

async function loadDCAList() {
    try {
        const result = await fetchAPI('/api/strategies/dca/list');
        const container = document.getElementById('dca-list');

        if (!result.strategies?.length) {
            container.innerHTML = '<div style="color:#6b7280;text-align:center;padding:20px">No active DCA strategies</div>';
            return;
        }

        let html = '<table class="table"><thead><tr><th>ID</th><th>Ticker</th><th>Direction</th><th>Entries</th><th>Avg Entry</th><th>PnL</th><th>Actions</th></tr></thead><tbody>';

        result.strategies.forEach(s => {
            const pnlClass = s.unrealized_pnl_pct >= 0 ? 'text-success' : 'text-danger';
            html += `<tr>
                <td style="font-size:11px">${s.config_id?.slice(-12) || '--'}</td>
                <td>${s.ticker}</td>
                <td><span class="badge ${s.direction === 'long' ? 'badge-green' : 'badge-red'}">${s.direction}</span></td>
                <td>${s.entries_count}/${s.entries_count + s.entries_remaining}</td>
                <td>${s.average_entry_price?.toFixed(4) || '--'}</td>
                <td class="${pnlClass}">${formatPercent(s.unrealized_pnl_pct)}</td>
                <td>
                    <button class="btn btn-sm btn-secondary" onclick="checkDCA('${s.config_id}')"><i class="ri-refresh-line"></i></button>
                    <button class="btn btn-sm btn-danger" onclick="closeDCA('${s.config_id}')"><i class="ri-close-line"></i></button>
                </td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;

        document.getElementById('st-dca-active').textContent = result.active_count || 0;
    } catch (err) {
        console.warn('Failed to load DCA list:', err);
    }
}

async function checkDCA(strategyId) {
    try {
        const result = await fetchAPI(`/api/strategies/dca/check/${strategyId}`, { method: 'POST' });

        if (result.action === 'dca_entry') {
            showToast(`DCA entry #${result.entry_idx} executed`, 'info', 'DCA Update');
        } else if (result.action === 'close') {
            showToast(`DCA closed: ${result.reason}, pnl=${formatPercent(result.pnl_pct)}`,
                result.pnl_pct >= 0 ? 'success' : 'warning', 'DCA Closed');
        }

        loadDCAList();
    } catch (err) {
        showToast(err.message, 'error', 'Check Failed');
    }
}

async function closeDCA(strategyId) {
    if (!confirm('Close this DCA strategy?')) return;

    try {
        const result = await fetchAPI(`/api/strategies/dca/close/${strategyId}`, { method: 'DELETE' });
        showToast(`DCA closed at ${result.close_price}, pnl=$${result.final_pnl_usdt?.toFixed(2)}`, 'success', 'DCA Closed');
        loadDCAList();
        loadStrategiesOverview();
    } catch (err) {
        showToast(err.message, 'error', 'Close Failed');
    }
}

async function createGrid() {
    const btn = document.querySelector('button[onclick="createGrid()"]');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="ri-loader-4-line"></i> Creating...'; }

    const request = {
        ticker: document.getElementById('grid-ticker')?.value || 'BTCUSDT',
        grid_count: parseInt(document.getElementById('grid-count')?.value) || 10,
        total_capital_usdt: parseFloat(document.getElementById('grid-capital')?.value) || 1000,
        grid_spacing_pct: parseFloat(document.getElementById('grid-spacing')?.value) || 1,
        spacing_mode: document.getElementById('grid-spacing-mode')?.value || 'arithmetic',
        mode: document.getElementById('grid-mode')?.value || 'neutral',
        upper_price: parseFloat(document.getElementById('grid-upper')?.value) || 0,
        lower_price: parseFloat(document.getElementById('grid-lower')?.value) || 0,
    };

    try {
        const result = await fetchAPI('/api/strategies/grid/create', {
            method: 'POST',
            body: JSON.stringify(request)
        });

        showToast(`Grid created for ${result.ticker}, ${result.grid_levels} levels`, 'success', 'Grid Created');
        loadGridList();
        loadStrategiesOverview();

        if (btn) { btn.innerHTML = '<i class="ri-check-line"></i> Created!'; }
        setTimeout(() => { if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-add-line"></i> Create Grid'; } }, 2000);
    } catch (err) {
        showToast(err.message, 'error', 'Grid Failed');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-add-line"></i> Create Grid'; }
    }
}

async function aiGenerateDCA() {
    const btn = document.querySelector('button[onclick="aiGenerateDCA()"]');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="ri-loader-4-line"></i> Generating...'; }

    const ticker = document.getElementById('dca-ticker')?.value || 'BTCUSDT';
    const riskLevel = document.getElementById('dca-risk-level')?.value || 'medium';

    try {
        const result = await fetchAPI('/api/strategies/ai/generate', {
            method: 'POST',
            body: JSON.stringify({
                ticker: ticker,
                strategy_type: 'dca',
                risk_level: riskLevel
            })
        });

        const config = result.config;
        if (config.reasoning) {
            showToast(config.reasoning, 'info', 'AI Analysis');
        }

        if (document.getElementById('dca-direction')) document.getElementById('dca-direction').value = config.direction || 'long';
        if (document.getElementById('dca-initial-capital')) document.getElementById('dca-initial-capital').value = Math.round(config.initial_capital_usdt || 1000);
        if (document.getElementById('dca-max-entries')) document.getElementById('dca-max-entries').value = config.max_entries || 5;
        if (document.getElementById('dca-spacing')) document.getElementById('dca-spacing').value = config.entry_spacing_pct || 2;
        if (document.getElementById('dca-sizing')) document.getElementById('dca-sizing').value = config.sizing_method || 'fixed';
        if (document.getElementById('dca-sl')) document.getElementById('dca-sl').value = config.stop_loss_pct || 10;
        if (document.getElementById('dca-tp')) document.getElementById('dca-tp').value = config.take_profit_pct || 5;
        if (document.getElementById('dca-activation')) document.getElementById('dca-activation').value = config.activation_loss_pct || 1;
        if (document.getElementById('dca-max-capital')) document.getElementById('dca-max-capital').value = Math.round(config.max_total_capital_usdt || 5000);
        if (document.getElementById('dca-mode')) document.getElementById('dca-mode').value = config.mode || 'average_down';

        showToast(`AI generated DCA config for ${ticker}`, 'success', 'Config Generated');
        if (btn) { btn.innerHTML = '<i class="ri-check-line"></i> Done!'; }
        setTimeout(() => { if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-magic-line"></i> AI Generate'; } }, 2000);
    } catch (err) {
        showToast(err.message, 'error', 'AI Generate Failed');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-magic-line"></i> AI Generate'; }
    }
}

async function aiGenerateGrid() {
    const btn = document.querySelector('button[onclick="aiGenerateGrid()"]');
    if (btn) { btn.disabled = true; btn.innerHTML = '<i class="ri-loader-4-line"></i> Generating...'; }

    const ticker = document.getElementById('grid-ticker')?.value || 'BTCUSDT';
    const riskLevel = document.getElementById('grid-risk-level')?.value || 'medium';

    try {
        const result = await fetchAPI('/api/strategies/ai/generate', {
            method: 'POST',
            body: JSON.stringify({
                ticker: ticker,
                strategy_type: 'grid',
                risk_level: riskLevel
            })
        });

        const config = result.config;
        if (config.reasoning) {
            showToast(config.reasoning, 'info', 'AI Analysis');
        }

        if (document.getElementById('grid-count')) document.getElementById('grid-count').value = config.grid_count || 15;
        if (document.getElementById('grid-capital')) document.getElementById('grid-capital').value = Math.round(config.total_capital_usdt || 1000);
        if (document.getElementById('grid-spacing')) document.getElementById('grid-spacing').value = config.grid_spacing_pct || 1;
        if (document.getElementById('grid-spacing-mode')) document.getElementById('grid-spacing-mode').value = config.spacing_mode || 'arithmetic';
        if (document.getElementById('grid-mode')) document.getElementById('grid-mode').value = config.mode || 'neutral';
        if (document.getElementById('grid-upper')) document.getElementById('grid-upper').value = config.upper_price || 0;
        if (document.getElementById('grid-lower')) document.getElementById('grid-lower').value = config.lower_price || 0;

        showToast(`AI generated Grid config for ${ticker}`, 'success', 'Config Generated');
        if (btn) { btn.innerHTML = '<i class="ri-check-line"></i> Done!'; }
        setTimeout(() => { if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-magic-line"></i> AI Generate'; } }, 2000);
    } catch (err) {
        showToast(err.message, 'error', 'AI Generate Failed');
        if (btn) { btn.disabled = false; btn.innerHTML = '<i class="ri-magic-line"></i> AI Generate'; }
    }
}

async function loadGridList() {
    try {
        const result = await fetchAPI('/api/strategies/grid/list');
        const container = document.getElementById('grid-list');

        if (!result.strategies?.length) {
            container.innerHTML = '<div style="color:#6b7280;text-align:center;padding:20px">No active grid strategies</div>';
            return;
        }

        let html = '<table class="table"><thead><tr><th>ID</th><th>Ticker</th><th>Range</th><th>Trades</th><th>PnL</th><th>Pending</th><th>Actions</th></tr></thead><tbody>';

        result.strategies.forEach(s => {
            const pnlClass = (s.realized_pnl_usdt || 0) >= 0 ? 'text-success' : 'text-danger';
            html += `<tr>
                <td style="font-size:11px">${s.config_id?.slice(-12) || '--'}</td>
                <td>${s.ticker}</td>
                <td>${s.lower_price?.toFixed(2)} - ${s.upper_price?.toFixed(2)}</td>
                <td>${s.total_trades || 0}</td>
                <td class="${pnlClass}">$${(s.realized_pnl_usdt || 0).toFixed(2)}</td>
                <td>${s.pending_orders || 0}</td>
                <td>
                    <button class="btn btn-sm btn-secondary" onclick="checkGrid('${s.config_id}')"><i class="ri-refresh-line"></i></button>
                    <button class="btn btn-sm btn-danger" onclick="closeGrid('${s.config_id}')"><i class="ri-close-line"></i></button>
                </td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;

        document.getElementById('st-grid-active').textContent = result.active_count || 0;
    } catch (err) {
        console.warn('Failed to load grid list:', err);
    }
}

async function checkGrid(strategyId) {
    try {
        const result = await fetchAPI(`/api/strategies/grid/check/${strategyId}`, { method: 'POST' });

        if (result.trades?.length) {
            showToast(`Grid executed ${result.trades.length} trades`, 'info', 'Grid Update');
        }

        loadGridList();
    } catch (err) {
        showToast(err.message, 'error', 'Check Failed');
    }
}

async function closeGrid(strategyId) {
    if (!confirm('Close this grid strategy?')) return;

    try {
        const result = await fetchAPI(`/api/strategies/grid/close/${strategyId}`, { method: 'DELETE' });
        showToast(`Grid closed, ${result.total_trades} trades, pnl=$${result.final_pnl_usdt?.toFixed(2)}`, 'success', 'Grid Closed');
        loadGridList();
        loadStrategiesOverview();
    } catch (err) {
        showToast(err.message, 'error', 'Close Failed');
    }
}

async function startStrategyMonitor() {
    const interval = parseInt(document.getElementById('monitor-interval')?.value) || 60;

    try {
        const result = await fetchAPI(`/api/strategies/monitor/start?interval_seconds=${interval}`, { method: 'POST' });
        showToast(`Monitor started, checking every ${interval}s`, 'success', 'Monitor');
        document.getElementById('st-monitor-status').textContent = 'Running';
    } catch (err) {
        showToast(err.message, 'error', 'Monitor Failed');
    }
}

async function stopStrategyMonitor() {
    try {
        const result = await fetchAPI('/api/strategies/monitor/stop', { method: 'POST' });
        showToast('Monitor stopped', 'info', 'Monitor');
        document.getElementById('st-monitor-status').textContent = 'Stopped';
    } catch (err) {
        showToast(err.message, 'error', 'Monitor Failed');
    }
}

async function loadStrategyHistory() {
    try {
        const type = document.getElementById('strategy-history-type')?.value || 'all';
        const status = document.getElementById('strategy-history-status')?.value || 'all';
        const result = await fetchAPI(`/api/strategies/history?strategy_type=${type}&status=${status}`);
        const container = document.getElementById('strategy-history-list');

        if (!result.strategies?.length) {
            container.innerHTML = '<div style="color:#6b7280;text-align:center;padding:40px">No strategies found</div>';
            return;
        }

        let html = '<table class="table"><thead><tr><th>ID</th><th>Type</th><th>Ticker</th><th>Status</th><th>Created</th><th>PnL</th><th>Actions</th></tr></thead><tbody>';

        result.strategies.forEach(s => {
            const statusBadge = getStatusBadge(s.status);
            const typeBadge = `<span class="badge ${s.strategy_type === 'dca' ? 'badge-green' : 'badge-blue'}">${s.strategy_type.toUpperCase()}</span>`;
            
            let pnlText = '--';
            let pnlClass = '';
            if (s.strategy_type === 'dca') {
                const pnl = s.unrealized_pnl_usdt || 0;
                pnlText = `$${pnl.toFixed(2)}`;
                pnlClass = pnl >= 0 ? 'text-success' : 'text-danger';
            } else if (s.strategy_type === 'grid') {
                const pnl = (s.realized_pnl_usdt || 0) + (s.unrealized_pnl_usdt || 0);
                pnlText = `$${pnl.toFixed(2)}`;
                pnlClass = pnl >= 0 ? 'text-success' : 'text-danger';
            }

            const created = s.created_at ? new Date(s.created_at).toLocaleString() : '--';

            html += `<tr>
                <td style="font-size:11px">${s.id?.slice(-12) || '--'}</td>
                <td>${typeBadge}</td>
                <td><strong>${escapeHtml(s.ticker)}</strong></td>
                <td>${statusBadge}</td>
                <td style="font-size:11px">${escapeHtml(created)}</td>
                <td class="${pnlClass}">${pnlText}</td>
                <td>
                    <button class="btn btn-sm btn-secondary" onclick="showStrategyDetail('${s.id}')"><i class="ri-eye-line"></i> Detail</button>
                </td>
            </tr>`;
        });

        html += '</tbody></table>';
        container.innerHTML = html;
    } catch (err) {
        console.warn('Failed to load strategy history:', err);
        document.getElementById('strategy-history-list').innerHTML = `<div style="color:#ef4444;text-align:center;padding:20px">Failed to load: ${escapeHtml(err.message)}</div>`;
    }
}

function getStatusBadge(status) {
    const statusMap = {
        'active': 'badge-active',
        'closed': 'badge-inactive',
        'manual_close': 'badge-warning',
        'stop_loss': 'badge-error',
        'take_profit': 'badge-success',
        'draft': 'badge-pending',
    };
    const className = statusMap[status] || 'badge-inactive';
    return `<span class="badge ${className}">${escapeHtml(status.replace('_', ' '))}</span>`;
}

async function showStrategyDetail(strategyId) {
    const modal = document.getElementById('strategy-detail-modal');
    const body = document.getElementById('strategy-detail-body');
    const title = document.getElementById('strategy-detail-title');

    modal.style.display = 'flex';
    body.innerHTML = '<div style="text-align:center;padding:40px;color:#6b7280"><i class="ri-loader-4-line ri-spin" style="font-size:24px"></i><p>Loading...</p></div>';

    try {
        const detail = await fetchAPI(`/api/strategies/detail/${strategyId}`);
        title.textContent = `${detail.strategy_type.toUpperCase()} - ${detail.ticker}`;

        let html = '';

        // Basic Info
        html += `<div style="margin-bottom:24px">
            <h4 style="margin:0 0 12px;color:#3b82f6"><i class="ri-information-line"></i> Basic Information</h4>
            <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px">
                <div style="padding:12px;background:rgba(59,130,246,0.06);border-radius:8px">
                    <div style="font-size:11px;color:#6b7280">Strategy ID</div>
                    <div style="font-size:13px;font-family:monospace">${escapeHtml(detail.id)}</div>
                </div>
                <div style="padding:12px;background:rgba(59,130,246,0.06);border-radius:8px">
                    <div style="font-size:11px;color:#6b7280">Status</div>
                    <div>${getStatusBadge(detail.status)}</div>
                </div>
                <div style="padding:12px;background:rgba(59,130,246,0.06);border-radius:8px">
                    <div style="font-size:11px;color:#6b7280">Created</div>
                    <div style="font-size:13px">${detail.created_at ? new Date(detail.created_at).toLocaleString() : '--'}</div>
                </div>
                <div style="padding:12px;background:rgba(59,130,246,0.06);border-radius:8px">
                    <div style="font-size:11px;color:#6b7280">Last Updated</div>
                    <div style="font-size:13px">${detail.updated_at ? new Date(detail.updated_at).toLocaleString() : '--'}</div>
                </div>
            </div>
        </div>`;

        if (detail.strategy_type === 'dca') {
            // DCA Specific Info
            html += `<div style="margin-bottom:24px">
                <h4 style="margin:0 0 12px;color:#10b981"><i class="ri-arrow-down-circle-line"></i> DCA Configuration</h4>
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px">
                    <div style="padding:12px;background:rgba(16,185,129,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Direction</div>
                        <div style="font-size:14px"><span class="badge ${detail.direction === 'long' ? 'badge-green' : 'badge-red'}">${escapeHtml(detail.direction)}</span></div>
                    </div>
                    <div style="padding:12px;background:rgba(16,185,129,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Sizing Method</div>
                        <div style="font-size:14px">${escapeHtml(detail.sizing_method || 'fixed')}</div>
                    </div>
                    <div style="padding:12px;background:rgba(16,185,129,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Max Entries</div>
                        <div style="font-size:14px">${detail.max_entries || '--'}</div>
                    </div>
                    <div style="padding:12px;background:rgba(16,185,129,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Entries Made</div>
                        <div style="font-size:14px">${detail.entries?.length || 0}</div>
                    </div>
                </div>
            </div>`;

            // PnL Info
            const state = detail.state || {};
            const pnl = state.unrealized_pnl_usdt || 0;
            const pnlPct = state.unrealized_pnl_pct || 0;
            html += `<div style="margin-bottom:24px">
                <h4 style="margin:0 0 12px;color:#f59e0b"><i class="ri-money-dollar-circle-line"></i> Performance</h4>
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px">
                    <div style="padding:12px;background:rgba(245,158,11,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Unrealized PnL</div>
                        <div style="font-size:18px;font-weight:bold" class="${pnl >= 0 ? 'text-success' : 'text-danger'}">$${pnl.toFixed(2)}</div>
                    </div>
                    <div style="padding:12px;background:rgba(245,158,11,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">PnL %</div>
                        <div style="font-size:18px;font-weight:bold" class="${pnlPct >= 0 ? 'text-success' : 'text-danger'}">${pnlPct.toFixed(2)}%</div>
                    </div>
                    <div style="padding:12px;background:rgba(245,158,11,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Avg Entry Price</div>
                        <div style="font-size:14px">${state.average_entry_price ? '$' + state.average_entry_price.toFixed(6) : '--'}</div>
                    </div>
                    <div style="padding:12px;background:rgba(245,158,11,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Total Invested</div>
                        <div style="font-size:14px">${state.total_invested_usdt ? '$' + state.total_invested_usdt.toFixed(2) : '--'}</div>
                    </div>
                </div>
            </div>`;

            // Entry History
            if (detail.entries?.length) {
                html += `<div style="margin-bottom:24px">
                    <h4 style="margin:0 0 12px;color:#8b5cf6"><i class="ri-list-check"></i> Entry History</h4>
                    <table class="table"><thead><tr><th>#</th><th>Price</th><th>Quantity</th><th>Time</th></tr></thead><tbody>`;
                detail.entries.forEach((entry, idx) => {
                    const time = entry.entry_time ? new Date(entry.entry_time).toLocaleString() : '--';
                    html += `<tr>
                        <td>${idx + 1}</td>
                        <td>${entry.price?.toFixed(6) || '--'}</td>
                        <td>${entry.quantity?.toFixed(6) || '--'}</td>
                        <td style="font-size:11px">${escapeHtml(time)}</td>
                    </tr>`;
                });
                html += '</tbody></table></div>';
            }
        } else if (detail.strategy_type === 'grid') {
            // Grid Specific Info
            const state = detail.state || {};
            const config = detail.config || {};
            const realizedPnl = state.realized_pnl_usdt || 0;
            const unrealizedPnl = state.unrealized_pnl_usdt || 0;
            const totalPnl = realizedPnl + unrealizedPnl;

            html += `<div style="margin-bottom:24px">
                <h4 style="margin:0 0 12px;color:#10b981"><i class="ri-grid-line"></i> Grid Configuration</h4>
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px">
                    <div style="padding:12px;background:rgba(16,185,129,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Mode</div>
                        <div style="font-size:14px">${escapeHtml(detail.mode || 'neutral')}</div>
                    </div>
                    <div style="padding:12px;background:rgba(16,185,129,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Spacing</div>
                        <div style="font-size:14px">${escapeHtml(detail.spacing_mode || 'arithmetic')}</div>
                    </div>
                    <div style="padding:12px;background:rgba(16,185,129,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Grid Levels</div>
                        <div style="font-size:14px">${config.grid_count || state.grid_levels?.length || '--'}</div>
                    </div>
                    <div style="padding:12px;background:rgba(16,185,129,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Total Trades</div>
                        <div style="font-size:14px">${state.total_trades || 0}</div>
                    </div>
                </div>
            </div>`;

            html += `<div style="margin-bottom:24px">
                <h4 style="margin:0 0 12px;color:#f59e0b"><i class="ri-money-dollar-circle-line"></i> Performance</h4>
                <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px">
                    <div style="padding:12px;background:rgba(245,158,11,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Realized PnL</div>
                        <div style="font-size:18px;font-weight:bold" class="${realizedPnl >= 0 ? 'text-success' : 'text-danger'}">$${realizedPnl.toFixed(2)}</div>
                    </div>
                    <div style="padding:12px;background:rgba(245,158,11,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Unrealized PnL</div>
                        <div style="font-size:18px;font-weight:bold" class="${unrealizedPnl >= 0 ? 'text-success' : 'text-danger'}">$${unrealizedPnl.toFixed(2)}</div>
                    </div>
                    <div style="padding:12px;background:rgba(245,158,11,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Total PnL</div>
                        <div style="font-size:18px;font-weight:bold" class="${totalPnl >= 0 ? 'text-success' : 'text-danger'}">$${totalPnl.toFixed(2)}</div>
                    </div>
                    <div style="padding:12px;background:rgba(245,158,11,0.06);border-radius:8px">
                        <div style="font-size:11px;color:#6b7280">Pending Orders</div>
                        <div style="font-size:14px">${state.pending_orders || 0}</div>
                    </div>
                </div>
            </div>`;

            // Grid Levels
            if (state.grid_levels?.length) {
                html += `<div style="margin-bottom:24px">
                    <h4 style="margin:0 0 12px;color:#8b5cf6"><i class="ri-list-check"></i> Grid Levels</h4>
                    <table class="table"><thead><tr><th>Level</th><th>Price</th><th>Type</th><th>Filled</th><th>Time</th></tr></thead><tbody>`;
                state.grid_levels.forEach((level, idx) => {
                    const filled = level.filled ? 'Yes' : 'No';
                    const time = level.filled_at ? new Date(level.filled_at).toLocaleString() : '--';
                    html += `<tr>
                        <td>${idx + 1}</td>
                        <td>${level.price?.toFixed(6) || '--'}</td>
                        <td><span class="badge ${level.order_type === 'buy' ? 'badge-green' : 'badge-red'}">${escapeHtml(level.order_type || '--')}</span></td>
                        <td>${filled}</td>
                        <td style="font-size:11px">${escapeHtml(time)}</td>
                    </tr>`;
                });
                html += '</tbody></table></div>';
            }
        }

        // Config JSON (collapsible)
        html += `<div style="margin-top:24px">
            <details>
                <summary style="cursor:pointer;color:#3b82f6;font-size:14px;margin-bottom:8px"><i class="ri-code-line"></i> View Full Configuration (JSON)</summary>
                <pre style="background:rgba(0,0,0,0.3);padding:16px;border-radius:8px;font-size:11px;overflow-x:auto;margin-top:8px">${escapeHtml(JSON.stringify(detail.config, null, 2))}</pre>
            </details>
        </div>`;

        // State JSON (collapsible)
        html += `<div style="margin-top:16px">
            <details>
                <summary style="cursor:pointer;color:#3b82f6;font-size:14px;margin-bottom:8px"><i class="ri-database-2-line"></i> View Full Runtime State (JSON)</summary>
                <pre style="background:rgba(0,0,0,0.3);padding:16px;border-radius:8px;font-size:11px;overflow-x:auto;margin-top:8px">${escapeHtml(JSON.stringify(detail.state, null, 2))}</pre>
            </details>
        </div>`;

        body.innerHTML = html;
    } catch (err) {
        body.innerHTML = `<div style="text-align:center;padding:40px;color:#ef4444">Failed to load detail: ${escapeHtml(err.message)}</div>`;
    }
}

function closeStrategyDetail() {
    document.getElementById('strategy-detail-modal').style.display = 'none';
}

// ─── Market Charts ───
async function loadChartPage() {
    const ticker = (document.getElementById('chart-ticker')?.value || 'BTCUSDT').trim().toUpperCase();
    const timeframe = document.getElementById('chart-timeframe')?.value || '1h';
    const days = parseInt(document.getElementById('chart-days')?.value) || 30;
    try {
        const [ohlcv, realtime, indicators, positions, signals] = await Promise.all([
            fetchAPI(`/api/chart/ohlcv/${encodeURIComponent(ticker)}?timeframe=${encodeURIComponent(timeframe)}&days=${days}`),
            fetchAPI(`/api/chart/realtime/${encodeURIComponent(ticker)}`).catch(() => null),
            fetchAPI(`/api/chart/indicators/${encodeURIComponent(ticker)}?timeframe=${encodeURIComponent(timeframe)}`).catch(() => null),
            fetchAPI(`/api/chart/positions/${encodeURIComponent(ticker)}`).catch(() => ({ markers: [] })),
            fetchAPI(`/api/chart/signals/${encodeURIComponent(ticker)}?days=${days}`).catch(() => ({ markers: [] })),
        ]);
        renderMarketChart(ohlcv.data || []);
        const lastBar = ohlcv.data?.length ? ohlcv.data[ohlcv.data.length - 1] : null;
        const live = _chartRealtimeState && _chartRealtimeState.ticker === ticker ? _chartRealtimeState : realtime;
        const price = live?.price ?? lastBar?.close;
        setText('chart-price', price ? `$${formatNum(price)}` : '--');
        const change = Number(live?.change_24h_pct ?? live?.change_1h_pct ?? 0);
        const changeEl = document.getElementById('chart-change');
        if (changeEl) {
            changeEl.textContent = `${change >= 0 ? '+' : ''}${change.toFixed(2)}%`;
            changeEl.className = `kpi-value ${change >= 0 ? 'pnl-positive' : 'pnl-negative'}`;
        }
        setText('chart-rsi', formatValue(live?.rsi_1h ?? indicators?.indicators?.rsi_1h));
        setText('chart-volume', formatCompact(live?.volume_24h ?? indicators?.indicators?.volume_24h ?? realtime?.volume_24h));
        renderMarkerList('chart-positions', positions.markers || [], t('pages.charts.no_positions', 'No open position markers'));
        renderMarkerList('chart-signals', signals.markers || [], t('pages.charts.no_signals', 'No executed signal markers'));
        connectPriceSocket(ticker);
    } catch (err) {
        showToast(err.message, 'error', 'Chart Load Failed');
    }
}

function renderMarketChart(data) {
    const ctx = document.getElementById('market-chart')?.getContext('2d');
    if (!ctx) return;
    if (window.marketChart) window.marketChart.destroy();
    const labels = data.map(bar => new Date((bar.time || 0) * 1000).toLocaleString());
    const close = data.map(bar => Number(bar.close || 0));
    const high = data.map(bar => Number(bar.high || 0));
    const low = data.map(bar => Number(bar.low || 0));
    window.marketChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels,
            datasets: [
                { label: 'Close', data: close, borderColor: '#22c55e', backgroundColor: 'rgba(34,197,94,.12)', borderWidth: 2, pointRadius: 0, tension: .25, fill: true },
                { label: 'High', data: high, borderColor: 'rgba(59,130,246,.55)', borderWidth: 1, pointRadius: 0, tension: .2 },
                { label: 'Low', data: low, borderColor: 'rgba(239,68,68,.55)', borderWidth: 1, pointRadius: 0, tension: .2 },
            ],
        },
        options: chartOptions('Price'),
    });
}

function renderMarkerList(id, markers, emptyText) {
    const el = document.getElementById(id);
    if (!el) return;
    if (!markers.length) {
        el.innerHTML = `<div class="empty-state">${escapeHtml(emptyText)}</div>`;
        return;
    }
    el.innerHTML = `<div class="table-wrapper"><table class="data-table"><thead><tr><th>${escapeHtml(t('common.time', 'Time'))}</th><th>${escapeHtml(t('pages.charts.marker', 'Marker'))}</th></tr></thead><tbody>${markers.map(m => `<tr><td>${escapeHtml(formatDateTime(new Date((m.time || 0) * 1000).toISOString()))}</td><td>${escapeHtml(m.text || m.position || '--')}</td></tr>`).join('')}</tbody></table></div>`;
}

function formatCompact(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n) || n === 0) return '--';
    if (Math.abs(n) >= 1_000_000_000) return `${(n / 1_000_000_000).toFixed(2)}B`;
    if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(2)}M`;
    if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(2)}K`;
    return n.toFixed(2);
}

// ─── Social Signals ───
async function loadSocialPage() {
    try {
        const [stats, feed, subs, leaderboard] = await Promise.all([
            fetchAPI('/api/social/stats'),
            fetchAPI('/api/social/list?limit=50'),
            fetchAPI('/api/social/subscriptions'),
            fetchAPI('/api/social/leaderboard?limit=10'),
        ]);
        setText('social-total-signals', stats.total_signals || 0);
        setText('social-total-subs', stats.total_subscriptions || 0);
        setText('social-active-users', stats.active_users || 0);
        setText('social-top-ticker', stats.top_tickers?.[0]?.ticker || '--');
        renderSocialFeed(feed.signals || []);
        renderSocialSubscriptions(subs.subscriptions || []);
        renderSocialLeaderboard(leaderboard.leaderboard || []);
    } catch (err) {
        showToast(err.message, 'error', 'Signals Load Failed');
    }
}

async function shareSocialSignal() {
    const payload = {
        ticker: (document.getElementById('social-ticker')?.value || 'BTCUSDT').trim().toUpperCase(),
        direction: document.getElementById('social-direction')?.value || 'long',
        entry_price: Number(document.getElementById('social-entry')?.value || 0),
        stop_loss: optionalNumber('social-sl'),
        take_profit: optionalNumber('social-tp'),
        confidence: Number(document.getElementById('social-confidence')?.value || 0),
        reason: document.getElementById('social-reason')?.value || '',
        strategy_name: 'manual-share',
    };
    if (!payload.entry_price || payload.entry_price <= 0) {
        showToast('Entry price must be greater than zero.', 'warning', 'Invalid Signal');
        return;
    }
    try {
        await fetchAPI('/api/social/share', { method: 'POST', body: JSON.stringify(payload) });
        showToast('Signal shared.', 'success', 'Shared');
        await loadSocialPage();
    } catch (err) {
        showToast(err.message, 'error', 'Share Failed');
    }
}

function optionalNumber(id) {
    const value = Number(document.getElementById(id)?.value || 0);
    return value > 0 ? value : null;
}

function renderSocialFeed(signals) {
    const el = document.getElementById('social-feed');
    if (!el) return;
    if (!signals.length) {
        el.innerHTML = `<div class="empty-state">${escapeHtml(t('pages.social.no_signals', 'No shared signals yet'))}</div>`;
        return;
    }
    el.innerHTML = `<div class="table-wrapper"><table class="data-table"><thead><tr><th>${escapeHtml(t('common.ticker', 'Ticker'))}</th><th>${escapeHtml(t('common.direction', 'Direction'))}</th><th>${escapeHtml(t('trading.entry', 'Entry'))}</th><th>${escapeHtml(t('common.confidence', 'Confidence'))}</th><th>${escapeHtml(t('pages.social.provider', 'Provider'))}</th><th>${escapeHtml(t('common.actions', 'Actions'))}</th></tr></thead><tbody>${signals.map(s => `<tr><td><strong>${escapeHtml(s.ticker)}</strong></td><td><span class="badge badge-${safeClassToken(s.direction)}">${escapeHtml(t(`trading.${s.direction}`, s.direction))}</span></td><td>$${formatNum(s.entry_price)}</td><td>${Math.round(Number(s.confidence || 0) * 100)}%</td><td>${escapeHtml(s.username || '--')}</td><td><div class="admin-actions"><button class="btn btn-sm btn-primary" onclick="subscribeSocialSignal('${escapeJsSingle(s.signal_id)}')">${escapeHtml(t('actions.subscribe', 'Subscribe'))}</button><button class="btn btn-sm btn-secondary" onclick="followSignalUser('${escapeJsSingle(s.username || '')}')">${escapeHtml(t('actions.follow', 'Follow'))}</button></div></td></tr>`).join('')}</tbody></table></div>`;
}

function renderSocialSubscriptions(subs) {
    const el = document.getElementById('social-subscriptions');
    if (!el) return;
    if (!subs.length) {
        el.innerHTML = `<div class="empty-state">${escapeHtml(t('pages.social.no_subscriptions', 'No signal subscriptions'))}</div>`;
        return;
    }
    el.innerHTML = `<div class="table-wrapper"><table class="data-table"><thead><tr><th>${escapeHtml(t('pages.social.signal', 'Signal'))}</th><th>${escapeHtml(t('pages.social.auto_execute', 'Auto Execute'))}</th><th>${escapeHtml(t('pages.social.max_position', 'Max Position'))}</th><th>${escapeHtml(t('pages.social.subscribed', 'Subscribed'))}</th><th>${escapeHtml(t('common.actions', 'Actions'))}</th></tr></thead><tbody>${subs.map(s => `<tr><td><code>${escapeHtml(s.signal_id)}</code></td><td>${s.auto_execute ? t('common.yes', 'Yes') : t('common.no', 'No')}</td><td>${formatNum(s.max_position_pct)}%</td><td>${escapeHtml(formatDateTime(s.subscribed_at))}</td><td><button class="btn btn-sm btn-danger" onclick="unsubscribeSocialSignal('${escapeJsSingle(s.signal_id)}')">${escapeHtml(t('actions.unsubscribe', 'Unsubscribe'))}</button></td></tr>`).join('')}</tbody></table></div>`;
}

function renderSocialLeaderboard(rows) {
    const el = document.getElementById('social-leaderboard');
    if (!el) return;
    if (!rows.length) {
        el.innerHTML = `<div class="empty-state">${escapeHtml(t('pages.social.no_leaderboard', 'No leaderboard data'))}</div>`;
        return;
    }
    el.innerHTML = rows.map((r, idx) => `<div class="metric-item"><span class="metric-label">#${idx + 1} ${escapeHtml(r.username || '--')} · ${escapeHtml(r.ticker || '--')}</span><span class="metric-value">${formatNum(r.success_rate || 0)}%</span></div>`).join('');
}

async function subscribeSocialSignal(signalId) {
    try {
        await fetchAPI(`/api/social/subscribe/${encodeURIComponent(signalId)}`, {
            method: 'POST',
            body: JSON.stringify({ signal_id: signalId, auto_execute: false, max_position_pct: 10 }),
        });
        showToast('Signal subscribed.', 'success', 'Subscribed');
        await loadSocialPage();
    } catch (err) {
        showToast(err.message, 'error', 'Subscribe Failed');
    }
}

async function unsubscribeSocialSignal(signalId) {
    try {
        await fetchAPI(`/api/social/unsubscribe/${encodeURIComponent(signalId)}`, { method: 'DELETE' });
        showToast('Signal subscription removed.', 'warning', 'Unsubscribed');
        await loadSocialPage();
    } catch (err) {
        showToast(err.message, 'error', 'Unsubscribe Failed');
    }
}

async function followSignalUser(username) {
    if (!username) return;
    try {
        await fetchAPI(`/api/social/follow/${encodeURIComponent(username)}`, { method: 'POST' });
        showToast(`Following ${username}.`, 'success', 'Following');
    } catch (err) {
        showToast(err.message, 'error', 'Follow Failed');
    }
}

// ─── Strategy Editor ───
async function loadStrategyEditorPage() {
    try {
        const [templates, strategies] = await Promise.all([
            fetchAPI('/api/strategy-editor/templates'),
            fetchAPI('/api/strategy-editor/list'),
        ]);
        renderStrategyTemplates(templates.templates || []);
        renderEditorStrategies(strategies.strategies || []);
    } catch (err) {
        showToast(err.message, 'error', 'Editor Load Failed');
    }
}

function renderStrategyTemplates(templates) {
    const el = document.getElementById('strategy-template-list');
    if (!el) return;
    _strategyTemplates = templates;
    if (!templates.length) {
        el.innerHTML = `<div class="empty-state">${escapeHtml(t('pages.editor.no_templates', 'No templates available'))}</div>`;
        return;
    }
    el.innerHTML = templates.map(item => `<div class="template-card"><div><strong>${escapeHtml(item.name)}</strong><span class="hint">${escapeHtml(item.category)} · ${escapeHtml(item.description)}</span></div><button class="btn btn-sm btn-secondary" onclick="useStrategyTemplateById('${escapeJsSingle(item.id || item.name)}')">${escapeHtml(t('actions.use', 'Use'))}</button></div>`).join('');
}

function useStrategyTemplateById(templateId) {
    const template = _strategyTemplates.find(t => (t.id || t.name) === templateId);
    if (template) useStrategyTemplate(template);
}

function useStrategyTemplate(template) {
    const config = template.config || {};
    setFieldValue('editor-strategy-id', '');
    setFieldValue('editor-name', template.name || 'Custom Strategy');
    setFieldValue('editor-entry-json', JSON.stringify(config.entry_conditions || [], null, 2));
    setFieldValue('editor-exit-json', JSON.stringify(config.exit_conditions || [], null, 2));
    setFieldValue('editor-risk-json', JSON.stringify(config.risk_management || {}, null, 2));
    setFieldValue('editor-tp-json', JSON.stringify(config.tp_levels || [], null, 2));
    setFieldValue('editor-trailing-json', JSON.stringify(config.trailing_stop || {}, null, 2));
}

function resetStrategyEditorForm() {
    setFieldValue('editor-strategy-id', '');
    setFieldValue('editor-name', 'My Strategy');
    setFieldValue('editor-ticker', 'BTCUSDT');
    setFieldValue('editor-direction', 'long');
    setFieldValue('editor-entry-json', '[]');
    setFieldValue('editor-exit-json', '[]');
    setFieldValue('editor-risk-json', '{}');
    setFieldValue('editor-tp-json', '[]');
    setFieldValue('editor-trailing-json', '{}');
}

async function saveEditorStrategy() {
    let payload;
    try {
        payload = {
            strategy_id: document.getElementById('editor-strategy-id')?.value || '',
            name: document.getElementById('editor-name')?.value || 'My Strategy',
            ticker: (document.getElementById('editor-ticker')?.value || 'BTCUSDT').trim().toUpperCase(),
            direction: document.getElementById('editor-direction')?.value || 'long',
            entry_conditions: JSON.parse(document.getElementById('editor-entry-json')?.value || '[]'),
            exit_conditions: JSON.parse(document.getElementById('editor-exit-json')?.value || '[]'),
            risk_management: JSON.parse(document.getElementById('editor-risk-json')?.value || '{}'),
            tp_levels: JSON.parse(document.getElementById('editor-tp-json')?.value || '[]'),
            trailing_stop: JSON.parse(document.getElementById('editor-trailing-json')?.value || '{}'),
        };
    } catch (err) {
        showToast('One of the JSON fields is invalid.', 'warning', 'Invalid JSON');
        return;
    }
    try {
        const existingId = payload.strategy_id;
        const endpoint = existingId ? `/api/strategy-editor/${encodeURIComponent(existingId)}` : '/api/strategy-editor/create';
        const method = existingId ? 'PUT' : 'POST';
        const result = await fetchAPI(endpoint, { method, body: JSON.stringify(payload) });
        setFieldValue('editor-strategy-id', result.strategy_id || existingId);
        showToast('Strategy saved.', 'success', 'Saved');
        await loadStrategyEditorPage();
    } catch (err) {
        showToast(err.message, 'error', 'Save Failed');
    }
}

function renderEditorStrategies(strategies) {
    const el = document.getElementById('editor-strategy-list');
    if (!el) return;
    if (!strategies.length) {
        el.innerHTML = `<div class="empty-state">${escapeHtml(t('pages.editor.no_saved_strategies', 'No saved custom strategies'))}</div>`;
        return;
    }
    el.innerHTML = `<div class="table-wrapper"><table class="data-table"><thead><tr><th>${escapeHtml(t('common.name', 'Name'))}</th><th>${escapeHtml(t('common.ticker', 'Ticker'))}</th><th>${escapeHtml(t('common.direction', 'Direction'))}</th><th>${escapeHtml(t('common.status', 'Status'))}</th><th>${escapeHtml(t('messages.updated', 'Updated'))}</th><th>${escapeHtml(t('common.actions', 'Actions'))}</th></tr></thead><tbody>${strategies.map(s => `<tr><td><strong>${escapeHtml(s.name || '--')}</strong></td><td>${escapeHtml(s.ticker || '--')}</td><td><span class="badge badge-${safeClassToken(s.direction || 'long')}">${escapeHtml(t(`trading.${s.direction || 'long'}`, s.direction || '--'))}</span></td><td><span class="badge badge-${s.is_active ? 'active' : 'pending'}">${s.is_active ? t('common.active', 'active') : t('common.draft', 'draft')}</span></td><td>${escapeHtml(formatDateTime(s.updated_at))}</td><td><div class="admin-actions"><button class="btn btn-sm btn-secondary" onclick="editStrategyDraft('${escapeJsSingle(s.strategy_id)}')">${escapeHtml(t('actions.edit', 'Edit'))}</button><button class="btn btn-sm btn-warning" onclick="exportStrategy('${escapeJsSingle(s.strategy_id)}')"><i class="ri-download-line"></i> Export</button><button class="btn btn-sm btn-primary" onclick="toggleEditorStrategy('${escapeJsSingle(s.strategy_id)}', ${s.is_active ? 'false' : 'true'})">${escapeHtml(s.is_active ? t('actions.deactivate', 'Deactivate') : t('actions.activate', 'Activate'))}</button><button class="btn btn-sm btn-danger" onclick="deleteEditorStrategy('${escapeJsSingle(s.strategy_id)}')">${escapeHtml(t('actions.delete', 'Delete'))}</button></div></td></tr>`).join('')}</tbody></table></div><div style="padding:12px;text-align:right"><button class="btn btn-secondary" onclick="exportAllStrategies()"><i class="ri-download-line"></i> Export All</button></div>`;
}

async function editStrategyDraft(strategyId) {
    try {
        const s = await fetchAPI(`/api/strategy-editor/${encodeURIComponent(strategyId)}`);
        setFieldValue('editor-strategy-id', s.strategy_id || strategyId);
        setFieldValue('editor-name', s.name || '');
        setFieldValue('editor-ticker', s.ticker || 'BTCUSDT');
        setFieldValue('editor-direction', s.direction || 'long');
        setFieldValue('editor-entry-json', JSON.stringify(s.entry_conditions || [], null, 2));
        setFieldValue('editor-exit-json', JSON.stringify(s.exit_conditions || [], null, 2));
        setFieldValue('editor-risk-json', JSON.stringify(s.risk_management || {}, null, 2));
        setFieldValue('editor-tp-json', JSON.stringify(s.tp_levels || [], null, 2));
        setFieldValue('editor-trailing-json', JSON.stringify(s.trailing_stop || {}, null, 2));
    } catch (err) {
        showToast(err.message, 'error', 'Load Failed');
    }
}

async function toggleEditorStrategy(strategyId, activate) {
    try {
        await fetchAPI(`/api/strategy-editor/${encodeURIComponent(strategyId)}/${activate ? 'activate' : 'deactivate'}`, { method: 'POST' });
        showToast(activate ? 'Strategy activated.' : 'Strategy deactivated.', 'success', 'Updated');
        await loadStrategyEditorPage();
    } catch (err) {
        showToast(err.message, 'error', 'Update Failed');
    }
}

async function deleteEditorStrategy(strategyId) {
    if (!confirm('Delete this strategy?')) return;
    try {
        await fetchAPI(`/api/strategy-editor/${encodeURIComponent(strategyId)}`, { method: 'DELETE' });
        showToast('Strategy deleted.', 'success', 'Deleted');
        await loadStrategyEditorPage();
    } catch (err) {
        showToast(err.message, 'error', 'Delete Failed');
    }
}

async function exportStrategy(strategyId) {
    try {
        const s = await fetchAPI(`/api/strategy-editor/export/${encodeURIComponent(strategyId)}`);
        const dataStr = JSON.stringify(s, null, 2);
        const blob = new Blob([dataStr], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `strategy_${s.name || strategyId}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        showToast('Strategy exported.', 'success', 'Export Complete');
    } catch (err) {
        showToast(err.message, 'error', 'Export Failed');
    }
}

async function exportAllStrategies() {
    try {
        const result = await fetchAPI('/api/strategy-editor/list');
        const strategies = result.strategies || [];
        if (!strategies.length) {
            showToast('No strategies to export.', 'warning', 'Empty');
            return;
        }
        const exportData = {
            exported_at: new Date().toISOString(),
            strategies: strategies,
        };
        const dataStr = JSON.stringify(exportData, null, 2);
        const blob = new Blob([dataStr], { type: 'application/json' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `all_strategies_${new Date().toISOString().slice(0, 10)}.json`;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
        showToast(`${strategies.length} strategies exported.`, 'success', 'Export Complete');
    } catch (err) {
        showToast(err.message, 'error', 'Export Failed');
    }
}

function importStrategyFromJSON() {
    const input = document.getElementById('strategy-import-file');
    if (input) input.click();
}

function triggerStrategyImport() {
    const input = document.getElementById('saved-strategy-import-file');
    if (input) input.click();
}

async function handleStrategyImportFile(event) {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
        const text = await file.text();
        const data = JSON.parse(text);
        setFieldValue('editor-strategy-id', data.strategy_id || '');
        setFieldValue('editor-name', data.name || file.name.replace('.json', ''));
        setFieldValue('editor-ticker', data.ticker || 'BTCUSDT');
        setFieldValue('editor-direction', data.direction || 'long');
        setFieldValue('editor-entry-json', JSON.stringify(data.entry_conditions || [], null, 2));
        setFieldValue('editor-exit-json', JSON.stringify(data.exit_conditions || [], null, 2));
        setFieldValue('editor-risk-json', JSON.stringify(data.risk_management || {}, null, 2));
        setFieldValue('editor-tp-json', JSON.stringify(data.tp_levels || [], null, 2));
        setFieldValue('editor-trailing-json', JSON.stringify(data.trailing_stop || {}, null, 2));
        showToast('Strategy imported to form.', 'success', 'Import Complete');
    } catch (err) {
        showToast('Invalid JSON file.', 'error', 'Import Failed');
    }
    event.target.value = '';
}

async function handleSavedStrategyImportFile(event) {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
        const text = await file.text();
        const data = JSON.parse(text);
        const strategies = data.strategies || [data];
        let imported = 0;
        for (const s of strategies) {
            try {
                await fetchAPI('/api/strategy-editor/import', {
                    method: 'POST',
                    body: JSON.stringify(s),
                });
                imported++;
            } catch (err) {
                console.warn('Failed to import strategy:', s.name, err);
            }
        }
        showToast(`${imported} strategies imported.`, 'success', 'Import Complete');
        await loadStrategyEditorPage();
    } catch (err) {
        showToast('Invalid JSON file.', 'error', 'Import Failed');
    }
    event.target.value = '';
}
