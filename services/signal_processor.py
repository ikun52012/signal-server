"""
Signal Server - Signal Processing Service
Handles the complete signal processing pipeline.
"""
import asyncio
import hashlib
import json
import math
import os
import time as _time
from collections.abc import Sequence
from datetime import datetime, timezone

import httpx
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ai_analyzer import analyze_signal
from core.config import settings
from core.database import (
    PositionModel,
    close_position_async,
    get_user_active_subscription,
    get_user_by_id,
    has_recent_webhook_event,
    log_trade_db,
    record_signal_decision_audit,
    record_webhook_event,
)
from core.metrics import (
    record_ai_analysis,
    record_prefilter_result,
    record_signal_received,
    record_trade,
)
from core.security import decrypt_settings_payload
from core.trading_control import trading_allowed
from core.utils.common import (
    first_valid,
    loads_list,
    position_symbol_key,
    resolve_limit_timeout_secs,
    safe_float,
    safe_int,
)
from exchange import cancel_order, execute_trade
from market_data import fetch_enhanced_market_context, fetch_market_context
from models import (
    AIAnalysis,
    MarketContext,
    PreFilterResult,
    SignalDirection,
    TradeDecision,
    TradingViewSignal,
)
from notifier import (
    notify_ai_analysis,
    notify_error,
    notify_pre_filter_blocked,
    notify_signal_batched,
    notify_signal_queued,
    notify_signal_received,
    notify_trade_executed,
)
from pre_filter import run_pre_filter_async
from services.order_reconciler import record_order_event

_WEBHOOK_LOCKS: dict[str, asyncio.Lock] = {}
_WEBHOOK_LOCKS_GUARD = asyncio.Lock()
_WEBHOOK_LOCK_MAX_SIZE = 5000
_WEBHOOK_LOCK_TTL = 600
_WEBHOOK_LOCK_CREATED: dict[str, float] = {}
_SENSITIVE_EVENT_KEY_PARTS = ("secret", "token", "password", "api_key", "api_secret")

# Per-ticker locks for concurrent signal handling
_TICKER_LOCKS: dict[str, asyncio.Lock] = {}
_TICKER_LOCKS_GUARD = asyncio.Lock()
_TICKER_LOCK_MAX_SIZE = 1000

# Per-ticker pending signal count for queue backpressure
_TICKER_PENDING: dict[str, int] = {}
_TICKER_PENDING_GUARD = asyncio.Lock()

# Global processing semaphore and interval control
_GLOBAL_PROCESSING_SEMAPHORE: asyncio.Semaphore | None = None
_GLOBAL_PROCESSING_GUARD = asyncio.Lock()
# Per-user processing interval tracking (was global, now user-isolated)
# Uses "_admin_" sentinel for admin signals to avoid collision with user_id=None
_ADMIN_RATE_LIMIT_KEY = "_admin_"
_LAST_SIGNAL_PROCESS_TIME: dict[str, float] = {}
_PROCESSING_INTERVAL_SEMAPHORE = asyncio.Lock()

# Dynamic interval tracking (Optimization 1)
_AI_RESPONSE_TIMES: list[float] = []
_AI_RESPONSE_TIMES_GUARD = asyncio.Lock()
_AI_RESPONSE_TIMES_MAX_SAMPLES = 20

# Batch processing state (Optimization 4) - keyed by (ticker, user_id) for user isolation
_PENDING_BATCH_SIGNALS: dict[tuple[str, str | None], list[tuple[TradingViewSignal, float, dict | None]]] = {}
_BATCH_SIGNALS_GUARD = asyncio.Lock()

# Prefetch market data cache (Optimization 5) with TTL
_PREFETCHED_MARKET_DATA: dict[str, tuple[float, MarketContext]] = {}
_PREFETCH_GUARD = asyncio.Lock()
_PREFETCH_TTL_SECONDS = 30.0
_PREFETCH_MAX_SIZE = 500


async def _track_ai_response_time(response_time: float) -> None:
    """Track AI response time for dynamic interval adjustment."""
    async with _AI_RESPONSE_TIMES_GUARD:
        _AI_RESPONSE_TIMES.append(response_time)
        if len(_AI_RESPONSE_TIMES) > _AI_RESPONSE_TIMES_MAX_SAMPLES:
            _AI_RESPONSE_TIMES.pop(0)


async def _get_avg_ai_response_time() -> float:
    """Get average AI response time from recent samples."""
    async with _AI_RESPONSE_TIMES_GUARD:
        if not _AI_RESPONSE_TIMES:
            return 0.0
        return sum(_AI_RESPONSE_TIMES) / len(_AI_RESPONSE_TIMES)


async def _get_dynamic_interval() -> float:
    """Calculate dynamic interval based on AI load (Optimization 1).

    High load (>30s avg response) -> double interval
    Normal load -> use base interval
    """
    if not settings.ai.dynamic_interval_enabled:
        return settings.ai.signal_processing_interval_secs

    avg_time = await _get_avg_ai_response_time()
    base_interval = settings.ai.signal_processing_interval_secs

    if avg_time > settings.ai.dynamic_interval_high_load_threshold:
        dynamic_interval = base_interval * settings.ai.dynamic_interval_high_load_multiplier
        logger.info(
            f"[SignalProcessor] High AI load detected (avg={avg_time:.1f}s), "
            f"increasing interval: {base_interval:.1f}s -> {dynamic_interval:.1f}s"
        )
        return dynamic_interval
    return base_interval


async def _get_global_semaphore() -> asyncio.Semaphore:
    """Get or create global processing semaphore (lazy init)."""
    global _GLOBAL_PROCESSING_SEMAPHORE
    async with _GLOBAL_PROCESSING_GUARD:
        if _GLOBAL_PROCESSING_SEMAPHORE is None:
            _GLOBAL_PROCESSING_SEMAPHORE = asyncio.Semaphore(settings.ai.global_processing_semaphore)
        return _GLOBAL_PROCESSING_SEMAPHORE


async def _wait_processing_interval(skip_interval: bool = False, user_id: str | None = None) -> None:
    """Wait for processing interval after completing a signal (Optimization 1 & 2).

    Args:
        skip_interval: If True, skip waiting (for high-confidence signals)
        user_id: User ID for per-user rate limiting (prevents cross-user throttling)
    """
    global _LAST_SIGNAL_PROCESS_TIME
    if skip_interval:
        logger.debug("[SignalProcessor] Skipping interval (high confidence signal)")
        return

    interval = await _get_dynamic_interval()
    if interval <= 0:
        return

    # Use sentinel key for admin signals to avoid collision with user_id=None
    rate_key = user_id if user_id is not None else _ADMIN_RATE_LIMIT_KEY

    async with _PROCESSING_INTERVAL_SEMAPHORE:
        now = _time.time()
        last_time = _LAST_SIGNAL_PROCESS_TIME.get(rate_key, 0.0)
        elapsed = now - last_time
        if elapsed < interval:
            wait_time = interval - elapsed
            logger.debug(f"[SignalProcessor] Waiting {wait_time:.1f}s before next signal (user={user_id})")
            await asyncio.sleep(wait_time)
        _LAST_SIGNAL_PROCESS_TIME[rate_key] = _time.time()


async def _prefetch_market_data_async(ticker: str, user_id: str | None = None) -> MarketContext | None:
    """Prefetch market data before acquiring semaphore (Optimization 5).

    This allows market data fetch to happen in parallel with other signals,
    reducing overall latency when semaphore is acquired.

    P1-8: Cache key includes user_id to isolate per-user exchange configurations.
    """
    if not settings.ai.prefetch_market_data:
        return None

    # P1-8: Include user_id in cache key for per-user isolation
    cache_scope = user_id or "admin"
    cache_key = f"{cache_scope}:{ticker.upper().strip()}"
    now = _time.time()

    async with _PREFETCH_GUARD:
        cached = _PREFETCHED_MARKET_DATA.get(cache_key)
        if cached:
            timestamp, context = cached
            if now - timestamp < _PREFETCH_TTL_SECONDS:
                return context
            # Expired - clean up
            _PREFETCHED_MARKET_DATA.pop(cache_key, None)

    # Cache miss or expired - fetch fresh data (outside lock to reduce contention)
    try:
        context = await fetch_market_context(ticker)
        async with _PREFETCH_GUARD:
            # Evict oldest entries if cache is too large
            if len(_PREFETCHED_MARKET_DATA) >= _PREFETCH_MAX_SIZE:
                oldest_key = next(iter(_PREFETCHED_MARKET_DATA))
                _PREFETCHED_MARKET_DATA.pop(oldest_key, None)
            _PREFETCHED_MARKET_DATA[cache_key] = (now, context)
        return context
    except (httpx.HTTPError, ValueError) as e:
        logger.warning(f"[SignalProcessor] Prefetch market data failed for {ticker}: {e}")
        return None
    except Exception as e:
        logger.warning(f"[SignalProcessor] Prefetch market data failed for {ticker}: {e}")
        return None


async def _check_batch_signals(ticker: str, signal: TradingViewSignal, raw_body: dict | None, user_id: str | None = None) -> bool:
    """Check if signal should be batched with similar pending signals (Optimization 4).

    Returns True if signal was batched (should not process individually).
    Uses (ticker, user_id) key for user isolation.
    """
    if not settings.ai.batch_signals_enabled:
        return False

    key = (ticker.upper().strip(), user_id)
    now = _time.time()

    async with _BATCH_SIGNALS_GUARD:
        pending = _PENDING_BATCH_SIGNALS.get(key, [])

        expired = [(s, t, b) for s, t, b in pending if now - t > settings.ai.batch_signals_window_secs]
        for item in expired:
            pending.remove(item)

        same_direction = [
            (s, t, b) for s, t, b in pending
            if s.direction == signal.direction
        ]

        if len(same_direction) >= settings.ai.batch_signals_max_count:
            logger.info(
                f"[SignalProcessor] Batch triggered for {ticker} {signal.direction.value}: "
                f"{len(same_direction) + 1} signals within {settings.ai.batch_signals_window_secs}s window. "
                f"Skipping individual processing (user={user_id})."
            )
            for item in same_direction:
                pending.remove(item)
            _PENDING_BATCH_SIGNALS[key] = pending
            return True

        pending.append((signal, now, raw_body))
        _PENDING_BATCH_SIGNALS[key] = pending

        if len(same_direction) >= 1:
            logger.debug(
                f"[SignalProcessor] Signal batching pending for {ticker}: "
                f"{len(same_direction) + 1}/{settings.ai.batch_signals_max_count} same-direction signals (user={user_id})"
            )

        return False


async def _ticker_lock(ticker: str, user_id: str | None = None) -> asyncio.Lock:
    """Get or create a lock for a specific ticker to prevent concurrent conflicts.

    This ensures that signals for the same ticker are processed sequentially,
    preventing race conditions when two opposite signals arrive simultaneously.

    Args:
        ticker: The ticker symbol (e.g., "BTCUSDT")
        user_id: Optional user ID for multi-user isolation

    Returns:
        asyncio.Lock for this ticker+user combination
    """
    scope = user_id or "admin"
    key = f"{scope}:{ticker.upper().strip()}"

    async with _TICKER_LOCKS_GUARD:
        lock = _TICKER_LOCKS.get(key)
        if lock is None:
            lock = asyncio.Lock()
            _TICKER_LOCKS[key] = lock

            if len(_TICKER_LOCKS) > _TICKER_LOCK_MAX_SIZE:
                keys_to_remove = list(_TICKER_LOCKS.keys())[:_TICKER_LOCK_MAX_SIZE // 2]
                for k in keys_to_remove:
                    if k == key:
                        continue
                    old_lock = _TICKER_LOCKS.get(k)
                    if old_lock and not old_lock.locked():
                        _TICKER_LOCKS.pop(k, None)

        return lock


async def _check_ticker_queue_limit(ticker: str, user_id: str | None = None) -> bool:
    """Check if ticker queue has room for another signal.

    Returns:
        True if queue has room, False if queue is full
    """
    scope = user_id or "admin"
    key = f"{scope}:{ticker.upper().strip()}"
    limit = settings.ai.signal_queue_limit

    async with _TICKER_PENDING_GUARD:
        current = _TICKER_PENDING.get(key, 0)
        if current >= limit:
            logger.warning(
                f"[SignalProcessor] Ticker {ticker} queue full: "
                f"{current}/{limit} pending signals, rejecting new signal"
            )
            return False
        _TICKER_PENDING[key] = current + 1
        return True


async def _release_ticker_queue_slot(ticker: str, user_id: str | None = None) -> None:
    """Release a ticker queue slot after signal processing completes."""
    scope = user_id or "admin"
    key = f"{scope}:{ticker.upper().strip()}"

    async with _TICKER_PENDING_GUARD:
        current = _TICKER_PENDING.get(key, 0)
        _TICKER_PENDING[key] = max(0, current - 1)


async def _release_ticker_lock(ticker: str, user_id: str | None = None) -> None:
    """Release a ticker lock after processing.

    Note: Locks are automatically released when the async context exits,
    but this function can be called to explicitly cleanup unused locks.
    """
    scope = user_id or "admin"
    key = f"{scope}:{ticker.upper().strip()}"

    async with _TICKER_LOCKS_GUARD:
        lock = _TICKER_LOCKS.get(key)
        if lock and not lock.locked():
            _TICKER_LOCKS.pop(key, None)


async def _fingerprint_lock(fingerprint: str) -> asyncio.Lock:
    async with _WEBHOOK_LOCKS_GUARD:
        now = _time.time()
        expired = [
            k for k, t in _WEBHOOK_LOCK_CREATED.items()
            if now - t > _WEBHOOK_LOCK_TTL and k != fingerprint
        ]
        for k in expired:
            lock = _WEBHOOK_LOCKS.get(k)
            if lock and not lock.locked():
                _WEBHOOK_LOCKS.pop(k, None)
                _WEBHOOK_LOCK_CREATED.pop(k, None)

        lock = _WEBHOOK_LOCKS.get(fingerprint)
        if lock is None:
            lock = asyncio.Lock()
            _WEBHOOK_LOCKS[fingerprint] = lock
            _WEBHOOK_LOCK_CREATED[fingerprint] = now

            if len(_WEBHOOK_LOCKS) > _WEBHOOK_LOCK_MAX_SIZE:
                sorted_keys = sorted(_WEBHOOK_LOCK_CREATED, key=_WEBHOOK_LOCK_CREATED.get)
                keys_to_remove = sorted_keys[:_WEBHOOK_LOCK_MAX_SIZE // 2]
                for k in keys_to_remove:
                    if k == fingerprint:
                        continue
                    old_lock = _WEBHOOK_LOCKS.get(k)
                    if old_lock and not old_lock.locked():
                        _WEBHOOK_LOCKS.pop(k, None)
                        _WEBHOOK_LOCK_CREATED.pop(k, None)
        else:
            _WEBHOOK_LOCK_CREATED[fingerprint] = now
        return lock


async def _release_fingerprint_lock(fingerprint: str, lock: asyncio.Lock) -> None:
    async with _WEBHOOK_LOCKS_GUARD:
        if not lock.locked() and _WEBHOOK_LOCKS.get(fingerprint) is lock:
            _WEBHOOK_LOCKS.pop(fingerprint, None)
            _WEBHOOK_LOCK_CREATED.pop(fingerprint, None)


def _safe_event_payload(value):
    """Redact secrets before webhook payloads are stored in event logs."""
    if isinstance(value, dict):
        safe = {}
        for key, item in value.items():
            key_text = str(key).lower()
            if any(part in key_text for part in _SENSITIVE_EVENT_KEY_PARTS):
                safe[key] = "***"
            else:
                safe[key] = _safe_event_payload(item)
        return safe
    if isinstance(value, list):
        return [_safe_event_payload(item) for item in value]
    return value


# ─────────────────────────────────────────────
# Webhook Fingerprint
# ─────────────────────────────────────────────

def compute_webhook_fingerprint(body: dict, user_id: str | None = None) -> str:
    """Compute a unique fingerprint for webhook deduplication."""
    scope = user_id or "admin"
    alert_id = str(body.get("alert_id") or body.get("order_id") or body.get("id") or "").strip()

    fields = {
        "scope": scope,
        "secret_hash": hashlib.sha256(str(body.get("secret", "")).strip().encode()).hexdigest()[:16],
        "ticker": str(body.get("ticker", "")).upper().strip(),
        "direction": str(body.get("direction", "")).lower().strip(),
        "timeframe": str(body.get("timeframe", "")).strip(),
        "price": round(float(body.get("price") or 0), 8),
        "strategy": str(body.get("strategy", "")).strip(),
        "message": str(body.get("message", "")).strip(),
    }

    if alert_id:
        fields = {"scope": scope, "secret_hash": fields["secret_hash"], "alert_id": alert_id}

    raw = json.dumps(fields, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


# ─────────────────────────────────────────────
# Signal Processing Pipeline
# ─────────────────────────────────────────────

class SignalProcessor:
    """Main signal processing service."""

    def __init__(self, session: AsyncSession):
        self.session = session

    @staticmethod
    def _live_trading_requested(user_settings: dict | None = None) -> bool:
        exchange_cfg = (user_settings or {}).get("exchange") or {}
        if "live_trading" in exchange_cfg:
            return bool(exchange_cfg.get("live_trading"))
        return bool(settings.exchange.live_trading)

    @staticmethod
    def _block_live_risk_check_errors(user_settings: dict | None = None) -> bool:
        risk_cfg = (user_settings or {}).get("risk") or {}
        if "block_live_on_risk_check_error" in risk_cfg:
            return bool(risk_cfg.get("block_live_on_risk_check_error"))
        return bool(settings.risk.block_live_on_risk_check_error)

    async def _record_signal_audit(
        self,
        *,
        fingerprint: str,
        signal: TradingViewSignal,
        user_id: str | None,
        stage: str,
        outcome: str,
        reason: str = "",
        payload: dict | None = None,
    ) -> None:
        await record_signal_decision_audit(
            session=self.session,
            user_id=user_id,
            fingerprint=fingerprint,
            ticker=signal.ticker,
            direction=signal.direction.value,
            stage=stage,
            outcome=outcome,
            reason=reason,
            payload=_safe_event_payload(payload or {}),
        )

    async def process_webhook(
        self,
        signal: TradingViewSignal,
        user_id: str | None = None,
        client_ip: str = "",
        raw_body: dict | None = None,
    ) -> dict:
        """
        Process a webhook signal through the complete pipeline.
        Returns the result of the processing.

        Architecture (6 Optimizations):
        1. Dynamic interval based on AI load
        2. Priority skip interval for high-confidence signals
        3. Prefetch market data before semaphore
        4. Batch similar signals (same ticker+direction)
        5. Global semaphore (5 concurrent max)
        6. Per-ticker lock prevents conflicts

        Flow:
        1. Prefetch market data (parallel optimization)
        2. Check batch signals
        3. Check queue limit
        4. Acquire global semaphore (waits if 5 processing)
        5. Acquire ticker lock (waits if same ticker)
        6. Process signal with prefetched market data
        7. Check confidence for interval skip
        8. Wait dynamic interval
        9. Release semaphore and lock
        """
        # Optimization 4: Check batch signals (with user isolation)
        batched = await _check_batch_signals(signal.ticker, signal, raw_body, user_id)
        if batched:
            await notify_signal_batched(
                signal.ticker,
                signal.direction.value,
                settings.ai.batch_signals_max_count,
                settings.ai.batch_signals_window_secs,
            )
            return {"status": "batched", "reason": "Signal added to batch queue"}

        # Check queue limit first (fast rejection for extreme load)
        if not await _check_ticker_queue_limit(signal.ticker, user_id):
            await notify_signal_queued(
                signal.ticker,
                signal.direction.value,
                f"Queue full for {signal.ticker} - too many pending signals",
            )
            return {
                "status": "rejected",
                "reason": f"Queue full for {signal.ticker} - too many pending signals",
                "queue_limit": settings.ai.signal_queue_limit,
            }

        # Optimization 5: Prefetch market data AFTER queue limit check
        prefetched_market = await _prefetch_market_data_async(signal.ticker, user_id)

        # Get global processing semaphore
        global_sem = await _get_global_semaphore()

        # Wait for global semaphore (max 5 concurrent signals)
        async with global_sem:
            # Acquire ticker-specific lock to prevent same-ticker conflicts
            ticker_lock = await _ticker_lock(signal.ticker, user_id)

            try:
                async with ticker_lock:
                    # Process signal with prefetched market data
                    result = await self._process_signal_locked(
                        signal, user_id, client_ip, raw_body, prefetched_market=prefetched_market
                    )

                # Optimization 2: Check confidence for interval skip
                skip_interval = False
                if result.get("status") in ("filled", "simulated"):
                    analysis_data = result.get("analysis", {})
                    analysis_confidence = analysis_data.get("confidence", 0.0) if isinstance(analysis_data, dict) else 0.0
                    if analysis_confidence >= settings.ai.priority_skip_interval_confidence_threshold:
                        skip_interval = True
                        logger.info(
                            f"[SignalProcessor] High confidence signal ({analysis_confidence:.2f}) "
                            f"skips interval for faster next signal"
                        )

                # Optimization 1: Wait dynamic interval (per-user)
                await _wait_processing_interval(skip_interval=skip_interval, user_id=user_id)

                return result
            finally:
                # Always release queue slot when done
                await _release_ticker_queue_slot(signal.ticker, user_id)

    async def _process_signal_locked(
        self,
        signal: TradingViewSignal,
        user_id: str | None = None,
        client_ip: str = "",
        raw_body: dict | None = None,
        prefetched_market: MarketContext | None = None,
    ) -> dict:
        """Internal signal processing with ticker lock already acquired.

        Args:
            prefetched_market: Pre-fetched market data (Optimization 5)
        """
        # Compute fingerprint for deduplication
        fingerprint = compute_webhook_fingerprint(raw_body or signal.model_dump(), user_id)
        user_settings = await self._load_user_settings(user_id)

        # Reserve the webhook before slow AI/exchange calls so concurrent or
        # retried TradingView deliveries cannot pass the dedupe check together.
        reservation = await self._reserve_webhook_event(
            fingerprint=fingerprint,
            signal=signal,
            user_id=user_id,
            client_ip=client_ip,
            payload=raw_body or signal.model_dump(),
        )
        if reservation is None:
            logger.warning(f"[Signal] Duplicate webhook: {fingerprint[:16]}")
            return {"status": "duplicate", "reason": "Duplicate signal within 5 minutes"}

        # Step 0: Check global trading control (emergency stop, paused, read_only)
        live_trading = self._live_trading_requested(user_settings)
        trading_state = await trading_allowed(self.session, user_id=user_id, live_trading=live_trading)
        if not trading_state.get("allowed"):
            logger.warning(
                f"[Signal] Trading blocked by global control for {signal.ticker}: "
                f"mode={trading_state.get('mode')}, reason={trading_state.get('block_reason')}"
            )
            self._update_reserved_event(
                reservation,
                status="blocked",
                status_code=423,
                reason=trading_state.get("block_reason", "Trading is currently disabled"),
                payload=raw_body or signal.model_dump(),
            )
            return {
                "status": "blocked",
                "reason": trading_state.get("block_reason", "Trading is currently disabled"),
                "trading_mode": trading_state.get("mode"),
            }

        # Record signal received
        record_signal_received(signal.ticker, signal.direction.value, user_id)

        # Notify signal received
        await notify_signal_received(signal.ticker, signal.direction.value, signal.price)

        try:
            # Step 1: Fetch market context (use prefetch
            enhanced_filters = settings.ai.voting_enabled or os.getenv("ENHANCED_FILTERS_ENABLED", "true").lower() == "true"
            if prefetched_market:
                market = prefetched_market
                logger.debug(f"[SignalProcessor] Using prefetched market data for {signal.ticker}")
            elif enhanced_filters:
                market = await fetch_enhanced_market_context(signal.ticker)
            else:
                market = await fetch_market_context(signal.ticker)

            # Step 2: Run pre-filter
            prefilter_result = await self._run_prefilter(signal, market, user_id, user_settings)
            await self._record_signal_audit(
                fingerprint=fingerprint,
                signal=signal,
                user_id=user_id,
                stage="prefilter",
                outcome="passed" if prefilter_result.passed else "blocked",
                reason=prefilter_result.reason,
                payload={
                    "score": prefilter_result.score,
                    "checks": prefilter_result.checks,
                },
            )

            if not prefilter_result.passed:
                await self._record_and_notify_blocked(
                    reservation, signal, fingerprint, user_id, client_ip, prefilter_result.reason, raw_body
                )
                return {
                    "status": "blocked",
                    "reason": prefilter_result.reason,
                    "checks": prefilter_result.checks,
                }

            # Step 3: AI Analysis
            analysis = await self._run_ai_analysis(signal, market, user_settings, prefilter_result)
            await self._record_signal_audit(
                fingerprint=fingerprint,
                signal=signal,
                user_id=user_id,
                stage="ai_analysis",
                outcome=analysis.recommendation,
                reason=analysis.reasoning,
                payload={"analysis": analysis.model_dump()},
            )

            # Step 4: Build trade decision
            decision = self._build_trade_decision(signal, analysis, market, user_id, user_settings)
            await self._record_signal_audit(
                fingerprint=fingerprint,
                signal=signal,
                user_id=user_id,
                stage="trade_decision",
                outcome="execute" if decision.execute else "rejected",
                reason=decision.reason,
                payload={
                    "entry_source": decision.entry_source,
                    "exit_quality_score": decision.exit_quality_score,
                    "exit_quality_reasons": decision.exit_quality_reasons,
                    "position_size_multiplier": decision.position_size_multiplier,
                    "entry_price": decision.entry_price,
                    "stop_loss": decision.stop_loss,
                    "take_profit_levels": [level.model_dump() for level in decision.take_profit_levels],
                    "quantity": decision.quantity,
                    "order_type": decision.order_type,
                    "trailing_stop": decision.trailing_stop.model_dump(),
                },
            )

            # Step 5: Check for conflicting open positions
            if decision.execute:
                conflict_reason, conflicting_position = await self._check_position_conflict(
                    decision, user_id, user_settings
                )
                if conflict_reason and not conflicting_position:
                    decision.execute = False
                    decision.reason = conflict_reason
                elif conflict_reason and conflicting_position:
                    # Close existing position before opening reverse position
                    close_result = await self._close_conflicting_position(
                        conflicting_position, user_id, user_settings
                    )
                    if close_result.get("status") == "error":
                        decision.execute = False
                        decision.reason = f"Failed to close existing position: {close_result.get('reason')}"
                    else:
                        logger.info(
                            f"[Signal] Reverse signal: closed existing {conflicting_position.direction} position "
                            f"on {decision.ticker}, proceeding with {decision.direction.value} trade"
                        )
                    # Refresh session to clear closed position state
                    await self.session.flush()

            # Step 6: Check correlation risk (same-direction concentration)
            if decision.execute:
                correlation_risk = await self._check_correlation_risk(decision, user_id, user_settings)
                if correlation_risk.get("exceeded"):
                    decision.execute = False
                    decision.reason = correlation_risk.get("reason")

            # Step 7: Execute trade
            if decision.execute:
                result = await self._execute_trade(decision, user_id, user_settings)
            else:
                result = {"status": "rejected", "reason": decision.reason}
                # Notify rejection to Telegram
                await notify_trade_executed(decision, result)

            await self._record_signal_audit(
                fingerprint=fingerprint,
                signal=signal,
                user_id=user_id,
                stage="execution",
                outcome=str(result.get("status", "unknown")),
                reason=str(result.get("reason", "")),
                payload={"result": result},
            )

            self._update_reserved_event(
                reservation,
                status=result.get("status", "processed"),
                status_code=200,
                reason=result.get("reason", ""),
                payload=raw_body or signal.model_dump(),
            )

            # Add analysis to result for skip_interval check
            if analysis:
                result["analysis"] = analysis.model_dump()

            return result

        except Exception as e:
            logger.error(f"[Signal] Processing error: {e}")
            await notify_error(str(e))

            await self._record_signal_audit(
                fingerprint=fingerprint,
                signal=signal,
                user_id=user_id,
                stage="error",
                outcome="error",
                reason=str(e),
                payload={"error": str(e)},
            )

            if reservation:
                self._update_reserved_event(
                    reservation,
                    status="error",
                    status_code=500,
                    reason=str(e),
                    payload=raw_body or signal.model_dump(),
                )

            return {"status": "error", "reason": str(e)}

    async def _reserve_webhook_event(
        self,
        fingerprint: str,
        signal: TradingViewSignal,
        user_id: str | None,
        client_ip: str,
        payload: dict,
    ):
        """Reserve a webhook fingerprint before slow processing starts.

        C1-FIX: If an event exists with status 'received' or 'reserved',
        return it instead of None to allow retries to proceed.
        """
        lock = await _fingerprint_lock(fingerprint)
        try:
            async with lock:
                # Extended to 30 minutes to cover TradingView's retry window
                # (TradingView may retry webhooks for up to 30 minutes)
                existing = await has_recent_webhook_event(self.session, fingerprint, window_secs=1800)
                if existing:
                    if existing.status in {"received", "reserved", "retrying"}:
                        existing.status = "retrying"
                        return existing
                    return None

                event = await record_webhook_event(
                    session=self.session,
                    user_id=user_id,
                    fingerprint=fingerprint,
                    ticker=signal.ticker,
                    direction=signal.direction.value,
                    status="received",
                    status_code=202,
                    reason="reserved",
                    client_ip=client_ip,
                    payload=_safe_event_payload(payload),
                )
                await self.session.flush()
                return event
        finally:
            await _release_fingerprint_lock(fingerprint, lock)

    @staticmethod
    def _update_reserved_event(event, status: str, status_code: int, reason: str, payload: dict) -> None:
        event.status = status
        event.status_code = status_code
        event.reason = reason or ""
        event.payload_json = json.dumps(_safe_event_payload(payload or {}), default=str)

    async def _run_prefilter(
        self,
        signal: TradingViewSignal,
        market: MarketContext,
        user_id: str | None,
        user_settings: dict | None = None,
    ) -> PreFilterResult:
        """Run pre-filter checks."""
        from pre_filter import get_thresholds

        # Get user settings for limits
        max_daily_trades = int(getattr(settings.risk, "max_daily_trades", 0) or 0)
        max_daily_loss = float(getattr(settings.risk, "max_daily_loss_pct", 0.0) or 0.0)
        thresholds = get_thresholds()
        min_pass_score = float(thresholds.get("min_pass_score", signal.ticker) or 0.0)
        use_scoring = min_pass_score > 0.0

        user_risk = (user_settings or {}).get("risk") or {}
        live_trading = self._live_trading_requested(user_settings)
        live_data_quality_mode = str(
            user_risk.get("live_data_quality_mode") or settings.risk.live_data_quality_mode
        ).lower().strip()
        try:
            max_live_missing_data_checks = max(
                0,
                int(user_risk.get("max_live_missing_data_checks", settings.risk.max_live_missing_data_checks)),
            )
        except (TypeError, ValueError):
            max_live_missing_data_checks = int(settings.risk.max_live_missing_data_checks)
        if user_risk:
            # BUG FIX: Validate user risk settings to prevent bypass of risk controls.
            # Negative or extreme values could disable safety limits entirely.
            raw_daily_trades = user_risk.get("max_daily_trades")
            if raw_daily_trades is not None:
                try:
                    max_daily_trades = max(1, min(int(float(raw_daily_trades)), 200))
                except (TypeError, ValueError):
                    pass

            raw_daily_loss = user_risk.get("max_daily_loss_pct")
            if raw_daily_loss is not None:
                try:
                    max_daily_loss = max(0.1, min(float(raw_daily_loss), 100.0))
                except (TypeError, ValueError):
                    pass

        result = await run_pre_filter_async(
            signal=signal,
            market=market,
            max_daily_trades=max_daily_trades,
            max_daily_loss_pct=max_daily_loss,
            user_id=user_id,
            use_scoring=use_scoring,
            min_pass_score=min_pass_score,
            live_trading=live_trading,
            data_quality_mode=live_data_quality_mode,
            max_missing_data_checks=max_live_missing_data_checks,
        )

        record_prefilter_result(
            signal.ticker,
            signal.direction.value,
            result.passed,
            result.reason,
        )

        return result

    async def _run_ai_analysis(
        self,
        signal: TradingViewSignal,
        market: MarketContext,
        user_settings: dict | None = None,
        prefilter_result: PreFilterResult | None = None,
    ) -> AIAnalysis:
        """Run AI analysis on the signal."""
        import time
        start = time.time()

        scoped_user_settings = dict(user_settings or {})
        if prefilter_result is not None:
            active_prefilter_checks = {
                check_name: check
                for check_name, check in prefilter_result.checks.items()
                if not check.get("disabled", False)
            }
            soft_fail_count = sum(1 for check in active_prefilter_checks.values() if check.get("soft_fail", False))
            hard_fail_count = sum(
                1
                for check in active_prefilter_checks.values()
                if not check.get("passed", True) and not check.get("soft_fail", False)
            )
            missing_data_count = sum(1 for check in active_prefilter_checks.values() if check.get("missing_data", False))
            notable_checks = []
            for check_name, check in active_prefilter_checks.items():
                if check.get("soft_fail", False) or not check.get("passed", True) or check.get("missing_data", False):
                    notable_checks.append(check_name)
            scoped_user_settings["_prefilter_summary"] = {
                "score": round(float(prefilter_result.score), 2),
                "soft_fail_count": soft_fail_count,
                "hard_fail_count": hard_fail_count,
                "missing_data_count": missing_data_count,
                "notable_checks": notable_checks[:6],
            }

        analysis = await analyze_signal(signal, market, scoped_user_settings)

        latency = time.time() - start
        # Optimization 1: Track AI response time for dynamic interval
        await _track_ai_response_time(latency)

        record_ai_analysis(
            settings.ai.provider,
            analysis.recommendation,
            analysis.confidence,
            latency,
        )

        await notify_ai_analysis(signal.ticker, analysis)

        return analysis

    def _build_trade_decision(
        self,
        signal: TradingViewSignal,
        analysis: AIAnalysis,
        market: MarketContext,
        user_id: str | None,
        user_settings: dict | None = None,
    ) -> TradeDecision:
        """Build trade decision from signal and analysis."""
        decision = TradeDecision(
            signal=signal,
            ai_analysis=analysis,
            ticker=signal.ticker,
            direction=signal.direction,
            entry_price=signal.price,
        )
        exchange_cfg = (user_settings or {}).get("exchange") or {}
        decision.order_type = str(
            exchange_cfg.get("default_order_type")
            or settings.exchange.default_order_type
            or "market"
        ).lower().strip()
        limit_timeout_overrides = (
            exchange_cfg.get("limit_timeout_overrides")
            if "limit_timeout_overrides" in exchange_cfg
            else settings.exchange.limit_timeout_overrides
        )
        decision.limit_timeout_secs = resolve_limit_timeout_secs(
            signal.timeframe,
            limit_timeout_overrides,
        )

        recommendation = str(analysis.recommendation or "hold").lower().strip()

        # Check AI recommendation
        if recommendation == "reject":
            decision.execute = False
            decision.reason = f"AI rejected: {analysis.reasoning}"
            return decision

        if recommendation not in {"execute", "modify"}:
            decision.execute = False
            decision.reason = f"AI did not approve execution ({recommendation}): {analysis.reasoning}"
            return decision

        if analysis.confidence < 0.4:
            decision.execute = False
            decision.reason = f"Low confidence: {analysis.confidence:.2f}"
            return decision

        if (
            analysis.suggested_direction
            and analysis.suggested_direction != signal.direction
            and signal.direction in {SignalDirection.LONG, SignalDirection.SHORT}
        ):
            decision.execute = False
            decision.reason = (
                f"AI suggested {analysis.suggested_direction.value} but signal was "
                f"{signal.direction.value}; rejecting direction conflict"
            )
            return decision

        # Set execute flag
        decision.execute = recommendation in ("execute", "modify")

        # ── SMC/FVG entry optimization ──
        # When AI recommends "modify" and provides a suggested_entry, use it
        # as the optimal entry price instead of the raw signal price.
        # BUG FIX: If modify fails validation, fallback to original price instead of rejecting
        if decision.execute and recommendation == "modify":
            suggested = float(analysis.suggested_entry or 0)

            if suggested > 0:
                price_diff_pct = abs(suggested - signal.price) / signal.price * 100 if signal.price > 0 else 0

                # Only accept modified entry if it's within 5% of signal price
                if price_diff_pct <= 5.0:
                    logger.info(
                        f"[Signal] AI modified entry: {signal.price} → {suggested} "
                        f"({price_diff_pct:+.2f}% adjustment via SMC/FVG)"
                    )
                    decision.entry_price = suggested
                    decision.entry_source = "ai_modified"
                else:
                    # Fallback: suggested entry too far, use original price
                    logger.warning(
                        f"[Signal] AI suggested entry {suggested} is {price_diff_pct:.2f}% away from signal price, "
                        f"falling back to original signal price {signal.price}"
                    )
                    decision.entry_price = signal.price
                    decision.entry_source = "fallback_raw_invalid_ai_entry"
                    # Don't reject the trade, just use original price
            else:
                # Fallback: no valid suggested_entry, use original signal price
                logger.warning(
                    f"[Signal] AI recommended modify without valid suggested_entry, "
                    f"using original signal price {signal.price}"
                )
                decision.entry_price = signal.price
                decision.entry_source = "fallback_raw_missing_ai_entry"
                # Don't reject the trade, continue with original price

        if decision.execute:
            self._apply_exit_plan(decision, signal, analysis, market, user_settings or {})
            if signal.direction in {SignalDirection.LONG, SignalDirection.SHORT}:
                if not decision.stop_loss:
                    decision.execute = False
                    decision.reason = "No valid stop loss available for opening trade"
                    return decision
                if not decision.take_profit_levels:
                    decision.execute = False
                    decision.reason = "No valid take-profit target available for opening trade"
                    return decision
                # Validate R:R ratio
                rr_valid, rr_reason = self._validate_risk_reward_ratio(
                    decision.entry_price, decision.stop_loss,
                    decision.take_profit_levels, signal.direction, user_settings or {}
                )
                if not rr_valid:
                    decision.execute = False
                    decision.reason = rr_reason
                    return decision
                self._apply_entry_exit_quality(decision, signal, analysis, market, user_settings or {})

        # Set trailing stop - use smart selector if not explicitly configured
        trailing_cfg = (user_settings or {}).get("trailing_stop") or {}
        user_trailing_mode = str(trailing_cfg.get("mode") or "").lower()

        # Determine trailing stop mode
        if user_trailing_mode and user_trailing_mode != "none" and user_trailing_mode != "auto":
            # User explicitly configured a mode (not "auto")
            trailing_mode = user_trailing_mode
            trailing_reason = "User configured trailing stop mode"
        elif user_trailing_mode == "auto" or not user_trailing_mode:
            # Use smart trailing stop selector
            from smart_trailing_stop import select_smart_trailing_stop
            from timeframe_exits import get_timeframe_config

            tf_config = get_timeframe_config(str(signal.timeframe or "60"))
            num_tp_levels = self._max_tp_levels(user_settings)

            trailing_decision = select_smart_trailing_stop(
                confidence=analysis.confidence,
                market_condition=analysis.market_condition,
                trend_strength=analysis.trend_strength or "moderate",
                risk_score=analysis.risk_score,
                timeframe=str(signal.timeframe or "60"),
                num_tp_levels=num_tp_levels,
                atr_pct=safe_float(market.atr_pct, tf_config.default_sl_pct),
                user_override=None,  # Auto mode, no override
            )
            trailing_mode = trailing_decision.mode.value
            trailing_reason = trailing_decision.reasoning

            # Log the smart selection
            logger.info(
                f"[Signal] Smart trailing stop selected: {trailing_mode} "
                f"(confidence={analysis.confidence:.2f}, market={analysis.market_condition}, "
                f"trend={analysis.trend_strength or 'moderate'}, reason={trailing_reason})"
            )
        else:
            # Default from settings
            trailing_mode = str(settings.trailing_stop.mode)
            trailing_reason = "Default from server settings"

        # Apply trailing stop if not "none"
        if trailing_mode != "none":
            from models import TrailingStopConfig, TrailingStopMode
            decision.trailing_stop = TrailingStopConfig(
                mode=TrailingStopMode(trailing_mode),
                trail_pct=safe_float(first_valid(trailing_cfg.get("trail_pct"), settings.trailing_stop.trail_pct), settings.trailing_stop.trail_pct),
                activation_profit_pct=safe_float(
                    first_valid(trailing_cfg.get("activation_profit_pct"), settings.trailing_stop.activation_profit_pct),
                    settings.trailing_stop.activation_profit_pct,
                ),
                trailing_step_pct=safe_float(
                    first_valid(trailing_cfg.get("trailing_step_pct"), settings.trailing_stop.trailing_step_pct),
                    settings.trailing_stop.trailing_step_pct,
                ),
            )

        # Calculate position size
        decision.quantity = self._calculate_position_size(
            market.current_price or signal.price,
            analysis.position_size_pct,
            analysis.recommended_leverage,
            decision=decision,
            user_settings=user_settings,
        )
        if decision.quantity and decision.position_size_multiplier < 1.0:
            original_qty = decision.quantity
            decision.quantity = float(round(decision.quantity * decision.position_size_multiplier, 6))
            logger.info(
                f"[Signal] Position size adjusted by entry/exit quality: "
                f"{original_qty} -> {decision.quantity} (multiplier={decision.position_size_multiplier:.2f}, "
                f"score={decision.exit_quality_score:.1f})"
            )

        decision.reason = analysis.reasoning
        return decision

    def _apply_exit_plan(
        self,
        decision: TradeDecision,
        signal: TradingViewSignal,
        analysis: AIAnalysis,
        market: MarketContext,
        user_settings: dict,
    ) -> None:
        """Apply either custom configured exits or validated AI-generated exits.

        Enhanced validation:
        - Minimum SL/TP distance (based on ATR or percentage)
        - Maximum SL distance (prevent oversized risk)
        - R:R ratio validation
        """
        if signal.direction not in {SignalDirection.LONG, SignalDirection.SHORT}:
            return

        risk_cfg = user_settings.get("risk") or {}
        exit_mode = str(risk_cfg.get("exit_management_mode") or settings.risk.exit_management_mode)
        atr_pct = safe_float(market.atr_pct, 0.0)

        if exit_mode == "custom":
            self._apply_custom_exit_plan(decision, signal, user_settings, atr_pct)
            return

        # AI-generated exits are the source of truth; the server only applies safety guards.
        # BUG FIX: Use decision.entry_price (may be modified by AI) instead of signal.price
        entry_price = float(decision.entry_price or signal.price or 0)
        timeframe = str(signal.timeframe or "60")
        sl_price = self._valid_stop_loss(
            signal.direction, entry_price, analysis.suggested_stop_loss,
            atr_pct=atr_pct, user_settings=user_settings, timeframe=timeframe
        )
        if not sl_price:
            sl_price = self._fallback_stop_loss(
                signal.direction,
                entry_price,
                atr_pct=atr_pct,
                user_settings=user_settings,
                timeframe=timeframe,
            )
            if sl_price:
                logger.warning(
                    f"[Signal] AI stop loss missing/invalid for {signal.ticker}; "
                    f"using deterministic fallback SL={sl_price}"
                )
                decision.exit_quality_reasons.append("server_fallback_stop_loss")
        decision.stop_loss = sl_price
        if sl_price:
            self._append_stop_loss_advisory_warnings(
                analysis,
                signal,
                entry_price,
                sl_price,
                atr_pct=atr_pct,
                user_settings=user_settings,
                timeframe=timeframe,
            )

        raw_levels = [
            (analysis.suggested_tp1, analysis.tp1_qty_pct),
            (analysis.suggested_tp2, analysis.tp2_qty_pct),
            (analysis.suggested_tp3, analysis.tp3_qty_pct),
            (analysis.suggested_tp4, analysis.tp4_qty_pct),
        ]
        max_levels = self._max_tp_levels(user_settings)
        decision.take_profit_levels = self._build_take_profit_levels(
            signal.direction, entry_price, raw_levels, max_levels,
            atr_pct=atr_pct, sl_price=sl_price, user_settings=user_settings, timeframe=timeframe
        )
        if decision.take_profit_levels:
            decision.take_profit = decision.take_profit_levels[0].price

    def _apply_custom_exit_plan(
        self,
        decision: TradeDecision,
        signal: TradingViewSignal,
        user_settings: dict,
        atr_pct: float = 0.0
    ) -> None:
        """Build fixed percentage SL/TP exits from admin configuration.

        Also validates against minimum/maximum distance requirements.
        """
        # BUG FIX: Use decision.entry_price (may be modified by AI) instead of signal.price
        entry = float(decision.entry_price or signal.price or 0)
        if entry <= 0:
            return

        timeframe = str(signal.timeframe or "60")
        risk_cfg = user_settings.get("risk") or {}
        tp_cfg = user_settings.get("take_profit") or {}
        stop_pct = max(0.01, safe_float(first_valid(risk_cfg.get("custom_stop_loss_pct"), settings.risk.custom_stop_loss_pct), 0.0))

        # Validate SL percentage against minimum requirements
        min_sl_pct = self._get_min_sl_percentage(atr_pct, user_settings, timeframe)
        max_sl_pct = self._get_max_sl_percentage(user_settings, timeframe)
        if stop_pct < min_sl_pct:
            logger.warning(f"[Signal] Custom SL {stop_pct}% below minimum {min_sl_pct}%, adjusting")
            stop_pct = min_sl_pct
        if stop_pct > max_sl_pct:
            logger.warning(f"[Signal] Custom SL {stop_pct}% above maximum {max_sl_pct}%, adjusting")
            stop_pct = max_sl_pct

        tp1_pct = safe_float(first_valid(tp_cfg.get("tp1_pct"), settings.take_profit.tp1_pct), settings.take_profit.tp1_pct)
        tp2_pct = safe_float(first_valid(tp_cfg.get("tp2_pct"), settings.take_profit.tp2_pct), settings.take_profit.tp2_pct)
        tp3_pct = safe_float(first_valid(tp_cfg.get("tp3_pct"), settings.take_profit.tp3_pct), settings.take_profit.tp3_pct)
        tp4_pct = safe_float(first_valid(tp_cfg.get("tp4_pct"), settings.take_profit.tp4_pct), settings.take_profit.tp4_pct)

        # Validate TP percentages against minimum
        min_tp_pct = self._get_min_tp_percentage(atr_pct, user_settings, timeframe)
        tp1_pct = max(min_tp_pct, tp1_pct)

        tp1_qty = safe_float(first_valid(tp_cfg.get("tp1_qty"), settings.take_profit.tp1_qty), settings.take_profit.tp1_qty)
        tp2_qty = safe_float(first_valid(tp_cfg.get("tp2_qty"), settings.take_profit.tp2_qty), settings.take_profit.tp2_qty)
        tp3_qty = safe_float(first_valid(tp_cfg.get("tp3_qty"), settings.take_profit.tp3_qty), settings.take_profit.tp3_qty)
        tp4_qty = safe_float(first_valid(tp_cfg.get("tp4_qty"), settings.take_profit.tp4_qty), settings.take_profit.tp4_qty)

        if signal.direction == SignalDirection.LONG:
            decision.stop_loss = round(entry * (1 - stop_pct / 100.0), 8)
            raw_levels = [
                (entry * (1 + tp1_pct / 100.0), tp1_qty),
                (entry * (1 + tp2_pct / 100.0), tp2_qty),
                (entry * (1 + tp3_pct / 100.0), tp3_qty),
                (entry * (1 + tp4_pct / 100.0), tp4_qty),
            ]
        else:
            decision.stop_loss = round(entry * (1 + stop_pct / 100.0), 8)
            raw_levels = [
                (entry * (1 - tp1_pct / 100.0), tp1_qty),
                (entry * (1 - tp2_pct / 100.0), tp2_qty),
                (entry * (1 - tp3_pct / 100.0), tp3_qty),
                (entry * (1 - tp4_pct / 100.0), tp4_qty),
            ]

        decision.take_profit_levels = self._build_take_profit_levels(
            signal.direction,
            entry,
            raw_levels,
            self._max_tp_levels(user_settings),
            atr_pct=atr_pct,
            sl_price=decision.stop_loss,
            user_settings=user_settings,
            timeframe=timeframe,
        )
        if decision.take_profit_levels:
            decision.take_profit = decision.take_profit_levels[0].price

    def _build_take_profit_levels(
        self,
        direction: SignalDirection,
        entry: float,
        raw_levels: Sequence[tuple[float | None, float]],
        max_levels: int,
        atr_pct: float = 0.0,
        sl_price: float | None = None,
        user_settings: dict | None = None,
        timeframe: str = "60",
    ) -> list:
        """Validate TP direction, distance, and cap cumulative close quantity to 100%.

        Enhanced validation:
        - Minimum TP distance (ATR-based or percentage floor)
        - Maximum TP distance (timeframe-based, warns if exceeded)
        - R:R ratio check (TP distance vs SL distance)
        """
        from models import TakeProfitLevel
        from timeframe_exits import get_max_tp_for_timeframe

        min_tp_pct = self._get_min_tp_percentage(atr_pct, user_settings or {}, timeframe)
        max_tp_pct = get_max_tp_for_timeframe(timeframe)

        # Get min R:R ratio from settings or derive from ai_risk_profile
        risk_cfg = (user_settings or {}).get("risk") or {}
        if risk_cfg.get("min_risk_reward_ratio") is not None:
            min_rr_ratio = safe_float(risk_cfg.get("min_risk_reward_ratio"), 1.5)
        else:
            # Derive from ai_risk_profile when not explicitly set
            from core.config import settings
            profile = str(risk_cfg.get("ai_risk_profile") or settings.risk.ai_risk_profile or "balanced").lower().strip()
            profile_rr_defaults = {"conservative": 2.0, "balanced": 1.5, "aggressive": 1.2}
            min_rr_ratio = profile_rr_defaults.get(profile, 1.5)

        levels = []
        remaining_pct = 100.0

        for price, qty_pct in raw_levels[:max_levels]:
            price = self._valid_take_profit(direction, entry, price, min_tp_pct=min_tp_pct, max_tp_pct=max_tp_pct)
            if not price:
                continue
            # Additional R:R validation if SL is provided
            if sl_price and entry > 0:
                tp_dist_pct = abs(price - entry) / entry * 100
                sl_dist_pct = abs(sl_price - entry) / entry * 100
                if sl_dist_pct > 0:
                    rr_ratio = tp_dist_pct / sl_dist_pct
                    if rr_ratio < min_rr_ratio:
                        logger.warning(
                            f"[Signal] TP at {price} has R:R {rr_ratio:.2f}:1, below minimum {min_rr_ratio}:1. "
                            f"Skipping this TP level."
                        )
                        continue
            qty = max(0.0, min(float(qty_pct or 0.0), remaining_pct))
            if qty <= 0:
                continue
            levels.append(TakeProfitLevel(price=round(price, 8), qty_pct=round(qty, 4)))
            remaining_pct -= qty
            if remaining_pct <= 0:
                break

        # BUG FIX: Sort TP levels by distance from entry (closest first).
        # For LONG: ascending price; for SHORT: descending price.
        # This ensures TP1 is always the nearest target.
        if levels:
            if direction == SignalDirection.LONG:
                levels.sort(key=lambda tp: tp.price)
            elif direction == SignalDirection.SHORT:
                levels.sort(key=lambda tp: tp.price, reverse=True)

        if not levels and raw_levels:
            fallback = self._valid_take_profit(direction, entry, raw_levels[0][0], min_tp_pct=min_tp_pct, max_tp_pct=max_tp_pct)
            if fallback:
                levels.append(TakeProfitLevel(price=round(fallback, 8), qty_pct=100.0))
        return levels

    @staticmethod
    def _max_tp_levels(user_settings: dict) -> int:
        tp_cfg = user_settings.get("take_profit") or {}
        return max(1, min(int(tp_cfg.get("num_levels") or settings.take_profit.num_levels or 1), 4))

    @staticmethod
    def _append_stop_loss_advisory_warnings(
        analysis: AIAnalysis,
        signal: TradingViewSignal,
        entry: float,
        sl_price: float,
        atr_pct: float = 0.0,
        user_settings: dict | None = None,
        timeframe: str = "60",
    ) -> None:
        """Record soft risk warnings when AI SL is outside ATR/timeframe guidance."""
        if entry <= 0 or sl_price <= 0:
            return

        sl_dist_pct = abs(sl_price - entry) / entry * 100
        min_sl_pct = SignalProcessor._get_min_sl_percentage(atr_pct, user_settings or {}, timeframe)
        max_sl_pct = SignalProcessor._get_max_sl_percentage(user_settings or {}, timeframe)

        if sl_dist_pct < min_sl_pct:
            warning = (
                f"AI stop loss distance {sl_dist_pct:.2f}% is below ATR/timeframe guidance "
                f"{min_sl_pct:.2f}% for {timeframe}; accepted as AI invalidation, with higher stop-out risk"
            )
            if warning not in analysis.warnings:
                analysis.warnings.append(warning)
            logger.warning(f"[Signal] {signal.ticker}: {warning}")
        elif max_sl_pct > 0 and sl_dist_pct > max_sl_pct:
            warning = (
                f"AI stop loss distance {sl_dist_pct:.2f}% is above timeframe guidance "
                f"{max_sl_pct:.2f}% for {timeframe}; accepted, position sizing will cap risk where possible"
            )
            if warning not in analysis.warnings:
                analysis.warnings.append(warning)
            logger.warning(f"[Signal] {signal.ticker}: {warning}")

    @staticmethod
    def _valid_stop_loss(
        direction: SignalDirection,
        entry: float,
        price: float | None,
        atr_pct: float = 0.0,
        user_settings: dict | None = None,
        timeframe: str = "60",
    ) -> float | None:
        """Validate stop loss using hard safety guards only.

        AI-generated stops are treated as the trading invalidation level. ATR and
        timeframe ranges are advisory elsewhere; this validator only rejects
        values that are unsafe to send to the exchange.
        """
        try:
            value = float(price or 0)
            entry = float(entry or 0)
        except (TypeError, ValueError):
            return None
        if value <= 0 or entry <= 0 or not math.isfinite(value) or not math.isfinite(entry):
            return None

        # Reject SL that equals entry (immediate trigger)
        if abs(value - entry) < entry * 0.0001:  # 0.01% tolerance
            logger.warning(f"[Signal] SL {value} too close to entry {entry}, rejecting")
            return None

        # Direction check
        if direction == SignalDirection.LONG and value >= entry:
            return None
        if direction == SignalDirection.SHORT and value <= entry:
            return None

        sl_dist_pct = abs(value - entry) / entry * 100
        min_sl_pct = SignalProcessor._get_min_sl_percentage(atr_pct, user_settings or {}, timeframe)
        max_sl_pct = SignalProcessor._get_max_sl_percentage(user_settings or {}, timeframe)
        if sl_dist_pct < min_sl_pct:
            logger.warning(
                f"[Signal] SL distance {sl_dist_pct:.2f}% below ATR/timeframe guidance {min_sl_pct:.2f}% "
                f"(entry={entry}, sl={value}); accepting AI invalidation level"
            )
        elif max_sl_pct > 0 and sl_dist_pct > max_sl_pct:
            logger.warning(
                f"[Signal] SL distance {sl_dist_pct:.2f}% above timeframe guidance {max_sl_pct:.2f}% "
                f"(entry={entry}, sl={value}); accepting and relying on risk-based position cap"
            )

        return round(value, 8)

    @staticmethod
    def _fallback_stop_loss(
        direction: SignalDirection,
        entry: float,
        atr_pct: float = 0.0,
        user_settings: dict | None = None,
        timeframe: str = "60",
    ) -> float | None:
        """Build a bounded fallback SL when AI exits are incomplete."""
        try:
            entry = float(entry or 0)
        except (TypeError, ValueError):
            return None
        if entry <= 0:
            return None

        from timeframe_exits import get_default_sl_for_timeframe

        user_settings = user_settings or {}
        min_sl_pct = SignalProcessor._get_min_sl_percentage(atr_pct, user_settings, timeframe)
        max_sl_pct = SignalProcessor._get_max_sl_percentage(user_settings, timeframe)
        if max_sl_pct <= 0:
            return None
        if min_sl_pct > max_sl_pct:
            min_sl_pct = max_sl_pct

        default_sl_pct = get_default_sl_for_timeframe(timeframe)
        atr_sl_pct = atr_pct * 1.5 if atr_pct > 0 else default_sl_pct
        sl_pct = max(min_sl_pct, min(atr_sl_pct, max_sl_pct))

        # P2-10: Hard boundaries to prevent extreme fallback SL values
        absolute_min_sl_pct = 0.1  # Never less than 0.1% from entry
        absolute_max_sl_pct = 50.0  # Never more than 50% from entry
        sl_pct = max(absolute_min_sl_pct, min(sl_pct, absolute_max_sl_pct))

        if direction == SignalDirection.LONG:
            price = entry * (1 - sl_pct / 100.0)
        elif direction == SignalDirection.SHORT:
            price = entry * (1 + sl_pct / 100.0)
        else:
            return None

        return SignalProcessor._valid_stop_loss(
            direction,
            entry,
            price,
            atr_pct=atr_pct,
            user_settings=user_settings,
            timeframe=timeframe,
        )

    @staticmethod
    def _valid_take_profit(
        direction: SignalDirection,
        entry: float,
        price: float | None,
        min_tp_pct: float = 0.0,
        max_tp_pct: float = 0.0,
    ) -> float | None:
        """Validate take profit with minimum/maximum distance requirement.

        Checks:
        1. Basic direction (LONG: TP > entry, SHORT: TP < entry)
        2. Minimum distance (auto-adjusts if too close)
        3. Maximum distance (warns if too far, but allows)

        If TP distance is below minimum, auto-adjusts to minimum distance
        instead of rejecting outright.
        """
        try:
            value = float(price or 0)
            entry = float(entry or 0)
        except (TypeError, ValueError):
            return None
        if value <= 0 or entry <= 0:
            return None

        # Direction check
        if direction == SignalDirection.LONG and value <= entry:
            return None
        if direction == SignalDirection.SHORT and value >= entry:
            return None

        # Minimum distance check - auto-adjust if too tight
        tp_dist_pct = abs(value - entry) / entry * 100
        if min_tp_pct <= 0:
            min_tp_pct = 0.3  # Default 0.3% minimum

        if max_tp_pct > 0 and tp_dist_pct > max_tp_pct:
            logger.warning(
                f"[Signal] TP distance {tp_dist_pct:.2f}% above suggested max {max_tp_pct:.2f}% "
                f"(entry={entry}, tp={value}), may be hard to reach for this timeframe"
            )

        if tp_dist_pct < min_tp_pct:
            logger.warning(
                f"[Signal] TP distance {tp_dist_pct:.2f}% below minimum {min_tp_pct:.2f}% "
                f"(entry={entry}, tp={value}), auto-adjusting to minimum distance"
            )
            # Adjust TP to minimum distance
            if direction == SignalDirection.LONG:
                value = entry * (1 + min_tp_pct / 100.0)
            else:
                value = entry * (1 - min_tp_pct / 100.0)

        return round(value, 8)

    @staticmethod
    def _get_min_sl_percentage(atr_pct: float, user_settings: dict, timeframe: str = "60") -> float:
        """Calculate minimum SL percentage based on ATR, config, and timeframe."""
        from timeframe_exits import get_min_sl_for_timeframe

        risk_cfg = user_settings.get("risk") or {}
        # Timeframe-based minimum (most important for realistic exits)
        tf_min = get_min_sl_for_timeframe(timeframe)
        # Config override can tighten but not loosen
        config_min = safe_float(risk_cfg.get("min_stop_loss_pct"), tf_min)
        # ATR-based minimum (dynamic volatility adjustment)
        atr_min = atr_pct * 1.2 if atr_pct > 0 else tf_min
        # Return the most restrictive minimum
        return max(tf_min, config_min, atr_min, 0.15)

    @staticmethod
    def _get_max_sl_percentage(user_settings: dict, timeframe: str = "60") -> float:
        """Maximum allowed SL percentage based on timeframe."""
        from timeframe_exits import get_max_sl_for_timeframe

        risk_cfg = user_settings.get("risk") or {}
        tf_max = get_max_sl_for_timeframe(timeframe)
        config_max = safe_float(risk_cfg.get("max_stop_loss_pct"), tf_max)
        # Use the more restrictive (smaller) max
        return min(tf_max, config_max)

    @staticmethod
    def _get_min_tp_percentage(atr_pct: float, user_settings: dict, timeframe: str = "60") -> float:
        """Calculate minimum TP percentage based on ATR, config, and timeframe."""
        from timeframe_exits import get_min_tp_for_timeframe

        tp_cfg = user_settings.get("take_profit") or {}
        tf_min = get_min_tp_for_timeframe(timeframe)
        config_min = safe_float(tp_cfg.get("min_tp_pct"), tf_min)
        atr_min = atr_pct * 0.8 if atr_pct > 0 else tf_min
        return max(tf_min, config_min, atr_min, 0.2)

    def _validate_risk_reward_ratio(
        self,
        entry: float,
        sl: float | None,
        tp_levels: list,
        direction: SignalDirection,
        user_settings: dict,
    ) -> tuple[bool, str]:
        """Validate that the trade has acceptable risk/reward ratio.

        Returns (is_valid, reason) tuple.
        Checks (prioritized by weighted average first):
        - Weighted average TP distance vs SL distance (minimum 1.2:1)
        - TP1 distance vs SL distance (minimum 1.5:1) - only checked if average fails
        """
        if not sl or not tp_levels or entry <= 0:
            return (False, "Missing SL or TP for R:R validation")

        sl_dist_pct = abs(sl - entry) / entry * 100
        if sl_dist_pct <= 0:
            return (False, "Invalid SL distance")

        risk_cfg = user_settings.get("risk") or {}
        if risk_cfg.get("min_risk_reward_ratio") is not None:
            min_rr_ratio = safe_float(risk_cfg.get("min_risk_reward_ratio"), 1.5)
        else:
            # Derive from ai_risk_profile when not explicitly set
            from core.config import settings
            profile = str(risk_cfg.get("ai_risk_profile") or settings.risk.ai_risk_profile or "balanced").lower().strip()
            profile_rr_defaults = {"conservative": 2.0, "balanced": 1.5, "aggressive": 1.2}
            min_rr_ratio = profile_rr_defaults.get(profile, 1.5)
        min_avg_rr = safe_float(risk_cfg.get("min_avg_risk_reward_ratio"), 1.2)

        # Calculate TP1 R:R
        tp1_dist_pct = abs(tp_levels[0].price - entry) / entry * 100
        tp1_rr = tp1_dist_pct / sl_dist_pct

        # Calculate weighted average TP R:R
        total_qty = sum(tp.qty_pct for tp in tp_levels)
        avg_rr = 0.0
        if total_qty > 0:
            avg_tp_dist = sum(
                abs(tp.price - entry) / entry * 100 * tp.qty_pct
                for tp in tp_levels
            ) / total_qty
            avg_rr = avg_tp_dist / sl_dist_pct

            # Prioritize weighted average R:R check
            if avg_rr >= min_avg_rr:
                logger.info(
                    f"[Signal] R:R validation passed (weighted average): "
                    f"avg={avg_rr:.2f}:1 >= min_avg={min_avg_rr:.2f}:1, "
                    f"TP1={tp1_rr:.2f}:1, TP levels={len(tp_levels)}"
                )
                return (True, "")

            # If average fails, check TP1 as fallback safety
            if tp1_rr >= min_rr_ratio:
                logger.info(
                    f"[Signal] R:R validation passed (TP1 fallback): "
                    f"TP1={tp1_rr:.2f}:1 >= min={min_rr_ratio:.2f}:1, "
                    f"avg={avg_rr:.2f}:1 < min_avg={min_avg_rr:.2f}:1"
                )
                return (True, "")

            # Both average and TP1 fail
            return (
                False,
                f"R:R validation failed: weighted avg={avg_rr:.2f}:1 < {min_avg_rr:.2f}:1, "
                f"TP1={tp1_rr:.2f}:1 < {min_rr_ratio:.2f}:1 "
                f"(TP1={tp_levels[0].price}, SL={sl}, entry={entry}, TP levels={len(tp_levels)}, "
                f"total_qty_pct={total_qty:.1f}%)"
            )

        return (False, "Invalid TP quantities for R:R calculation")

    def _apply_entry_exit_quality(
        self,
        decision: TradeDecision,
        signal: TradingViewSignal,
        analysis: AIAnalysis,
        market: MarketContext,
        user_settings: dict,
    ) -> None:
        """Score entry/exit quality and reduce size for weaker but still valid setups."""
        if not decision.execute or not decision.entry_price or not decision.stop_loss or not decision.take_profit_levels:
            return

        score = 100.0
        reasons = list(decision.exit_quality_reasons)
        entry = float(decision.entry_price)
        sl_dist_pct = abs(float(decision.stop_loss) - entry) / entry * 100 if entry > 0 else 0.0
        atr_pct = safe_float(market.atr_pct, 0.0)
        timeframe = str(signal.timeframe or "60")

        if decision.entry_source.startswith("fallback_raw"):
            score -= 20.0
            reasons.append(decision.entry_source)

        if analysis.confidence < 0.55:
            score -= 15.0
            reasons.append("low_ai_confidence")
        elif analysis.confidence < 0.70:
            score -= 6.0
            reasons.append("moderate_ai_confidence")

        if analysis.risk_score > 0.75:
            score -= 15.0
            reasons.append("high_ai_risk_score")
        elif analysis.risk_score > 0.55:
            score -= 7.0
            reasons.append("elevated_ai_risk_score")

        if market.atr_pct is None:
            score -= 6.0
            reasons.append("missing_atr_for_exit_quality")
        elif atr_pct > 0:
            min_sl_pct = self._get_min_sl_percentage(atr_pct, user_settings, timeframe)
            max_sl_pct = self._get_max_sl_percentage(user_settings, timeframe)
            if sl_dist_pct < min_sl_pct:
                score -= 12.0
                reasons.append("stop_loss_below_atr_timeframe_guidance")
            elif max_sl_pct > 0 and sl_dist_pct > max_sl_pct:
                score -= 8.0
                reasons.append("stop_loss_above_timeframe_guidance")

        total_qty = sum(float(tp.qty_pct or 0.0) for tp in decision.take_profit_levels)
        if total_qty > 0 and sl_dist_pct > 0:
            avg_tp_dist = sum(
                abs(float(tp.price) - entry) / entry * 100 * float(tp.qty_pct or 0.0)
                for tp in decision.take_profit_levels
            ) / total_qty
            avg_rr = avg_tp_dist / sl_dist_pct
            if avg_rr < 1.5:
                score -= 10.0
                reasons.append(f"thin_average_rr_{avg_rr:.2f}")
            elif avg_rr >= 2.0:
                score += 3.0

        if len(decision.take_profit_levels) == 1:
            score -= 3.0
            reasons.append("single_take_profit_level")

        if analysis.warnings:
            warning_penalty = min(10.0, len(analysis.warnings) * 3.0)
            score -= warning_penalty
            reasons.append("ai_warnings_present")

        score = max(0.0, min(100.0, score))
        multiplier = 1.0
        if decision.entry_source.startswith("fallback_raw"):
            multiplier = min(multiplier, 0.75)
        if "server_fallback_stop_loss" in reasons:
            multiplier = min(multiplier, 0.80)
        if score < 60.0:
            multiplier = min(multiplier, 0.50)
        elif score < 75.0:
            multiplier = min(multiplier, 0.75)
        elif score < 85.0:
            multiplier = min(multiplier, 0.90)

        decision.exit_quality_score = round(score, 2)
        decision.position_size_multiplier = round(multiplier, 4)
        decision.exit_quality_reasons = list(dict.fromkeys(reasons))

    async def _check_correlation_risk(
        self,
        decision: TradeDecision,
        user_id: str | None,
        user_settings: dict | None = None,
    ) -> dict:
        """
        Check for correlation risk - too many positions in the same direction.

        Prevents over-concentration in one direction (e.g., multiple LONG positions)
        which would amplify losses if market moves against that direction.

        Returns dict with:
        - exceeded: bool - whether limit is exceeded
        - reason: str - explanation
        - current_exposure: dict - current position summary
        """
        result = {
            "exceeded": False,
            "reason": "",
            "current_exposure": {
                "long_positions": 0,
                "short_positions": 0,
                "long_notional_usdt": 0.0,
                "short_notional_usdt": 0.0,
            },
        }

        try:
            stmt = select(PositionModel).where(PositionModel.status.in_(["open", "pending"]))
            if user_id:
                stmt = stmt.where(PositionModel.user_id == user_id)

            db_result = await self.session.execute(stmt)
            positions = list(db_result.scalars().all())

            if not positions:
                return result

            # Count positions by direction
            long_positions = []
            short_positions = []

            for pos in positions:
                pos_dir = str(pos.direction or "long").lower()
                entry = safe_float(pos.entry_price)
                qty = safe_float(pos.remaining_quantity or pos.quantity)
                # Get contract_size from position's trailing_stop_config
                try:
                    pos_ts_config = json.loads(pos.trailing_stop_config_json or "{}")
                except (json.JSONDecodeError, TypeError):
                    pos_ts_config = {}
                pos_contract_size = safe_float(pos_ts_config.get("_contract_size"), 1.0)

                notional = entry * qty * pos_contract_size if entry > 0 and qty > 0 else 0

                if pos_dir == "long":
                    long_positions.append({"ticker": pos.ticker, "notional": notional})
                elif pos_dir == "short":
                    short_positions.append({"ticker": pos.ticker, "notional": notional})

            result["current_exposure"]["long_positions"] = len(long_positions)
            result["current_exposure"]["short_positions"] = len(short_positions)
            result["current_exposure"]["long_notional_usdt"] = sum(p["notional"] for p in long_positions)
            result["current_exposure"]["short_notional_usdt"] = sum(p["notional"] for p in short_positions)

            # Get correlation limits from settings
            risk_cfg = (user_settings or {}).get("risk") or {}
            max_same_direction_positions = safe_int(
                first_valid(risk_cfg.get("max_same_direction_positions"), settings.risk.max_same_direction_positions),
                5,
            )
            max_correlated_pct = safe_float(
                first_valid(risk_cfg.get("max_correlated_exposure_pct"), settings.risk.max_correlated_exposure_pct),
                50.0,
            )

            # Check if new position would exceed limits
            new_direction = str(decision.direction.value or "long").lower()
            if new_direction == "long":
                current_count = len(long_positions)
                current_notional = sum(p["notional"] for p in long_positions)
            else:
                current_count = len(short_positions)
                current_notional = sum(p["notional"] for p in short_positions)

            # Position count limit
            if current_count >= max_same_direction_positions:
                result["exceeded"] = True
                result["reason"] = (
                    f"Correlation risk: {current_count} {new_direction} positions already open "
                    f"(max={max_same_direction_positions}). Adding more would over-concentrate risk."
                )
                logger.warning(f"[Signal] Correlation risk exceeded: {result['reason']}")
                return result

            # Notional exposure limit
            risk_settings = self._resolved_risk_settings(user_settings)
            equity = float(risk_settings.get("account_equity_usdt") or 1000)
            if risk_settings.get("position_sizing_mode") == "fixed":
                max_leverage = 125
                if user_id:
                    user = await get_user_by_id(self.session, user_id)
                    max_leverage = int(getattr(user, "max_leverage", None) or max_leverage) if user else max_leverage
                new_notional = float(risk_settings.get("fixed_position_size_usdt") or 100.0) * self._effective_leverage(
                    decision.ai_analysis,
                    max_leverage,
                )
            else:
                # Get contract_size for new position notional calculation
                new_contract_size = 1.0
                try:
                    from exchange import get_market_limits
                    exchange_config = self._get_exchange_config(user_settings)
                    exchange_id = exchange_config.get("exchange") or exchange_config.get("name") or settings.exchange.name
                    market_type = exchange_config.get("market_type") or settings.exchange.market_type
                    limits = get_market_limits(exchange_id, decision.ticker, market_type)
                    if limits and limits.get("contract_size", 1.0) > 1.0:
                        new_contract_size = float(limits.get("contract_size", 1.0))
                except Exception:
                    pass
                new_notional = decision.entry_price * decision.quantity * new_contract_size if decision.entry_price and decision.quantity else 0
            total_notional_after = current_notional + new_notional
            exposure_pct = total_notional_after / equity * 100 if equity > 0 else 0

            if exposure_pct > max_correlated_pct:
                result["exceeded"] = True
                result["reason"] = (
                    f"Correlation risk: {new_direction} exposure would be {exposure_pct:.1f}% "
                    f"of equity (max={max_correlated_pct}%). "
                    f"Current={current_notional:.2f}USDT, New={new_notional:.2f}USDT, Equity={equity:.2f}USDT"
                )
                logger.warning(f"[Signal] Correlation risk exceeded: {result['reason']}")
                return result

            # Log correlation status
            logger.info(
                f"[Signal] Correlation check passed: {current_count + 1} {new_direction} positions "
                f"(exposure={exposure_pct:.1f}%, max={max_correlated_pct}%)"
            )

        except Exception as e:
            live_trading = self._live_trading_requested(user_settings)
            if live_trading and self._block_live_risk_check_errors(user_settings):
                result["exceeded"] = True
                result["reason"] = f"Correlation risk check failed in live mode: {e}"
                logger.error(f"[Signal] {result['reason']}")
                return result
            logger.warning(f"[Signal] Correlation check failed (allowing trade): {e}")

        return result

    @staticmethod
    def _normalize_size_pct(size_pct: float) -> float:
        """Normalize AI-returned size_pct to a 0-1 fraction.

        AI models may return either a 0-1 fraction or a 1-100 percentage.
        We detect which format was used and always return a 0-1 fraction.
        """
        value = float(size_pct or 0.0)
        if value <= 0:
            return 0.0
        # Values > 1 are treated as percentages (e.g. 50 means 50%)
        if value > 1.0:
            value = value / 100.0
        return max(0.0, min(value, 1.0))

    @staticmethod
    def _coerce_risk_float(value, default: float, min_value: float, max_value: float) -> float:
        if isinstance(value, bool):
            parsed = default
        elif isinstance(value, (int, float)):
            parsed = float(value)
        elif isinstance(value, str) and value.strip():
            try:
                parsed = float(value)
            except ValueError:
                parsed = default
        else:
            parsed = default
        return max(min_value, min(parsed, max_value))

    @classmethod
    def _effective_leverage(cls, ai_analysis: AIAnalysis | None, max_leverage: float | int | None = None) -> float:
        """Return the leverage that execution will actually be allowed to use."""
        raw_leverage = 1.0
        if ai_analysis and ai_analysis.recommended_leverage:
            raw_leverage = cls._coerce_risk_float(ai_analysis.recommended_leverage, 1.0, 1.0, 125.0)
        leverage_cap = cls._coerce_risk_float(max_leverage, 125.0, 1.0, 125.0)
        return max(1.0, min(raw_leverage, leverage_cap, 125.0))

    @classmethod
    def _best_fixed_margin_leverage(
        cls,
        notional_value: float,
        fixed_margin: float,
        max_leverage: float | int | None = None,
        preferred_leverage: float | int | None = None,
    ) -> tuple[int, float]:
        """Pick the integer leverage that keeps fixed margin closest after exchange limits.

        Returns (leverage, deviation_pct) where deviation_pct is the margin deviation %.
        """
        leverage_cap = int(cls._coerce_risk_float(max_leverage, 125.0, 1.0, 125.0))
        preferred = int(round(cls._coerce_risk_float(preferred_leverage, 1.0, 1.0, leverage_cap)))

        def _deviation(lev: int) -> float:
            if fixed_margin <= 0:
                return 0.0
            return abs((notional_value / lev) - fixed_margin) / fixed_margin * 100.0

        if notional_value <= 0 or fixed_margin <= 0:
            lev = max(1, min(preferred, leverage_cap))
            return lev, _deviation(lev)

        ideal = notional_value / fixed_margin
        candidate_values = {
            1,
            leverage_cap,
            preferred,
            int(math.floor(ideal)),
            int(round(ideal)),
            int(math.ceil(ideal)),
        }
        candidates = [max(1, min(leverage_cap, value)) for value in candidate_values if value > 0]
        if not candidates:
            lev = max(1, min(preferred, leverage_cap))
            return lev, _deviation(lev)
        best_lev = min(candidates, key=lambda value: (abs((notional_value / value) - fixed_margin), abs(value - preferred)))
        return best_lev, round(_deviation(best_lev), 2)

    @classmethod
    def _resolved_risk_settings(cls, user_settings: dict | None = None) -> dict[str, float | str]:
        risk_cfg = (user_settings or {}).get("risk") or {}
        default_mode = str(getattr(settings.risk, "position_sizing_mode", "percentage") or "percentage").lower().strip()
        if default_mode not in {"percentage", "fixed", "risk_ratio"}:
            default_mode = "percentage"

        sizing_mode = str(risk_cfg.get("position_sizing_mode") or default_mode).lower().strip()
        if sizing_mode not in {"percentage", "fixed", "risk_ratio"}:
            sizing_mode = default_mode

        return {
            "account_equity_usdt": cls._coerce_risk_float(
                risk_cfg.get("account_equity_usdt"),
                cls._coerce_risk_float(getattr(settings.risk, "account_equity_usdt", 10000.0), 10000.0, 100.0, 10_000_000.0),
                100.0,
                10_000_000.0,
            ),
            "max_position_pct": cls._coerce_risk_float(
                risk_cfg.get("max_position_pct"),
                cls._coerce_risk_float(getattr(settings.risk, "max_position_pct", 10.0), 10.0, 0.1, 100.0),
                0.1,
                100.0,
            ),
            "fixed_position_size_usdt": cls._coerce_risk_float(
                risk_cfg.get("fixed_position_size_usdt"),
                cls._coerce_risk_float(getattr(settings.risk, "fixed_position_size_usdt", 100.0), 100.0, 1.0, 1_000_000.0),
                1.0,
                1_000_000.0,
            ),
            "risk_per_trade_pct": cls._coerce_risk_float(
                risk_cfg.get("risk_per_trade_pct"),
                cls._coerce_risk_float(getattr(settings.risk, "risk_per_trade_pct", 1.0), 1.0, 0.1, 100.0),
                0.1,
                100.0,
            ),
            "position_sizing_mode": sizing_mode,
        }

    def _calculate_position_size(
        self,
        price: float,
        size_pct: float,
        leverage: float,
        decision: TradeDecision | None = None,
        user_settings: dict | None = None,
    ) -> float:
        """Calculate position size based on account equity and risk.

        Supports three sizing modes:
        - percentage: AI suggests fraction of max_position_pct
        - fixed: Fixed USDT amount per trade
        - risk_ratio: Risk X% of account per trade (accounts for SL distance)

        NEW: Automatically respects exchange market limits (min/max amount, min/max cost).
        """
        risk_settings = self._resolved_risk_settings(user_settings)
        equity = float(risk_settings["account_equity_usdt"])
        max_position = float(risk_settings["max_position_pct"])
        sizing_mode = risk_settings["position_sizing_mode"]
        leverage = max(1.0, float(leverage or 1.0))

        size_fraction = self._normalize_size_pct(size_pct)

        if sizing_mode == "fixed":
            fixed_amount = float(risk_settings["fixed_position_size_usdt"])
            notional_value = fixed_amount * leverage
            logger.info(
                f"[PositionSize] Fixed mode: margin={fixed_amount}USDT, leverage={leverage}, notional={notional_value}USDT"
            )

        elif sizing_mode == "risk_ratio":
            risk_pct = float(risk_settings["risk_per_trade_pct"])
            sl_distance_pct = 0.0
            if decision and decision.stop_loss and self._has_valid_sl(price, decision.stop_loss):
                sl_distance_pct = self._sl_distance_pct(decision.direction, price, decision.stop_loss)

            if not sl_distance_pct or sl_distance_pct <= 0:
                logger.warning(
                    f"[PositionSize] risk_ratio mode requires valid stop loss, "
                    f"but SL distance is {sl_distance_pct}. Falling back to percentage mode."
                )
                margin_value = equity * (max_position / 100.0) * size_fraction
                notional_value = margin_value * leverage
            else:
                # Risk-based sizing: position size where hitting SL loses exactly risk_amount
                # NOT multiplied by leverage - leverage affects margin, not risk-based size
                risk_amount = equity * (risk_pct / 100.0)
                notional_value = risk_amount / (sl_distance_pct / 100.0)

                # Apply max_position_pct cap to prevent excessive positions
                max_notional = equity * (max_position / 100.0)
                if notional_value > max_notional:
                    original_notional = notional_value
                    notional_value = max_notional
                    logger.warning(
                        f"[PositionSize] risk_ratio exceeded max_position_pct ({max_position}%): "
                        f"calculated={original_notional:.2f}USDT, capped={max_notional:.2f}USDT"
                    )

                logger.info(
                    f"[PositionSize] risk_ratio mode: equity={equity}USDT, risk_pct={risk_pct}%, "
                    f"SL_distance={sl_distance_pct}% -> notional={notional_value}USDT (risk_amount={risk_amount}USDT)"
                )
        else:
            margin_value = equity * (max_position / 100.0) * size_fraction
            notional_value = margin_value * leverage

        notional_value = self._cap_notional_by_stop_risk(notional_value, price, decision, risk_settings)

        # Calculate initial quantity
        if price <= 0:
            return 0.0

        # Get contract size from market limits if available
        contract_size = 1.0
        if decision and decision.ticker:
            try:
                from exchange import get_market_limits

                exchange_config = self._get_exchange_config(user_settings)
                exchange_id = exchange_config.get("exchange") or exchange_config.get("name") or settings.exchange.name
                market_type = exchange_config.get("market_type") or settings.exchange.market_type

                limits = get_market_limits(exchange_id, decision.ticker, market_type)
                if limits and limits.get("contract_size", 1.0) > 1.0:
                    contract_size = float(limits.get("contract_size", 1.0))
            except Exception:
                pass

        # For contract markets, quantity is contract count (notional / price / contractSize)
        # For spot markets, quantity is base currency amount (notional / price)
        if contract_size > 1.0:
            quantity = notional_value / (price * contract_size)
        else:
            quantity = notional_value / price

        # NEW: Apply exchange market limits
        if decision and decision.ticker:
            try:
                from exchange import adjust_quantity_for_limits, get_market_limits

                exchange_config = self._get_exchange_config(user_settings)
                exchange_id = exchange_config.get("exchange") or exchange_config.get("name") or settings.exchange.name
                market_type = exchange_config.get("market_type") or settings.exchange.market_type

                # Get market limits
                limits = get_market_limits(exchange_id, decision.ticker, market_type)

                if limits:
                    # Adjust quantity to respect limits
                    quantity = adjust_quantity_for_limits(quantity, price, limits)

                    # Log the adjustment
                    min_cost = limits.get("min_cost", 0)
                    max_cost = limits.get("max_cost", float("inf"))
                    if min_cost > 0 or max_cost < float("inf"):
                        if contract_size > 1.0:
                            final_cost = quantity * price * contract_size
                            logger.info(
                                f"[PositionSize] Exchange limits applied: "
                                f"contracts={quantity:.6f}, cost={final_cost:.2f}USDT "
                                f"(contractSize={contract_size}, min_cost={min_cost}, max_cost={max_cost})"
                            )
                        else:
                            final_cost = quantity * price
                            logger.info(
                                f"[PositionSize] Exchange limits applied: "
                                f"quantity={quantity:.6f}, cost={final_cost:.2f}USDT "
                                f"(min_cost={min_cost}, max_cost={max_cost})"
                            )
            except Exception as e:
                logger.warning(f"[PositionSize] Could not apply exchange limits: {e}")

        if contract_size > 1.0:
            actual_quantity = quantity * contract_size
            logger.info(
                f"[PositionSize] Contract size adjustment: "
                f"contracts={quantity:.6f}, actual_quantity={actual_quantity:.6f} "
                f"(contractSize={contract_size}, notional={notional_value:.2f}USDT)"
            )

        return float(round(quantity, 6))

    def _cap_notional_by_stop_risk(
        self,
        notional_value: float,
        price: float,
        decision: TradeDecision | None,
        risk_settings: dict[str, float | str],
    ) -> float:
        """Limit notional so the accepted AI SL cannot exceed configured account risk."""
        if not decision or not decision.stop_loss or not self._has_valid_sl(price, decision.stop_loss):
            return notional_value

        sl_distance_pct = self._sl_distance_pct(decision.direction, price, decision.stop_loss)
        if sl_distance_pct <= 0:
            return notional_value

        equity = float(risk_settings["account_equity_usdt"])
        risk_pct = float(risk_settings["risk_per_trade_pct"])
        risk_amount = equity * (risk_pct / 100.0)
        max_notional_by_risk = risk_amount / (sl_distance_pct / 100.0)

        if max_notional_by_risk <= 0 or not math.isfinite(max_notional_by_risk):
            return notional_value

        if notional_value > max_notional_by_risk:
            logger.warning(
                f"[PositionSize] Capping notional by accepted SL risk: "
                f"calculated={notional_value:.2f}USDT, capped={max_notional_by_risk:.2f}USDT, "
                f"SL_distance={sl_distance_pct:.2f}%, risk_pct={risk_pct:.2f}%"
            )
            return max_notional_by_risk

        return notional_value

    def _get_exchange_config(self, user_settings: dict | None = None) -> dict:
        """Get exchange configuration from settings."""
        config = {
            "exchange": settings.exchange.name,
            "market_type": settings.exchange.market_type,
        }
        if user_settings:
            user_exchange = user_settings.get("exchange") or {}
            config.update({
                "exchange": user_exchange.get("name") or user_exchange.get("exchange") or settings.exchange.name,
                "market_type": user_exchange.get("market_type") or settings.exchange.market_type,
            })
        return config

    def _has_valid_sl(self, entry_price: float, stop_loss: float | None = None) -> bool:
        """Check if we have valid stop loss info for risk-based sizing."""
        if not stop_loss or stop_loss <= 0 or entry_price <= 0:
            return False
        return True

    def _sl_distance_pct(self, direction, entry_price: float, stop_loss: float) -> float:
        """Calculate stop loss distance as percentage of entry price."""
        if entry_price <= 0 or stop_loss <= 0:
            return 0.0
        # BUG FIX: Use abs() to ensure we always return a positive distance.
        # A negative distance would invert position sizing calculations.
        if direction and str(direction).lower() == "short":
            return abs((stop_loss - entry_price) / entry_price) * 100.0
        return abs((entry_price - stop_loss) / entry_price) * 100.0

    async def _execute_trade(
        self,
        decision: TradeDecision,
        user_id: str | None,
        user_settings: dict | None = None,
    ) -> dict:
        """Execute the trade on the exchange."""
        exchange_config = {
            "exchange": settings.exchange.name,
            "api_key": settings.exchange.api_key,
            "api_secret": settings.exchange.api_secret,
            "password": settings.exchange.password,
            "live_trading": settings.exchange.live_trading,
            "sandbox_mode": settings.exchange.sandbox_mode,
            "market_type": settings.exchange.market_type,
            "default_order_type": settings.exchange.default_order_type,
            "stop_loss_order_type": settings.exchange.stop_loss_order_type,
            "limit_timeout_overrides": settings.exchange.limit_timeout_overrides,
        }
        if user_id:
            user = await get_user_by_id(self.session, user_id)
            if user:
                if user_settings is None:
                    user_settings = await self._load_user_settings(user_id)

                user_exchange = (user_settings or {}).get("exchange") or {}
                exchange_config.update({
                    "exchange": user_exchange.get("name") or user_exchange.get("exchange") or settings.exchange.name,
                    "api_key": user_exchange.get("api_key") if "api_key" in user_exchange else settings.exchange.api_key,
                    "api_secret": user_exchange.get("api_secret") if "api_secret" in user_exchange else settings.exchange.api_secret,
                    "password": user_exchange.get("password") if "password" in user_exchange else settings.exchange.password,
                    "live_trading": bool(user_exchange.get("live_trading")) if "live_trading" in user_exchange else bool(settings.exchange.live_trading),
                    "sandbox_mode": bool(user_exchange.get("sandbox_mode")) if "sandbox_mode" in user_exchange else bool(settings.exchange.sandbox_mode),
                    "market_type": user_exchange.get("market_type") or settings.exchange.market_type,
                    "default_order_type": user_exchange.get("default_order_type") or settings.exchange.default_order_type,
                    "stop_loss_order_type": user_exchange.get("stop_loss_order_type") or settings.exchange.stop_loss_order_type,
                    "limit_timeout_overrides": (
                        user_exchange.get("limit_timeout_overrides")
                        if "limit_timeout_overrides" in user_exchange
                        else settings.exchange.limit_timeout_overrides
                    ),
                    "max_leverage": user.max_leverage or 20,
                    "max_position_pct": user.max_position_pct or settings.risk.max_position_pct,
                })

                subscription = await get_user_active_subscription(self.session, user_id)
                if exchange_config["live_trading"] and (not user.live_trading_allowed or not subscription):
                    logger.warning(
                        f"[Signal] User {user_id} requested live trading without permission/subscription; using paper mode"
                    )
                    exchange_config["live_trading"] = False
                    # Notify user if subscription just expired
                    if user.live_trading_allowed and not subscription:
                        try:
                            from notifier import notify_subscription_expired
                            await notify_subscription_expired(user_id)
                        except Exception:
                            pass

        self._apply_position_limits(decision, exchange_config, user_settings)
        if not decision.execute:
            raw_result = {"status": "rejected", "reason": decision.reason}
        else:
            control_state = await trading_allowed(
                self.session,
                user_id=user_id,
                live_trading=bool(exchange_config.get("live_trading")),
            )
            if not control_state.get("allowed"):
                reason = control_state.get("block_reason") or "Trading is currently disabled"
                logger.warning(f"[Signal] Trade blocked by control mode: {reason}")
                return {
                    "status": "rejected",
                    "reason": reason,
                    "trading_control": control_state,
                }

            raw_result = await execute_trade(decision, exchange_config)
        result: dict[str, object] = dict(raw_result) if isinstance(raw_result, dict) else {}
        order_status = str(result.get("status", "unknown"))

# Record trade
        signal_data = decision.signal.model_dump() if decision.signal else {}
        risk_cfg = (user_settings or {}).get("risk") or {}
        user_risk_profile = str(risk_cfg.get("ai_risk_profile") or settings.risk.ai_risk_profile)

        trade = await log_trade_db(
            session=self.session,
            user_id=user_id,
            ticker=decision.ticker,
            direction=decision.direction.value if decision.direction else "unknown",
            execute=decision.execute,
            order_status=order_status,
            pnl_pct=0.0,  # Will be updated on close
            payload={
                "signal": signal_data,
                "analysis": decision.ai_analysis.model_dump() if decision.ai_analysis else {},
                "entry_exit_quality": {
                    "entry_source": decision.entry_source,
                    "exit_quality_score": decision.exit_quality_score,
                    "exit_quality_reasons": decision.exit_quality_reasons,
                    "position_size_multiplier": decision.position_size_multiplier,
                },
                "result": result,
                "exchange_config": {
                    "exchange": exchange_config.get("exchange") or exchange_config.get("name"),
                    "live_trading": bool(exchange_config.get("live_trading")),
                    "sandbox_mode": bool(exchange_config.get("sandbox_mode")),
                },
                "strategy_name": signal_data.get("strategy", ""),
                "user_risk_profile": user_risk_profile,
            },
        )
        try:
            trade_payload = json.loads(str(trade.payload_json or "{}"))
        except (TypeError, json.JSONDecodeError):
            trade_payload = {}

        position_id = trade_payload.get("position_id")
        if position_id is not None:
            position_id = str(position_id)

        order_event = await record_order_event(
            session=self.session,
            decision=decision,
            result=result,
            user_id=user_id,
            trade_id=str(trade.id) if trade.id is not None else None,
            position_id=position_id,
        )
        result["order_event_id"] = order_event.id

        # Record metrics
        record_trade(
            decision.ticker,
            decision.direction.value if decision.direction else "unknown",
            order_status,
        )

        # Notify
        await notify_trade_executed(decision, result)

        return result

    async def _load_user_settings(self, user_id: str | None) -> dict:
        """Load decrypted per-user settings once for this webhook.

        Logs at ERROR level if decryption fails so admins are alerted.
        Returns empty dict as fallback — signal processing continues with defaults.
        """
        if not user_id:
            return {}
        user = await get_user_by_id(self.session, user_id)
        if not user:
            return {}
        try:
            raw_settings = json.loads(str(user.settings_json or "{}"))
            settings_data = decrypt_settings_payload(raw_settings)
            return dict(settings_data) if isinstance(settings_data, dict) else {}
        except Exception as exc:
            logger.error(
                f"[Signal] Could not load user settings for user {user_id}: {exc}. "
                f"Signal will process with default settings — verify user configuration."
            )
            return {}

    def _apply_position_limits(
        self,
        decision: TradeDecision,
        exchange_config: dict,
        user_settings: dict | None = None,
    ) -> None:
        """Cap final quantity by the account and user max-position limits."""
        if not decision.entry_price or not decision.quantity or decision.quantity <= 0:
            return
        risk_settings = self._resolved_risk_settings(user_settings)
        sizing_mode = risk_settings.get("position_sizing_mode", "percentage")

        # Get contract size for correct notional calculation
        contract_size = 1.0
        limits = None
        try:
            from exchange import get_market_limits
            exchange_id = exchange_config.get("exchange") or exchange_config.get("name") or settings.exchange.name
            market_type = exchange_config.get("market_type") or settings.exchange.market_type
            limits = get_market_limits(exchange_id, decision.ticker, market_type)
            if limits and limits.get("contract_size", 1.0) > 1.0:
                contract_size = float(limits.get("contract_size", 1.0))
        except Exception:
            pass

        # Fixed mode: ensure quantity matches the configured fixed amount
        # Skip the max_position_pct limit since user explicitly set the amount
        if sizing_mode == "fixed":
            fixed_amount = float(risk_settings.get("fixed_position_size_usdt", 100.0))
            max_leverage = exchange_config.get("max_leverage")
            leverage = self._effective_leverage(decision.ai_analysis, max_leverage)
            expected_notional = fixed_amount * leverage
            # For contract markets: notional = quantity * price * contractSize
            current_notional = decision.quantity * decision.entry_price * contract_size
            if abs(current_notional - expected_notional) > 1.0:
                logger.warning(
                    f"[Signal] Fixed mode: correcting notional from {current_notional:.2f}USDT "
                    f"to {expected_notional:.2f}USDT (margin={fixed_amount}USDT, leverage={leverage}, "
                    f"contractSize={contract_size})"
                )
                decision.quantity = round(expected_notional / (decision.entry_price * contract_size), 6)
                current_notional = decision.quantity * decision.entry_price * contract_size

            if limits:
                try:
                    from exchange import adjust_quantity_for_limits

                    adjusted_quantity = adjust_quantity_for_limits(
                        float(decision.quantity),
                        float(decision.entry_price),
                        limits,
                    )
                    adjusted_notional = adjusted_quantity * float(decision.entry_price) * contract_size
                    selected_leverage, deviation_pct = self._best_fixed_margin_leverage(
                        adjusted_notional,
                        fixed_amount,
                        max_leverage,
                        leverage,
                    )
                except Exception as exc:
                    logger.warning(f"[Signal] Could not verify fixed margin deviation: {exc}")
                    return
            else:
                selected_leverage, deviation_pct = self._best_fixed_margin_leverage(
                    current_notional,
                    fixed_amount,
                    max_leverage,
                    leverage,
                )
                adjusted_quantity = decision.quantity

            previous_leverage = int(round(leverage))
            if selected_leverage != previous_leverage:
                logger.info(
                    f"[Signal] Fixed mode: adjusted leverage {previous_leverage}x -> "
                    f"{selected_leverage}x to keep margin near {fixed_amount:.2f}USDT "
                    f"after exchange limits"
                )
            if decision.ai_analysis:
                decision.ai_analysis.recommended_leverage = selected_leverage
            leverage = float(selected_leverage)

            max_deviation_pct = 20.0
            if deviation_pct > max_deviation_pct:
                actual_margin = (adjusted_quantity * float(decision.entry_price) * contract_size) / max(1.0, leverage)
                decision.execute = False
                decision.reason = (
                    f"Fixed margin deviation too large: "
                    f"configured={fixed_amount:.2f}USDT, actual={actual_margin:.2f}USDT "
                    f"({deviation_pct:.2f}% > {max_deviation_pct:.2f}%)"
                )
                logger.warning(
                    f"[Signal] {decision.reason} "
                    f"(ticker={decision.ticker}, qty={decision.quantity}, adjusted_qty={adjusted_quantity}, "
                    f"leverage={leverage}x, contractSize={contract_size})"
                )
                return

            decision.quantity = round(adjusted_quantity, 6)
            return

        # Percentage/risk_ratio mode: apply max_position_pct limit
        account_equity = float(risk_settings["account_equity_usdt"])
        exchange_cap = self._coerce_risk_float(
            exchange_config.get("max_position_pct"),
            float(risk_settings["max_position_pct"]),
            0.1,
            100.0,
        )
        max_position_pct = min(exchange_cap, float(risk_settings["max_position_pct"]))
        max_leverage = max(1.0, min(float(exchange_config.get("max_leverage") or 125.0), 125.0))
        leverage = self._effective_leverage(decision.ai_analysis, max_leverage)
        max_notional = account_equity * (max_position_pct / 100.0) * leverage
        # For contract markets: max_quantity = max_notional / (price * contract_size)
        max_quantity = max_notional / (float(decision.entry_price) * contract_size)
        if max_quantity > 0 and decision.quantity > max_quantity:
            logger.warning(
                f"[Signal] Quantity capped by max_position_pct: {decision.quantity} -> {max_quantity:.6f}"
            )
            decision.quantity = round(max_quantity, 6)

    async def _check_position_conflict(
        self,
        decision: TradeDecision,
        user_id: str | None,
        user_settings: dict | None = None,
    ) -> tuple[str | None, PositionModel | None]:
        """
        Check for conflicting open positions on the same ticker.
        Returns (rejection_reason, conflicting_position) tuple.

        Checks THREE layers:
        1. Pending orders of opposite direction (cancel them first)
        2. Database tracked OPEN positions (status="open", NOT pending)
        3. Exchange actual positions (for live trading)

        If opposite direction found, returns reason and the position to close.

        FIX: Pending orders should be cancelled before checking open positions.
        """
        try:
            direction = decision.direction.value if decision.direction else ""
            target_key = position_symbol_key(decision.ticker)

            # Step 1: Check for pending orders of opposite direction and cancel them
            pending_stmt = select(PositionModel).where(PositionModel.status == "pending")
            if user_id:
                pending_stmt = pending_stmt.where(PositionModel.user_id == user_id)

            pending_result = await self.session.execute(pending_stmt)
            pending_positions = [
                pos for pos in pending_result.scalars().all()
                if position_symbol_key(pos.ticker) == target_key
            ]

            # Cancel pending orders of opposite direction
            for pending_pos in pending_positions:
                if getattr(pending_pos, "status", None) != "pending":
                    continue
                pending_dir = (pending_pos.direction or "").lower()
                if (direction in ("long", "short") and pending_dir in ("long", "short")
                        and direction != pending_dir):
                    # Cancel this pending order
                    logger.info(
                        f"[Signal] Cancelling pending {pending_dir} order on {decision.ticker} "
                        f"(id={pending_pos.id[:8]}) before opening {direction}"
                    )
                    cancel_result = await self._cancel_pending_position(pending_pos, user_id, user_settings)
                    if cancel_result.get("status") == "error":
                        return (cancel_result.get("reason") or "Failed to cancel conflicting pending order", None)

            # Step 2: Check database OPEN positions (FIX: only status="open")
            stmt = select(PositionModel).where(PositionModel.status == "open")
            if user_id:
                stmt = stmt.where(PositionModel.user_id == user_id)

            result = await self.session.execute(stmt)
            open_positions = [
                pos for pos in result.scalars().all()
                if position_symbol_key(pos.ticker) == target_key
            ]

            direction = decision.direction.value if decision.direction else ""

            # Check database positions for conflict
            for pos in open_positions:
                pos_dir = (pos.direction or "").lower()
                if (direction in ("long", "short") and pos_dir in ("long", "short")
                        and direction != pos_dir):
                    msg = (
                        f"Conflicting position: open {pos_dir} on {decision.ticker} "
                        f"(id={pos.id[:8]}). Closing existing position before opening {direction}."
                    )
                    logger.warning(f"[Signal] Database position conflict detected: {msg}")
                    return (msg, pos)

            # Step 2: Check exchange actual positions (for live trading)
            # This catches positions that might not be in database yet (concurrent signals)
            # FIX: Use settings.exchange.live_trading as fallback, not hardcoded False
            live_trading = bool(settings.exchange.live_trading)
            if user_settings:
                exchange_cfg = (user_settings or {}).get("exchange") or {}
                user_live = exchange_cfg.get("live_trading")
                if user_live is not None:
                    live_trading = bool(user_live)

            if live_trading:
                from exchange import get_open_positions
                exchange_config = self._build_exchange_config(user_id, user_settings)
                try:
                    exchange_positions = await get_open_positions(exchange_config)
                    for ex_pos in exchange_positions:
                        ex_symbol = position_symbol_key(ex_pos.get("symbol") or "")
                        if ex_symbol != target_key:
                            continue
                        ex_side = str(ex_pos.get("side") or "").lower()
                        excontracts = safe_float(ex_pos.get("contracts") or ex_pos.get("contractSize") or 0)
                        if excontracts <= 0:
                            continue

                        # Map exchange side to position direction
                        ex_dir = "long" if ex_side in ("long", "buy") else "short"
                        if (direction in ("long", "short") and ex_dir in ("long", "short")
                                and direction != ex_dir):
                            msg = (
                                f"Exchange position conflict: {ex_dir} {excontracts} contracts on {decision.ticker}. "
                                f"Closing exchange position before opening {direction}."
                            )
                            logger.warning(f"[Signal] Exchange position conflict detected: {msg}")

                            # Create a synthetic position model for closing
                            # Use the first database position if exists, or create synthetic
                            for pos in open_positions:
                                if (pos.direction or "").lower() == ex_dir:
                                    return (msg, pos)

                            # No database position found, create synthetic position for closing
                            synthetic_pos = PositionModel(
                                id="exchange-sync",
                                ticker=decision.ticker,
                                direction=ex_dir,
                                quantity=excontracts,
                                entry_price=safe_float(ex_pos.get("entryPrice") or ex_pos.get("entry_price") or 0),
                                status="open",
                                live_trading=True,
                            )
                            return (msg, synthetic_pos)
                except Exception as ex:
                    if self._block_live_risk_check_errors(user_settings):
                        msg = f"Exchange position conflict check failed in live mode: {ex}"
                        logger.error(f"[Signal] {msg}")
                        return (msg, None)
                    logger.warning(f"[Signal] Failed to check exchange positions: {ex}")

            # Allow same-direction (scaling in)
            return (None, None)
        except Exception as e:
            logger.warning(f"[Signal] Position conflict check failed (allowing trade): {e}")
            return (None, None)

    async def _close_conflicting_position(
        self,
        position: PositionModel,
        user_id: str | None,
        user_settings: dict | None = None,
    ) -> dict:
        """
        Close an existing position and cancel its TP/SL orders.
        Used for reverse signal handling (close opposite position before opening new).

        Handles both:
        - Database tracked positions (with proper ID)
        - Synthetic positions from exchange (id="exchange-sync")
        """
        is_synthetic = position.id == "exchange-sync"
        result = {
            "status": "unknown",
            "ticker": position.ticker,
            "position_id": position.id[:8] if len(position.id) >= 8 else position.id,
            "is_synthetic": is_synthetic,
        }

        # Build exchange config
        exchange_config = self._build_exchange_config(user_id, user_settings)
        exchange_config["live_trading"] = position.live_trading

        try:
            cancel_results = []
            sl_cancel_result = None

            # Step 1: Close position on exchange first. Do not remove protection while the position may still be open.
            exit_price = float(position.entry_price or 0)
            if position.live_trading and exchange_config.get("live_trading"):
                from exchange import get_ticker
                ticker_data = await get_ticker(position.ticker, exchange_config)
                exit_price = safe_float(ticker_data.get("last") or position.last_price or position.entry_price)

                # Build decision to close position
                close_qty = float(position.remaining_quantity or position.quantity or 0)
                if close_qty <= 0:
                    close_qty = float(position.quantity or 0)

                close_decision = TradeDecision(
                    ticker=position.ticker,
                    direction=SignalDirection.CLOSE_LONG if str(position.direction).lower() == "long" else SignalDirection.CLOSE_SHORT,
                    quantity=close_qty,
                    execute=True,
                )
                close_result = await execute_trade(close_decision, exchange_config)
                if close_result.get("status") == "closed":
                    exit_price = safe_float(close_result.get("exit_price") or exit_price)
                    result["exchange_close"] = close_result
                elif close_result.get("status") == "no_position":
                    logger.warning(
                        f"[Signal] Exchange returned no_position while DB still tracks {position.ticker}. "
                        "Keeping DB position open and preserving TP/SL until monitor confirms flat."
                    )
                    result["exchange_close_error"] = close_result
                    result["status"] = "error"
                    result["reason"] = "Exchange close not confirmed: no_position while DB position is open"
                    return result
                else:
                    logger.warning(f"[Signal] Failed to close position on exchange: {close_result}")
                    result["exchange_close_error"] = close_result
                    result["status"] = "error"
                    result["reason"] = f"Failed to close on exchange: {close_result.get('reason')}"
                    return result

            # Step 2: Record close in database (only for non-synthetic positions)
            if not is_synthetic and exit_price > 0:
                try:
                    locked_result = await self.session.execute(
                        select(PositionModel)
                        .where(PositionModel.id == position.id)
                        .with_for_update()
                    )
                    locked_position = locked_result.scalar_one_or_none()
                    if locked_position and locked_position.status == "open":
                        await close_position_async(
                            session=self.session,
                            position=locked_position,
                            exit_price=exit_price,
                            close_reason="reverse_signal",
                        )
                        await self.session.flush()
                    elif locked_position and locked_position.status != "open":
                        logger.info(f"[Signal] Position {position.id[:8]} already closed by concurrent operation")
                except Exception as db_err:
                    logger.warning(f"[Signal] Failed to update database position: {db_err}")

            # Step 3: The position is closed or already absent; now clean up any leftover TP/SL orders.
            if not is_synthetic and hasattr(position, "take_profit_order_ids_json"):
                tp_order_ids = loads_list(position.take_profit_order_ids_json)
                for order_id in tp_order_ids:
                    if order_id:
                        cancel_result = await cancel_order(str(order_id), position.ticker, exchange_config)
                        cancel_results.append(cancel_result)

            if not is_synthetic and hasattr(position, "stop_loss_order_id") and position.stop_loss_order_id:
                sl_cancel_result = await cancel_order(str(position.stop_loss_order_id), position.ticker, exchange_config)

            result["status"] = "closed"
            result["exit_price"] = exit_price
            result["cancelled_tp_orders"] = len([r for r in cancel_results if r.get("status") in ("cancelled", "simulated")])
            if sl_cancel_result:
                result["stop_loss_cancel"] = sl_cancel_result
            logger.info(
                f"[Signal] ✅ Closed conflicting position {position.id[:8] if len(position.id) >= 8 else position.id} "
                f"on {position.ticker} (exit={exit_price}, synthetic={is_synthetic})"
            )

        except Exception as e:
            logger.error(f"[Signal] Failed to close conflicting position: {e}")
            result["status"] = "error"
            result["reason"] = str(e)

        return result

    async def _cancel_pending_position(
        self,
        position: PositionModel,
        user_id: str | None,
        user_settings: dict | None = None,
    ) -> dict:
        """
        Cancel a pending position (limit order not yet filled).

        Used for reverse signal handling - cancel pending orders of opposite direction
        before opening new position.
        """
        result = {
            "status": "unknown",
            "ticker": position.ticker,
            "position_id": position.id[:8] if len(position.id) >= 8 else position.id,
        }

        try:
            # Build exchange config
            exchange_config = self._build_exchange_config(user_id, user_settings)
            exchange_config["live_trading"] = position.live_trading

            # Step 1: Cancel limit entry order on exchange
            if position.live_trading and position.entry_order_id:
                from exchange import cancel_order
                cancel_result = await cancel_order(str(position.entry_order_id), position.ticker, exchange_config)
                result["exchange_cancel"] = cancel_result

                if cancel_result.get("status") not in ("cancelled", "simulated"):
                    logger.warning(f"[Signal] Failed to cancel pending order on exchange: {cancel_result}")
                    result["status"] = "error"
                    result["reason"] = f"Exchange cancellation failed: {cancel_result.get('reason', 'unknown')}"
                    return result

            # Step 2: Cancel TP orders if any
            if hasattr(position, "take_profit_order_ids_json") and position.take_profit_order_ids_json:
                from exchange import cancel_order
                tp_order_ids = loads_list(position.take_profit_order_ids_json)
                for order_id in tp_order_ids:
                    if order_id:
                        await cancel_order(str(order_id), position.ticker, exchange_config)

            # Step 3: Cancel SL order if any
            if hasattr(position, "stop_loss_order_id") and position.stop_loss_order_id:
                from exchange import cancel_order
                await cancel_order(str(position.stop_loss_order_id), position.ticker, exchange_config)

            # Step 4: Mark position as cancelled in database
            position.status = "cancelled"
            position.closed_at = datetime.now(timezone.utc)
            position.close_reason = "cancelled_reverse_signal"
            await self.session.flush()

            result["status"] = "cancelled"
            logger.info(
                f"[Signal] ✅ Cancelled pending position {position.id[:8]} "
                f"on {position.ticker}"
            )

        except Exception as e:
            logger.error(f"[Signal] Failed to cancel pending position: {e}")
            result["status"] = "error"
            result["reason"] = str(e)
            return result

        return result

    def _build_exchange_config(self, user_id: str | None, user_settings: dict | None = None) -> dict:
        """Build exchange configuration from user settings or defaults."""
        exchange_config = {
            "exchange": settings.exchange.name,
            "api_key": settings.exchange.api_key,
            "api_secret": settings.exchange.api_secret,
            "password": settings.exchange.password,
            "live_trading": settings.exchange.live_trading,
            "sandbox_mode": settings.exchange.sandbox_mode,
            "market_type": settings.exchange.market_type,
            "default_order_type": settings.exchange.default_order_type,
            "stop_loss_order_type": settings.exchange.stop_loss_order_type,
            "limit_timeout_overrides": settings.exchange.limit_timeout_overrides,
            "margin_mode": settings.risk.margin_mode,
        }

        if user_id and user_settings:
            user_exchange = (user_settings or {}).get("exchange") or {}
            user_live = user_exchange.get("live_trading")
            user_sandbox = user_exchange.get("sandbox_mode")
            exchange_config.update({
                "exchange": user_exchange.get("name") or user_exchange.get("exchange") or settings.exchange.name,
                "api_key": user_exchange.get("api_key") if "api_key" in user_exchange else settings.exchange.api_key,
                "api_secret": user_exchange.get("api_secret") if "api_secret" in user_exchange else settings.exchange.api_secret,
                "password": user_exchange.get("password") if "password" in user_exchange else settings.exchange.password,
                "live_trading": bool(user_live if user_live is not None else settings.exchange.live_trading),
                "sandbox_mode": bool(user_sandbox if user_sandbox is not None else settings.exchange.sandbox_mode),
                "market_type": user_exchange.get("market_type") or settings.exchange.market_type,
                "default_order_type": user_exchange.get("default_order_type") or settings.exchange.default_order_type,
                "stop_loss_order_type": user_exchange.get("stop_loss_order_type") or settings.exchange.stop_loss_order_type,
                "margin_mode": user_exchange.get("margin_mode") or settings.risk.margin_mode,
            })

        return exchange_config

    async def _record_and_notify_blocked(
        self,
        reservation,
        signal: TradingViewSignal,
        fingerprint: str,
        user_id: str | None,
        client_ip: str,
        reason: str,
        raw_body: dict | None = None,
    ):
        """Record and notify about blocked signal."""
        await notify_pre_filter_blocked(signal.ticker, signal.direction.value, reason)

        self._update_reserved_event(
            reservation,
            status="blocked",
            status_code=200,
            reason=reason,
            payload=raw_body or signal.model_dump(),
        )
