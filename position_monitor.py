"""
Signal Server - Position Monitor
Tracks open positions, settles paper TP/SL, reconciles exchange closes,
and keeps realised PnL in the database.
P0-FIX: Dynamic ghost position threshold based on position value
P1-FIX: Re-evaluate trailing_stop config when limit order fills (CRITICAL BUG FIX)
P2-FIX: Verify and re-place TP/SL orders periodically to protect against exchange-side cancellations
"""
import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import ccxt
from loguru import logger
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import settings
from core.database import (
    PositionModel,
    UserModel,
    close_position_async,
    db_manager,
    record_position_close_trade_async,
)
from core.security import decrypt_settings_payload
from core.utils.common import (
    first_valid,
    loads_dict,
    loads_list,
    normalize_limit_timeout_overrides,
    position_symbol_key,
    price_pnl_pct,
    safe_bool,
    safe_float,
    suggested_limit_timeout_secs,
)
from core.utils.datetime import utcnow

_safe_float = safe_float
_loads_list = loads_list
_loads_dict = loads_dict


def _resolve_trailing_mode(trailing_config: dict, position: "PositionModel" = None) -> str:
    """Resolve 'auto' trailing-stop mode to a concrete mode.

    When the stored mode is 'auto' (or empty while global setting is 'auto'),
    use the AI metadata stored alongside the config to re-resolve via
    ``select_smart_trailing_stop``.  If metadata is missing, fall back to
    ``step_trailing`` as a safe default.
    """
    raw_mode = str(trailing_config.get("mode") or "").lower()

    if raw_mode and raw_mode not in {"auto", "", "none"}:
        return raw_mode

    global_mode = str(settings.trailing_stop.mode or "none").lower()

    if raw_mode != "auto" and global_mode != "auto":
        return raw_mode if raw_mode else global_mode

    from smart_trailing_stop import select_smart_trailing_stop

    confidence = safe_float(trailing_config.get("_ai_confidence"), 0.65)
    risk_score = safe_float(trailing_config.get("_ai_risk_score"), 0.5)
    market_condition = str(trailing_config.get("_ai_market_condition") or "unknown").lower()
    trend_strength = str(trailing_config.get("_ai_trend_strength") or "moderate").lower()
    timeframe = str(trailing_config.get("_signal_timeframe") or "60")
    atr_pct = safe_float(trailing_config.get("_atr_pct_at_fill"), None)

    tp_levels = []
    if position is not None:
        tp_levels = loads_list(position.take_profit_json)
    num_tp_levels = len(tp_levels) if tp_levels else 4

    try:
        decision = select_smart_trailing_stop(
            confidence=confidence,
            market_condition=market_condition,
            trend_strength=trend_strength,
            risk_score=risk_score,
            timeframe=timeframe,
            num_tp_levels=num_tp_levels,
            atr_pct=atr_pct,
            user_override=None,
        )
        resolved = decision.mode.value
        logger.info(
            f"[TrailingStop] Resolved 'auto' -> '{resolved}' for "
            f"confidence={confidence:.2f} market={market_condition} "
            f"trend={trend_strength} (reason: {decision.reasoning})"
        )
        return resolved
    except Exception as e:
        logger.warning(f"[TrailingStop] Failed to resolve 'auto' mode: {e}. Falling back to 'step_trailing'")
        return "step_trailing"


_position_monitor_lock = asyncio.Lock()
_GHOST_POSITION_TRACKER: dict[str, dict[str, Any]] = {}
_PROTECTIVE_ORDERS_LAST_VERIFY: dict[str, datetime] = {}
_PROTECTIVE_ORDERS_VERIFY_INTERVAL = 600  # seconds (10 min) between TP/SL existence checks

# P2-14: Per-position reconcile locks to prevent concurrent TP/SL processing
_position_reconcile_locks: dict[str, asyncio.Lock] = {}
_position_reconcile_locks_guard = asyncio.Lock()

# P0-FIX: Dynamic ghost position thresholds based on position value
# Higher-value positions need more confirmation attempts before auto-close
_GHOST_THRESHOLD_SMALL_POSITION = 5   # < $100
_GHOST_THRESHOLD_MEDIUM_POSITION = 8   # $100 - $1000
_GHOST_THRESHOLD_LARGE_POSITION = 12   # > $1000
_GHOST_THRESHOLD_HUGE_POSITION = 15    # > $10,000
_MAX_GHOST_THRESHOLD = _GHOST_THRESHOLD_HUGE_POSITION  # Backward compatibility alias
_GHOST_CHECK_INTERVAL_SECS = 3600
_GHOST_MIN_ELAPSED_SECS = 900  # minimum elapsed before ghost-close (15 min, protects against API instability)
_GHOST_TRACKER_FILE = Path("data") / "ghost_position_tracker.json"
_CLOSED_POSITION_RECOVERY_LOOKBACK_HOURS = 24


def _save_ghost_tracker() -> None:
    """Persist ghost tracker to disk for restart survival."""
    if not _GHOST_TRACKER_FILE:
        return
    try:
        _GHOST_TRACKER_FILE.parent.mkdir(parents=True, exist_ok=True)
        serializable = {}
        for pos_id, entry in _GHOST_POSITION_TRACKER.items():
            first_missing = entry.get("first_missing_at")
            serializable[pos_id] = {
                "fail_count": entry.get("fail_count", 0),
                "first_missing_at": first_missing.isoformat() if first_missing else None,
            }
        _GHOST_TRACKER_FILE.write_text(json.dumps(serializable))
    except Exception:
        pass


def _load_ghost_tracker() -> None:
    """Restore ghost tracker from disk after restart."""
    if not _GHOST_TRACKER_FILE or not _GHOST_TRACKER_FILE.exists():
        return
    try:
        data = json.loads(_GHOST_TRACKER_FILE.read_text())
        for pos_id, entry in data.items():
            first_missing = entry.get("first_missing_at")
            if first_missing:
                try:
                    first_missing = datetime.fromisoformat(first_missing)
                except (ValueError, TypeError):
                    first_missing = None
            _GHOST_POSITION_TRACKER[pos_id] = {
                "fail_count": entry.get("fail_count", 0),
                "first_missing_at": first_missing or utcnow(),
                "last_check": utcnow(),
            }
        logger.info(f"[PositionMonitor] Restored ghost tracker: {len(data)} entries")
    except Exception as e:
        logger.warning(f"[PositionMonitor] Failed to restore ghost tracker: {e}")
_POSITION_VALUE_THRESHOLDS = [100.0, 1000.0, 10000.0]  # USDT thresholds for dynamic thresholds


def _calculate_ghost_threshold(position: PositionModel) -> int:
    """P0-FIX: Calculate dynamic ghost position threshold based on position value.

    Larger positions need more confirmation attempts before auto-close to prevent
    premature closure of significant positions during temporary API/network issues.

    Args:
        position: PositionModel instance

    Returns:
        int: Dynamic threshold (3-10 based on position value)
    """
    entry_price = safe_float(position.entry_price, 0.0)
    quantity = safe_float(position.quantity, 0.0)
    leverage = max(1.0, safe_float(position.leverage, 1.0))
    # Use position.margin as primary source for actual margin (updated on fills)
    # Fallback to calculated value only if margin is not available
    stored_margin = safe_float(position.margin, 0.0)
    if stored_margin > 0:
        position_value = stored_margin * leverage
    else:
        # Fallback: recalculate from entry_price, quantity, and stored contract_size
        ts_config = loads_dict(position.trailing_stop_config_json)
        contract_size = safe_float(ts_config.get("_contract_size"), 1.0)
        position_value = (entry_price * quantity * contract_size) if entry_price > 0 and quantity > 0 else 0.0

    # Dynamic thresholds based on position value
    if position_value < _POSITION_VALUE_THRESHOLDS[0]:  # < $100
        threshold = _GHOST_THRESHOLD_SMALL_POSITION
    elif position_value < _POSITION_VALUE_THRESHOLDS[1]:  # $100 - $1000
        threshold = _GHOST_THRESHOLD_MEDIUM_POSITION
    elif position_value < _POSITION_VALUE_THRESHOLDS[2]:  # $1000 - $10,000
        threshold = _GHOST_THRESHOLD_LARGE_POSITION
    else:  # > $10,000
        threshold = _GHOST_THRESHOLD_HUGE_POSITION

    logger.debug(
        f"[P0-FIX] Ghost threshold for {position.ticker}: "
        f"value=${position_value:.2f}, threshold={threshold} attempts"
    )

    return threshold


def _position_contract_size(position: PositionModel) -> float:
    """Return stored contract multiplier for margin/PnL calculations."""
    ts_config = loads_dict(position.trailing_stop_config_json)
    return max(0.0, safe_float(ts_config.get("_contract_size"), 1.0))


def _filled_margin_from_order(
    position: PositionModel,
    filled_cost: float,
    filled_amount: float,
    filled_price: float,
) -> float:
    """Calculate margin for a filled entry order.

    Exchange-reported cost already represents notional. If it is missing, rebuild
    notional from amount, price, and the stored contract multiplier.
    """
    leverage = safe_float(position.leverage, 1.0)
    if leverage <= 0:
        return safe_float(position.margin, 0.0)
    if filled_cost > 0:
        return filled_cost / leverage
    if filled_amount > 0 and filled_price > 0:
        contract_size = _position_contract_size(position) or 1.0
        return (filled_amount * filled_price * contract_size) / leverage
    return safe_float(position.margin, 0.0)


async def _fetch_market_price_changes(symbol: str, exchange_config: dict, current_price: float) -> dict:
    """Fetch OHLCV data to calculate price_change_1h and price_change_24h.

    The get_ticker() function does NOT return price_change_1h/24h, so we
    fetch OHLCV candles directly and compute the changes ourselves.
    """
    from exchange import _get_or_create_exchange, _resolve_symbol

    exchange = _get_or_create_exchange(
        exchange_id=exchange_config.get("exchange", settings.exchange.name),
        api_key=exchange_config.get("api_key", settings.exchange.api_key),
        api_secret=exchange_config.get("api_secret", settings.exchange.api_secret),
        password=exchange_config.get("password", settings.exchange.password),
        live=bool(exchange_config.get("live_trading", False)),
        sandbox=bool(exchange_config.get("sandbox_mode", False)),
        market_type=exchange_config.get("market_type", settings.exchange.market_type),
    )

    try:
        resolved_symbol = await asyncio.to_thread(
            _resolve_symbol,
            exchange,
            symbol,
            exchange_config.get("market_type", settings.exchange.market_type),
        )

        ohlcv_1h = await asyncio.to_thread(exchange.fetch_ohlcv, resolved_symbol, "1h", None, 25)
        ohlcv_4h = await asyncio.to_thread(exchange.fetch_ohlcv, resolved_symbol, "4h", None, 8)

        price_change_1h = 0.0
        price_change_24h = 0.0

        if len(ohlcv_1h) >= 2:
            price_1h_ago = ohlcv_1h[-2][4]
            if price_1h_ago > 0:
                price_change_1h = ((current_price - price_1h_ago) / price_1h_ago) * 100

        if len(ohlcv_4h) >= 7:
            price_24h_ago = ohlcv_4h[-7][4]
            if price_24h_ago > 0:
                price_change_24h = ((current_price - price_24h_ago) / price_24h_ago) * 100
        elif len(ohlcv_1h) >= 24:
            price_24h_ago = ohlcv_1h[-24][4]
            if price_24h_ago > 0:
                price_change_24h = ((current_price - price_24h_ago) / price_24h_ago) * 100

        return {
            "price_change_1h": price_change_1h,
            "price_change_24h": price_change_24h,
        }
    except Exception as e:
        logger.warning(f"[P1-FIX] Failed to fetch OHLCV for price changes: {e}")
        return {"price_change_1h": 0.0, "price_change_24h": 0.0}


async def _reevaluate_trailing_stop_config(
    session: AsyncSession,
    position: PositionModel,
    exchange_config: dict,
    entry_price: float,
    current_price: float,
) -> dict:
    """P1-FIX: Re-evaluate trailing_stop config when limit order fills.

    CRITICAL BUG FIX:
    - Limit orders may wait hours before filling
    - Market conditions at fill time ≠ market conditions at signal time
    - Re-evaluate trailing_stop mode based on current market conditions

    Args:
        session: Database session
        position: PositionModel instance
        exchange_config: Exchange configuration
        entry_price: Filled entry price
        current_price: Current market price

    Returns:
        dict: Updated trailing_stop config
    """
    from smart_trailing_stop import select_smart_trailing_stop

    trailing_config = loads_dict(position.trailing_stop_config_json)
    user_mode = str(trailing_config.get("mode") or "").lower()

    if user_mode and user_mode not in {"auto", "", "none"}:
        logger.info(
            f"[P1-FIX] Limit order filled: {position.ticker} - "
            f"user trailing_stop mode '{user_mode}' preserved (not re-evaluating)"
        )
        return trailing_config

    try:
        market_changes = await _fetch_market_price_changes(position.ticker, exchange_config, current_price)
        price_change_1h = market_changes["price_change_1h"]
        price_change_24h = market_changes["price_change_24h"]

        atr_pct = abs(price_change_24h)

        if price_change_24h > 10.0:
            market_condition = "trending_up"
        elif price_change_24h < -10.0:
            market_condition = "trending_down"
        elif abs(price_change_1h) > 3.0:
            market_condition = "volatile"
        elif atr_pct < 1.0:
            market_condition = "calm"
        else:
            market_condition = "ranging"

        if abs(price_change_24h) > 15.0:
            trend_strength = "strong"
        elif abs(price_change_24h) > 5.0:
            trend_strength = "moderate"
        elif abs(price_change_24h) > 2.0:
            trend_strength = "weak"
        else:
            trend_strength = "none"

        confidence = safe_float(trailing_config.get("_ai_confidence") or 0.65)
        risk_score = safe_float(trailing_config.get("_ai_risk_score") or 0.5)

        timeframe = str(trailing_config.get("_signal_timeframe") or "60")

        tp_levels = loads_list(position.take_profit_json)
        num_tp_levels = len(tp_levels) if tp_levels else 4

        decision = select_smart_trailing_stop(
            confidence=confidence,
            market_condition=market_condition,
            trend_strength=trend_strength,
            risk_score=risk_score,
            timeframe=timeframe,
            num_tp_levels=num_tp_levels,
            atr_pct=atr_pct,
            user_override=None,
        )

        new_config = {
            "mode": decision.mode.value,
            "_reevaluated_at_fill": True,
            "_reasoning": decision.reasoning,
            "_market_condition_at_fill": market_condition,
            "_trend_strength_at_fill": trend_strength,
            "_atr_pct_at_fill": atr_pct,
        }

        for key, value in trailing_config.items():
            if key not in {"mode", "_reevaluated_at_fill", "_reasoning", "_market_condition", "_trend_strength", "_market_condition_at_fill", "_trend_strength_at_fill"}:
                new_config[key] = value

        old_mode = trailing_config.get("mode", "none")
        new_mode = decision.mode.value

        if old_mode != new_mode:
            logger.info(
                f"[P1-FIX] Limit order filled - trailing_stop re-evaluated: "
                f"{position.ticker} {position.direction} "
                f"mode '{old_mode}' -> '{new_mode}' "
                f"(market: {market_condition}, trend: {trend_strength}, ATR: {atr_pct:.1f}%)"
            )
            logger.info(
                f"[P1-FIX] Reason: {decision.reasoning}"
            )
        else:
            logger.info(
                f"[P1-FIX] Limit order filled - trailing_stop unchanged: "
                f"{position.ticker} mode '{new_mode}' "
                f"(market condition still suitable)"
            )

        return new_config

    except Exception as e:
        logger.warning(
            f"[P1-FIX] Failed to re-evaluate trailing_stop for {position.ticker}: {e}. "
            f"Keeping original config."
        )
        return trailing_config


def _position_limit_timeout_secs(position: PositionModel) -> float:
    configured = safe_float(getattr(position, "limit_timeout_secs", 0), 0.0)
    if configured > 0:
        return configured
    return float(suggested_limit_timeout_secs("1h"))


def _paper_trailing_stop_price(position: PositionModel, mark_price: float) -> float | None:
    trailing_config = loads_dict(position.trailing_stop_config_json)
    trailing_mode = _resolve_trailing_mode(trailing_config, position)
    if trailing_mode not in {"moving", "profit_pct_trailing"}:
        return None

    direction = str(position.direction or "long").lower()
    entry_price = safe_float(position.entry_price)
    leverage = max(1.0, safe_float(position.leverage, 1.0))
    if mark_price <= 0 or entry_price <= 0:
        return None

    activation_pct = safe_float(
        first_valid(trailing_config.get("activation_profit_pct"), settings.trailing_stop.activation_profit_pct),
        1.0,
    )
    trail_pct = safe_float(first_valid(trailing_config.get("trail_pct"), settings.trailing_stop.trail_pct), 1.0)
    profit_pct = _price_pnl_pct(direction, entry_price, mark_price, leverage)
    if profit_pct < activation_pct:
        return None

    if direction == "short":
        return mark_price * (1 + trail_pct / 100.0)
    return mark_price * (1 - trail_pct / 100.0)


def _has_partial_position_fills(position: PositionModel) -> bool:
    return any(
        str(level.get("status") or "").lower() in {"hit", "filled", "closed"}
        for level in loads_list(position.take_profit_json)
        if isinstance(level, dict)
    )


def _effective_remaining_quantity(position: PositionModel, opened_qty: float) -> float:
    remaining_qty = safe_float(position.remaining_quantity, opened_qty)
    if remaining_qty > 0:
        return remaining_qty
    if (
        position.status in {"open", "pending"}
        and safe_float(position.realized_pnl_pct) == 0
        and not _has_partial_position_fills(position)
    ):
        return opened_qty
    # If partial fills exist but remaining_quantity is 0, calculate from TP levels
    if _has_partial_position_fills(position) and opened_qty > 0:
        tp_levels = loads_list(position.take_profit_json)
        filled_qty = sum(
            opened_qty * (safe_float(level.get("qty_pct"), 0) / 100.0)
            for level in tp_levels
            if isinstance(level, dict) and str(level.get("status") or "").lower() in {"hit", "filled", "closed"}
        )
        calculated_remaining = max(0.0, opened_qty - filled_qty)
        if calculated_remaining > 0:
            return calculated_remaining
    return 0.0


def _symbol_key(symbol: str) -> str:
    return position_symbol_key(symbol)


def _price_pnl_pct(direction: str, entry_price: float, exit_price: float, leverage: float = 1.0) -> float:
    return price_pnl_pct(direction, entry_price, exit_price, leverage)


def _get_exchange_config_for_position(position: PositionModel) -> dict | None:
    if not position.user_id:
        return None
    exchange_name = str(position.exchange or "").lower()
    if not exchange_name:
        return None
    return {
        "exchange": exchange_name,
        "user_id": position.user_id,
        "sandbox_mode": position.sandbox_mode,
        "live_trading": position.live_trading,
        "limit_timeout_overrides": normalize_limit_timeout_overrides(getattr(settings.exchange, "limit_timeout_overrides", {})),
    }


async def get_monitor_state() -> dict:
    """Get position monitor state."""
    raw_mode = str(settings.trailing_stop.mode or "none").lower()
    display_mode = raw_mode if raw_mode != "auto" else f"auto -> {_resolve_trailing_mode({})}"
    return {
        "enabled": True,
        "position_tracking_enabled": True,
        "trailing_stop_enabled": raw_mode != "none",
        "interval_secs": settings.position_monitor_interval_secs,
        "mode": display_mode,
    }


def _exchange_position_contracts(exchange_pos: dict | None) -> float:
    if not exchange_pos:
        return 0.0
    return abs(safe_float(exchange_pos.get("contracts") or 0))


def _exchange_position_side_matches_position(position: PositionModel, exchange_pos: dict | None) -> bool:
    if not exchange_pos:
        return False
    exchange_side = str(exchange_pos.get("side") or "").lower().strip()
    direction = str(position.direction or "").lower().strip()
    if exchange_side in {"buy", "long"}:
        exchange_side = "long"
    elif exchange_side in {"sell", "short"}:
        exchange_side = "short"
    if direction in {"buy", "long"}:
        direction = "long"
    elif direction in {"sell", "short"}:
        direction = "short"
    return not exchange_side or not direction or exchange_side == direction


def _sync_open_position_from_exchange(position: PositionModel, exchange_pos: dict) -> float:
    contracts = _exchange_position_contracts(exchange_pos)
    if contracts <= 0:
        return 0.0

    if safe_float(position.quantity) <= 0 or contracts > safe_float(position.quantity):
        position.quantity = contracts
    position.remaining_quantity = contracts
    position.status = "open"
    position.close_reason = None
    position.closed_at = None
    position.close_trade_id = None
    position.pnl_pct = None
    position.exit_price = None

    entry_price = safe_float(exchange_pos.get("entry_price") or exchange_pos.get("entryPrice"))
    if entry_price > 0:
        position.entry_price = entry_price

    mark_price = safe_float(
        exchange_pos.get("mark_price")
        or exchange_pos.get("markPrice")
        or exchange_pos.get("entry_price")
        or exchange_pos.get("entryPrice")
    )
    if mark_price > 0:
        position.last_price = mark_price
        if safe_float(position.entry_price) > 0:
            _update_unrealized(position, mark_price)

    position.updated_at = utcnow()
    return contracts


async def _has_active_duplicate_position(session: AsyncSession, position: PositionModel) -> bool:
    result = await session.execute(
        select(PositionModel).where(
            PositionModel.status.in_(["open", "pending"]),
            PositionModel.id != position.id,
            PositionModel.direction == position.direction,
        )
    )
    target_key = position_symbol_key(position.ticker)
    return any(position_symbol_key(row.ticker) == target_key for row in result.scalars().all())


async def _close_db_position_without_exchange_exposure(
    session: AsyncSession,
    position: PositionModel,
    close_reason: str,
) -> None:
    """Close a DB-only position after targeted exchange verification found no exposure."""
    now = utcnow()
    position.status = "closed"
    position.close_reason = close_reason
    position.closed_at = now
    position.updated_at = now
    position.remaining_quantity = 0.0
    position.unrealized_pnl_usdt = 0.0
    fallback_price = safe_float(position.last_price or position.entry_price)
    if fallback_price > 0:
        position.exit_price = fallback_price
    position.current_pnl_pct = safe_float(position.realized_pnl_pct)
    position.pnl_pct = safe_float(position.realized_pnl_pct)
    _GHOST_POSITION_TRACKER.pop(str(position.id), None)
    _save_ghost_tracker()
    await session.flush()


async def _close_missing_entry_order_without_exposure(
    session: AsyncSession,
    position: PositionModel,
    exchange_config: dict,
) -> bool:
    """Close a stale DB entry when its entry order and exchange exposure are both gone."""
    if not position.entry_order_id:
        return False

    try:
        from exchange import fetch_single_position

        exchange_pos = await fetch_single_position(
            position.ticker,
            {**exchange_config, "raise_on_error": True},
        )
    except Exception as exc:
        logger.warning(
            f"[PositionMonitor] Entry order {position.entry_order_id} not found for {position.ticker}, "
            f"but single-position verification failed: {exc}. Leaving DB state unchanged."
        )
        return False

    if (
        exchange_pos is not None
        and _exchange_position_side_matches_position(position, exchange_pos)
        and _exchange_position_contracts(exchange_pos) > 0
    ):
        contracts = _sync_open_position_from_exchange(position, exchange_pos)
        position.status = "open"
        position.close_reason = ""
        position.closed_at = None
        logger.warning(
            f"[PositionMonitor] Entry order {position.entry_order_id} not found for {position.ticker}, "
            f"but exchange still reports {contracts} contracts. Synced DB position instead of closing."
        )
        await session.flush()
        return False

    await _close_db_position_without_exchange_exposure(session, position, "entry_order_not_found")
    logger.warning(
        f"[PositionMonitor] Closed stale DB position {position.id} for {position.ticker}: "
        f"entry order {position.entry_order_id} is not on exchange and no matching exchange exposure exists."
    )
    return True


async def _recover_ghost_closed_positions(session: AsyncSession, user_configs: dict) -> int:
    """Recover closed live positions that are still open on the exchange.

    If a closed position is found to be still open on the exchange, reopen it
    in the database and restore protective orders. Returns count of recovered positions.
    """
    cutoff = utcnow() - timedelta(hours=_CLOSED_POSITION_RECOVERY_LOOKBACK_HOURS)
    recoverable_reasons = {
        "ghost_position_auto_close",
        "exchange_closed",
        "exchange_closed_unmatched",
        "manual_close",
        "manual_close_all",
        "reverse_signal",
        "black_swan_loss_protection",
        "take_profit",
        "stop_loss",
    }
    result = await session.execute(
        select(PositionModel)
        .where(
            PositionModel.status == "closed",
            PositionModel.live_trading.is_(True),
            PositionModel.closed_at.is_not(None),
            PositionModel.closed_at >= cutoff,
            PositionModel.close_reason.in_(recoverable_reasons),
        )
        .order_by(PositionModel.closed_at.desc())
        .limit(200)
    )
    closed_positions = list(result.scalars().all())
    if not closed_positions:
        return 0

    logger.info(f"[ClosedRecovery] Checking {len(closed_positions)} recently closed live position(s) for exchange residuals")
    recovered = 0

    for position in closed_positions:
        exchange_config = await _exchange_config_for_position(session, position, user_configs)
        if not exchange_config.get("live_trading"):
            continue
        if await _has_active_duplicate_position(session, position):
            continue

        try:
            from exchange import fetch_single_position
            exchange_pos = await fetch_single_position(position.ticker, {**exchange_config, "raise_on_error": True})
        except Exception as exc:
            logger.debug(f"[GhostRecovery] Cannot verify {position.ticker}: {exc}")
            continue

        if exchange_pos is None:
            continue
        if not _exchange_position_side_matches_position(position, exchange_pos):
            continue

        contracts = _exchange_position_contracts(exchange_pos)

        if contracts <= 0:
            continue
        exchange_entry = safe_float(exchange_pos.get("entry_price") or exchange_pos.get("entryPrice"))
        db_entry = safe_float(position.entry_price)
        if exchange_entry > 0 and db_entry > 0 and abs(exchange_entry - db_entry) / db_entry > 0.05:
            logger.warning(
                f"[ClosedRecovery] Skipping {position.ticker}: exchange entry {exchange_entry} "
                f"differs from closed DB entry {db_entry}; likely a separate manual position."
            )
            continue

        logger.warning(
            f"[ClosedRecovery] POSITION RECOVERED: {position.id[:8]} on {position.ticker} "
            f"is still open on the exchange (contracts={contracts})! Reopening in database."
        )

        _sync_open_position_from_exchange(position, exchange_pos)
        await session.flush()
        try:
            await _verify_protective_orders(session, position, exchange_config)
        except Exception as exc:
            logger.error(f"[ClosedRecovery] Failed to restore protective orders for {position.ticker}: {exc}")
        _GHOST_POSITION_TRACKER.pop(str(position.id), None)
        _save_ghost_tracker()
        recovered += 1

    if recovered > 0:
        logger.warning(f"[ClosedRecovery] Recovered {recovered} closed position(s) that are still on the exchange")
    return recovered


async def run_position_monitor_once(user_configs: dict | None = None) -> dict:
    """Run one full tracking cycle and persist TP/SL/PnL updates.

    Protected by asyncio.Lock to prevent concurrent execution from
    both scheduler and manual admin API trigger.
    """
    async with _position_monitor_lock:
        stats = {
            "tracked": 0,
            "updated": 0,
            "partials": 0,
            "closed": 0,
            "adjusted": 0,
            "errors": 0,
            "recovered": 0,
            "timestamp": utcnow().isoformat(),
        }
        _load_ghost_tracker()

        try:
            async with db_manager.async_session_factory() as session:
                # P0-FIX: Recover ghost-closed positions that are still on the exchange
                try:
                    recovered = await _recover_ghost_closed_positions(session, user_configs or {})
                    stats["recovered"] = recovered
                    if recovered > 0:
                        await session.commit()
                except Exception as exc:
                    logger.warning(f"[GhostRecovery] Recovery check failed: {exc}")

                result = await session.execute(
                    select(PositionModel)
                    .where(PositionModel.status.in_(["open", "pending"]))
                    .order_by(PositionModel.opened_at.asc())
                )
                positions = list(result.scalars().all())
                stats["tracked"] = len(positions)

                for position in positions:
                    try:
                        # P2-14: Acquire per-position lock to prevent concurrent TP/SL processing
                        pos_lock = await _get_position_lock(str(position.id))
                        if pos_lock.locked():
                            logger.debug(f"[PositionMonitor] Skipping {position.id} - reconciliation already in progress")
                            continue
                        async with pos_lock:
                            changed = await _reconcile_position(session, position, user_configs or {})
                            for key, value in changed.items():
                                stats[key] = stats.get(key, 0) + value
                    except Exception as exc:
                        stats["errors"] += 1
                        logger.exception(f"[PositionMonitor] Failed to reconcile {position.id}: {exc}")

                await session.commit()
        except Exception as exc:
            stats["errors"] += 1
            logger.exception(f"[PositionMonitor] Cycle failed: {exc}")

        return stats


async def _get_position_lock(position_id: str) -> asyncio.Lock:
    """P2-14: Get or create a per-position lock to prevent concurrent reconciliation."""
    async with _position_reconcile_locks_guard:
        lock = _position_reconcile_locks.get(position_id)
        if lock is None:
            lock = asyncio.Lock()
            _position_reconcile_locks[position_id] = lock
            # Periodic cleanup of old locks
            if len(_position_reconcile_locks) > 5000:
                unlocked = [k for k, v in _position_reconcile_locks.items() if not v.locked()]
                for k in unlocked[:2500]:
                    _position_reconcile_locks.pop(k, None)
        return lock


async def _reconcile_position(session, position: PositionModel, user_configs: dict) -> dict:
    exchange_config = await _exchange_config_for_position(session, position, user_configs)

    if not bool(position.live_trading):
        return await _reconcile_paper_position(session, position, exchange_config)

    exchange_config["live_trading"] = True
    return await _reconcile_exchange_position(session, position, exchange_config)


async def _exchange_config_for_position(session, position: PositionModel, user_configs: dict) -> dict:
    config = {
        "exchange": position.exchange or settings.exchange.name,
        "api_key": settings.exchange.api_key,
        "api_secret": settings.exchange.api_secret,
        "password": settings.exchange.password,
        "live_trading": bool(position.live_trading),
        "sandbox_mode": bool(position.sandbox_mode),
        "market_type": settings.exchange.market_type,
    }
    if position.user_id and position.user_id in user_configs:
        config.update(user_configs[position.user_id])
        return config

    if position.user_id:
        user = await session.get(UserModel, position.user_id)
        if user:
            try:
                raw = loads_dict(user.settings_json)
                user_settings = decrypt_settings_payload(raw)
                exchange = (user_settings or {}).get("exchange") or {}
                config.update({
                    "exchange": exchange.get("name") or exchange.get("exchange") or config["exchange"],
                    "api_key": exchange.get("api_key") if "api_key" in exchange else config["api_key"],
                    "api_secret": exchange.get("api_secret") if "api_secret" in exchange else config["api_secret"],
                    "password": exchange.get("password") if "password" in exchange else config["password"],
                    "live_trading": safe_bool(exchange.get("live_trading"), config["live_trading"]),
                    "sandbox_mode": safe_bool(exchange.get("sandbox_mode"), config["sandbox_mode"]),
                    "market_type": exchange.get("market_type") or config["market_type"],
                })
            except (ValueError, TypeError, KeyError) as exc:
                logger.warning(f"[PositionMonitor] Could not decrypt user exchange config: {exc}")
    return config


async def _reconcile_paper_position(session, position: PositionModel, exchange_config: dict) -> dict:
    from exchange import get_latest_candle, get_ticker

    stats = {"updated": 0, "partials": 0, "closed": 0, "adjusted": 0}
    candle = await get_latest_candle(position.ticker, "1m", {**exchange_config, "live_trading": False})
    if not candle:
        ticker = await get_ticker(position.ticker, {**exchange_config, "live_trading": False})
        last = safe_float(ticker.get("last") or ticker.get("bid") or ticker.get("ask"))
        bid = safe_float(ticker.get("bid"))
        ask = safe_float(ticker.get("ask"))
        # Use bid/ask spread for SL/TP detection if available; otherwise widen by 0.5%
        if bid > 0 and ask > 0:
            candle = {"high": ask, "low": bid, "close": last}
        else:
            spread = last * 0.005 if last > 0 else 0
            candle = {"high": last + spread, "low": max(last - spread, 0.00000001), "close": last}

    high = safe_float(candle.get("high"))
    low = safe_float(candle.get("low"))
    close = safe_float(candle.get("close"))
    if close <= 0:
        return stats

    entry_price = safe_float(position.entry_price)
    direction = str(position.direction or "long").lower()
    order_type = str(position.order_type or "market").lower()
    limit_timeout = _position_limit_timeout_secs(position)

    entry_filled = position.status != "pending"

    if not entry_filled:
        if order_type == "limit" and entry_price > 0:
            entry_hit = (direction == "long" and low <= entry_price) or (direction == "short" and high >= entry_price)
            if entry_hit:
                position.status = "open"
                position.last_price = entry_price
                entry_filled = True

                # P1-FIX: CRITICAL - Re-evaluate trailing_stop config when limit order fills
                # Market conditions at fill time may differ from signal time
                new_trailing_config = await _reevaluate_trailing_stop_config(
                    session=session,
                    position=position,
                    exchange_config=exchange_config,
                    entry_price=entry_price,
                    current_price=close,
                )

                # Update position with new trailing_stop config
                position.trailing_stop_config_json = json.dumps(new_trailing_config, ensure_ascii=False)
                position.updated_at = utcnow()

                logger.info(
                    f"[PositionMonitor] 📍 Paper LIMIT order FILLED: {position.ticker} "
                    f"{direction} @ {entry_price} (low={low}, high={high}) "
                    f"trailing_stop_mode={new_trailing_config.get('mode', 'none')}"
                )
                stats["updated"] += 1
            else:
                opened_at = position.opened_at
                if opened_at:
                    if opened_at.tzinfo is None:
                        opened_at = opened_at.replace(tzinfo=timezone.utc)
                    age_secs = (utcnow() - opened_at).total_seconds()
                    if age_secs > limit_timeout:
                        position.status = "closed"
                        position.close_reason = "limit_order_timeout"
                        position.closed_at = utcnow()
                        position.updated_at = utcnow()
                        logger.warning(
                            f"[PositionMonitor] Paper limit order TIMEOUT: {position.ticker} "
                            f"(age={age_secs:.0f}s > timeout={limit_timeout}s) - position closed"
                        )
                        stats["closed"] += 1
                        return stats
                logger.debug(
                    f"[PositionMonitor] Paper LIMIT order waiting: {position.ticker} "
                    f"entry={entry_price} current=[{low},{high}]"
                )
                return stats
        else:
            position.status = "open"
            position.last_price = close
            entry_filled = True
            stats["updated"] += 1

    if not entry_filled:
        return stats

    _update_unrealized(position, close)
    if entry_filled and not stats.get("updated"):
        stats["updated"] += 1

    trailing_stop = _paper_trailing_stop_price(position, close)
    current_stop = safe_float(position.stop_loss)
    if trailing_stop and trailing_stop > 0:
        should_update = False
        if current_stop <= 0:
            should_update = True
        elif direction == "short" and trailing_stop < current_stop:
            should_update = True
        elif direction != "short" and trailing_stop > current_stop:
            should_update = True
        if should_update:
            position.stop_loss = trailing_stop
            position.updated_at = utcnow()
            stats["adjusted"] += 1

    stop_loss = safe_float(position.stop_loss)
    stop_hit = bool(stop_loss > 0 and ((direction == "long" and low <= stop_loss) or (direction == "short" and high >= stop_loss)))

    if stop_hit:
        await record_position_close_trade_async(
            session=session,
            position=position,
            exit_price=stop_loss,
            close_reason="stop_loss",
            order_status="paper_closed",
            order_details={"trigger": "stop_loss", "candle": candle, "entry_filled": entry_filled},
        )
        stats["closed"] += 1
        return stats

    tp_levels = loads_list(position.take_profit_json)
    hit_levels = _hit_take_profit_levels(direction, tp_levels, high, low)
    if hit_levels:
        opened_qty = max(safe_float(position.quantity), 0.0)
        remaining_qty = _effective_remaining_quantity(position, opened_qty)
        total_level_pnl_usdt = 0.0

        for level in hit_levels:
            qty_pct = max(0.0, safe_float(level.get("qty_pct"), 100.0))
            qty = min(remaining_qty, opened_qty * (qty_pct / 100.0)) if opened_qty > 0 else 0.0
            if qty <= 0:
                level["status"] = "hit"
                continue
            weight = qty / opened_qty if opened_qty > 0 else 1.0
            leverage = safe_float(position.leverage, 1.0)
            level_pnl = _price_pnl_pct(position.direction, position.entry_price, level.get("price"), leverage)
            position.realized_pnl_pct = round(safe_float(position.realized_pnl_pct) + (level_pnl * weight), 6)
            remaining_qty = max(0.0, remaining_qty - qty)
            level["status"] = "hit"
            level["hit_at"] = utcnow().isoformat()
            stats["partials"] += 1

            # Calculate USDT PnL for this partial close
            entry_price = safe_float(position.entry_price)
            leverage = safe_float(position.leverage, 1.0)
            # Get contract_size from trailing_stop_config
            ts_config = loads_dict(position.trailing_stop_config_json)
            contract_size = safe_float(ts_config.get("_contract_size"), 1.0)
            if entry_price > 0 and qty > 0:
                # Margin = (entry_price * qty * contract_size) / leverage
                margin_used = (entry_price * qty * contract_size) / max(1.0, leverage)
                level_pnl_usdt = margin_used * (level_pnl / 100.0)
                total_level_pnl_usdt += level_pnl_usdt

        position.remaining_quantity = remaining_qty
        position.take_profit_json = json.dumps(tp_levels, ensure_ascii=False, default=str)
        _update_unrealized(position, close)
        position.updated_at = utcnow()
        await session.flush()

        # Update user balance for partial TP hits in paper trading
        if not position.live_trading and position.user_id and total_level_pnl_usdt != 0.0:
            from core.database import update_user_balance
            await update_user_balance(session, position.user_id, total_level_pnl_usdt)

        if remaining_qty > 0:
            if position.live_trading:
                from exchange import place_protective_stop
                exchange_config = _get_exchange_config_for_position(position)
                if exchange_config:
                    await _adjust_trailing_stop_on_tp_hit(position, tp_levels, hit_levels, exchange_config, place_protective_stop)
            else:
                new_stop = _compute_paper_trailing_stop(position, hit_levels)
                if new_stop and new_stop > 0:
                    old_sl = safe_float(position.stop_loss)
                    direction = str(position.direction or "long").lower()
                    if direction == "short":
                        if old_sl <= 0 or new_stop < old_sl:
                            position.stop_loss = new_stop
                    else:
                        if old_sl <= 0 or new_stop > old_sl:
                            position.stop_loss = new_stop

        if remaining_qty <= max(0.00000001, opened_qty * 0.000001):
            final_price = safe_float(hit_levels[-1].get("price"), close)
            await record_position_close_trade_async(
                session=session,
                position=position,
                exit_price=final_price,
                close_reason="take_profit",
                order_status="paper_closed",
                order_details={"trigger": "take_profit", "levels": hit_levels, "candle": candle},
            )
            stats["closed"] += 1

    return stats


def _compute_paper_trailing_stop(position: PositionModel, hit_levels: list[dict]) -> float | None:
    trailing_config = loads_dict(position.trailing_stop_config_json)
    trailing_mode = _resolve_trailing_mode(trailing_config, position)
    if trailing_mode not in {"breakeven_on_tp1", "step_trailing"}:
        return None
    if not hit_levels:
        return None

    direction = str(position.direction or "long").lower()
    entry_price = safe_float(position.entry_price)
    if entry_price <= 0:
        return None

    breakeven_buffer = safe_float(trailing_config.get("breakeven_buffer_pct"), 0.2)
    step_buffer = safe_float(trailing_config.get("step_buffer_pct"), 0.3)

    hit_count = max(hit_lvl.get("level", 0) for hit_lvl in hit_levels) if hit_levels else 0

    if trailing_mode == "breakeven_on_tp1":
        if direction == "short":
            return entry_price * (1 + breakeven_buffer / 100.0)
        return entry_price * (1 - breakeven_buffer / 100.0)

    if trailing_mode == "step_trailing":
        tp_levels = loads_list(position.take_profit_json)
        if not tp_levels:
            if direction == "short":
                return entry_price * (1 + breakeven_buffer / 100.0)
            return entry_price * (1 - breakeven_buffer / 100.0)

        reverse_sort = direction == "short"
        all_levels = sorted(tp_levels, key=lambda x: safe_float(x.get("price")), reverse=reverse_sort)

        if hit_count == 1:
            if direction == "short":
                return entry_price * (1 + breakeven_buffer / 100.0)
            return entry_price * (1 - breakeven_buffer / 100.0)
        elif hit_count >= 2 and hit_count - 1 < len(all_levels):
            ref_price = safe_float(all_levels[hit_count - 2].get("price"))
            if ref_price > 0:
                if direction == "short":
                    return ref_price * (1 + step_buffer / 100.0)
                return ref_price * (1 - step_buffer / 100.0)

    return None


def _hit_take_profit_levels(direction: str, levels: list[dict], high: float, low: float) -> list[dict]:
    pending = [level for level in levels if str(level.get("status") or "pending").lower() not in {"hit", "filled", "closed"}]
    if str(direction).lower() == "short":
        pending.sort(key=lambda item: safe_float(item.get("price")), reverse=True)
        return [level for level in pending if safe_float(level.get("price")) > 0 and low <= safe_float(level.get("price"))]
    pending.sort(key=lambda item: safe_float(item.get("price")))
    return [level for level in pending if safe_float(level.get("price")) > 0 and high >= safe_float(level.get("price"))]


async def _check_pending_limit_orders(session, position: PositionModel, exchange_config: dict) -> None:
    """Check status of pending limit orders and update position if filled or expired."""
    if not position.entry_order_id or position.entry_order_id == "":
        return

    try:
        import ccxt

        from exchange import _get_or_create_exchange, _resolve_symbol

        exchange = _get_or_create_exchange(
            exchange_id=exchange_config.get("exchange", settings.exchange.name),
            api_key=exchange_config.get("api_key", settings.exchange.api_key),
            api_secret=exchange_config.get("api_secret", settings.exchange.api_secret),
            password=exchange_config.get("password", settings.exchange.password),
            live=bool(exchange_config.get("live_trading", False)),
            sandbox=bool(exchange_config.get("sandbox_mode", False)),
            market_type=exchange_config.get("market_type", settings.exchange.market_type),
        )

        try:
            symbol = await asyncio.to_thread(
                _resolve_symbol,
                exchange,
                position.ticker,
                exchange_config.get("market_type", settings.exchange.market_type),
            )
            order = await asyncio.to_thread(exchange.fetch_order, position.entry_order_id, symbol)

            raw_status = order.get("status")
            # P0-FIX: For limit orders, status=None means "not yet filled" (pending),
            # NOT "open/filled". OKX Sandbox returns None for unfilled limit orders.
            # Treating None as "open" caused Ghost Position detection to trigger
            # prematurely, killing limit orders before their timeout expired.
            if raw_status is None:
                order_status = "open"  # CCXT convention: open = waiting to fill
                logger.info(f"[PositionMonitor] OKX sandbox returned status=None for order {position.entry_order_id}, treating as 'open' (pending fill)")
            else:
                order_status = str(raw_status).lower()

            if order_status in {"closed", "filled"}:
                # P0-FIX: Prevent re-processing an already-consumed filled order.
                # Once the position is open with an entry_price (set from a prior
                # fill), every subsequent cycle re-sets status="open" from the same
                # filled order that never changes status. This creates an infinite
                # loop when the user manually closes on exchange: filled→open→
                # exchange empty→next cycle→filled→open→...
                if position.status == "open" and safe_float(position.entry_price) > 0:
                    return

                # Limit order filled - update position entry price and quantity
                filled_price = safe_float(order.get("average") or order.get("price"))
                filled_amount = safe_float(order.get("filled") or 0)
                filled_cost = safe_float(order.get("cost") or 0)

                position.status = "open"
                position.updated_at = utcnow()

                if filled_price > 0:
                    position.entry_price = filled_price
                    position.last_price = filled_price

                    # P1-FIX: CRITICAL - Re-evaluate trailing_stop config when limit order fills
                    # Market conditions at fill time may differ from signal time (hours later)
                    # Fetch current market price
                    try:
                        from exchange import get_ticker
                        ticker = await get_ticker(position.ticker, exchange_config)
                        current_price = safe_float(ticker.get("last") or ticker.get("bid") or ticker.get("ask") or filled_price)

                        new_trailing_config = await _reevaluate_trailing_stop_config(
                            session=session,
                            position=position,
                            exchange_config=exchange_config,
                            entry_price=filled_price,
                            current_price=current_price,
                        )

                        # Update position with new trailing_stop config
                        position.trailing_stop_config_json = json.dumps(new_trailing_config, ensure_ascii=False)

                        logger.info(
                            f"[P1-FIX] Live LIMIT order filled: {position.ticker} "
                            f"@ {filled_price} - trailing_stop re-evaluated: "
                            f"mode={new_trailing_config.get('mode', 'none')}, "
                            f"market={new_trailing_config.get('_market_condition_at_fill', 'unknown')}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"[P1-FIX] Failed to re-evaluate trailing_stop for live limit order: {e}. "
                            f"Using original config."
                        )

                # Sync actual filled quantity from exchange
                if filled_amount > 0:
                    position.quantity = filled_amount
                    position.remaining_quantity = filled_amount

                # Update margin based on actual filled cost or contract-aware fallback
                position.margin = _filled_margin_from_order(position, filled_cost, filled_amount, filled_price)

                # Log fee if available
                fee_info = order.get("fee", {})
                if fee_info:
                    fee_cost = safe_float(fee_info.get("cost", 0))
                    fee_currency = str(fee_info.get("currency", ""))
                    if fee_cost > 0:
                        position.fees_total_usdt = fee_cost
                        logger.info(f"[PositionMonitor] Fee recorded: {fee_cost} {fee_currency}")

                logger.info(
                    f"[PositionMonitor] Limit order filled for {position.ticker}: "
                    f"qty={filled_amount}, price={filled_price}, cost={filled_cost}, margin={position.margin}"
                )

                await _create_protective_orders_for_position(position, exchange, symbol, filled_amount)

            elif order_status in {"canceled", "cancelled", "expired", "rejected"}:
                filled_amount = safe_float(order.get("filled") or 0)
                filled_price = safe_float(order.get("average") or order.get("price"))

                if filled_amount > 0 and filled_price > 0:
                    position.status = "open"
                    position.quantity = filled_amount
                    position.remaining_quantity = filled_amount
                    if filled_price > 0:
                        position.entry_price = filled_price
                        position.last_price = filled_price
                    filled_cost = safe_float(order.get("cost") or 0)
                    position.margin = _filled_margin_from_order(position, filled_cost, filled_amount, filled_price)

                    # P1-FIX: Re-evaluate trailing_stop for partial fill on cancel/expire
                    try:
                        from exchange import get_ticker
                        ticker_data = await get_ticker(position.ticker, exchange_config)
                        current_price = safe_float(ticker_data.get("last") or ticker_data.get("bid") or ticker_data.get("ask") or filled_price)

                        new_trailing_config = await _reevaluate_trailing_stop_config(
                            session=session,
                            position=position,
                            exchange_config=exchange_config,
                            entry_price=filled_price,
                            current_price=current_price,
                        )
                        position.trailing_stop_config_json = json.dumps(new_trailing_config, ensure_ascii=False)

                        logger.info(
                            f"[P1-FIX] Partial fill on {order_status} - trailing_stop re-evaluated: "
                            f"{position.ticker} mode={new_trailing_config.get('mode', 'none')}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"[P1-FIX] Failed to re-evaluate trailing_stop for partial fill: {e}"
                        )

                    position.updated_at = utcnow()
                    logger.warning(
                        f"[PositionMonitor] Limit order {order_status} with partial fill for {position.ticker}: "
                        f"qty={filled_amount}, price={filled_price}, position remains open"
                    )
                    await _create_protective_orders_for_position(position, exchange, symbol, filled_amount)
                else:
                    position.status = "closed"
                    position.close_reason = "limit_order_expired"
                    position.closed_at = utcnow()
                    position.updated_at = utcnow()
                    logger.warning(f"[PositionMonitor] Limit order {order_status} for {position.ticker}, position closed")

            elif order_status in {"open", "new"}:
                # Check for partial fills first
                filled_amount = safe_float(order.get("filled") or 0)
                filled_price = safe_float(order.get("average") or order.get("price"))

                if filled_amount > 0 and filled_price > 0:
                    # Partially filled - update position with filled amount
                    if position.status == "pending":
                        position.status = "open"
                    position.quantity = filled_amount
                    position.remaining_quantity = filled_amount
                    if filled_price > 0:
                        position.entry_price = filled_price
                        position.last_price = filled_price
                    filled_cost = safe_float(order.get("cost") or 0)
                    position.margin = _filled_margin_from_order(position, filled_cost, filled_amount, filled_price)

                    # P1-FIX: Re-evaluate trailing_stop for partial fill
                    try:
                        from exchange import get_ticker
                        ticker_data = await get_ticker(position.ticker, exchange_config)
                        current_price = safe_float(ticker_data.get("last") or ticker_data.get("bid") or ticker_data.get("ask") or filled_price)

                        new_trailing_config = await _reevaluate_trailing_stop_config(
                            session=session,
                            position=position,
                            exchange_config=exchange_config,
                            entry_price=filled_price,
                            current_price=current_price,
                        )
                        position.trailing_stop_config_json = json.dumps(new_trailing_config, ensure_ascii=False)

                        logger.info(
                            f"[P1-FIX] Partial fill detected - trailing_stop re-evaluated: "
                            f"{position.ticker} mode={new_trailing_config.get('mode', 'none')}"
                        )
                    except Exception as e:
                        logger.warning(
                            f"[P1-FIX] Failed to re-evaluate trailing_stop for partial fill: {e}"
                        )

                    position.updated_at = utcnow()
                    logger.info(
                        f"[PositionMonitor] Limit order partially filled for {position.ticker}: "
                        f"qty={filled_amount}, price={filled_price}, position remains open"
                    )
                    await _create_protective_orders_for_position(position, exchange, symbol, filled_amount)
                    return

                # Check if order has exceeded timeout
                created_at = order.get("timestamp")
                if created_at:
                    order_age_secs = (time.time() * 1000 - created_at) / 1000
                    limit_timeout = _position_limit_timeout_secs(position)
                    if order_age_secs > limit_timeout:
                        cancel_confirmed = False
                        try:
                            await asyncio.to_thread(exchange.cancel_order, position.entry_order_id, symbol)
                            cancel_confirmed = True
                        except ccxt.OrderNotFound:
                            logger.warning(
                                f"[PositionMonitor] Limit order not found during timeout cancel for {position.ticker}. "
                                f"Re-fetching to check if it was filled before we could cancel."
                            )
                            try:
                                recheck = await asyncio.to_thread(exchange.fetch_order, position.entry_order_id, symbol)
                                if str(recheck.get("status", "")).lower() in {"closed", "filled"}:
                                    filled_qty = safe_float(recheck.get("filled") or recheck.get("amount") or 0)
                                    if filled_qty > 0:
                                        logger.info(
                                            f"[PositionMonitor] Order was actually filled before cancel: {position.ticker} qty={filled_qty}"
                                        )
                                        position.status = "open"
                                        position.quantity = filled_qty
                                        position.remaining_quantity = filled_qty
                                        position.updated_at = utcnow()
                                        await session.flush()
                                        return {"status": "filled", "order": recheck}
                            except Exception:
                                pass
                            cancel_confirmed = True
                        except ccxt.NetworkError as e:
                            logger.warning(f"[PositionMonitor] Network error cancelling limit order: {e}")
                        except Exception as e:
                            logger.warning(f"[PositionMonitor] Failed to cancel limit order: {e}")

                        if not cancel_confirmed:
                            return

                        position.status = "closed"
                        position.close_reason = "limit_order_timeout"
                        position.closed_at = utcnow()
                        position.updated_at = utcnow()
                        logger.info(f"[PositionMonitor] Cancelled expired limit order for {position.ticker}")
        finally:
            pass
    except ccxt.OrderNotFound:
        if await _close_missing_entry_order_without_exposure(session, position, exchange_config):
            return
        logger.warning(
            f"[PositionMonitor] Limit order not found on exchange for {position.ticker}; "
            "leaving position state unchanged because exchange exposure could not be safely ruled out"
        )
    except ccxt.BaseError as e:
        logger.warning(f"[PositionMonitor] Exchange error checking limit order for {position.ticker}: {e}")
    except Exception as e:
        logger.debug(f"[PositionMonitor] Error checking limit order for {position.ticker}: {e}")


async def _create_protective_orders_for_position(position: PositionModel, exchange, symbol: str, filled_qty: float) -> None:
    """Create TP/SL orders for a position that just filled from a pending limit order."""
    if filled_qty <= 0:
        return

    side = "buy" if str(position.direction or "").lower() == "long" else "sell"
    tp_side = "sell" if side == "buy" else "buy"
    pos_side = "long" if side == "buy" else "short"

    tp_levels = loads_list(position.take_profit_json)
    if tp_levels:
        from exchange import _create_conditional_order as _cco
        tp_ids = [str(order_id or "") for order_id in loads_list(position.take_profit_order_ids_json)]
        if len(tp_ids) < len(tp_levels):
            tp_ids.extend([""] * (len(tp_levels) - len(tp_ids)))
        tp_changed = False

        for i, tp in enumerate(tp_levels):
            level_status = str(tp.get("status") or "pending").lower()
            if level_status in {"hit", "filled", "closed"}:
                continue
            existing_id = str(tp.get("order_id") or (tp_ids[i] if i < len(tp_ids) else "") or "")
            if existing_id:
                if i < len(tp_ids) and tp_ids[i] != existing_id:
                    tp_ids[i] = existing_id
                    tp_changed = True
                if tp.get("order_id") != existing_id:
                    tp["order_id"] = existing_id
                    tp_changed = True
                continue

            tp_price = safe_float(tp.get("price"))
            tp_qty_pct = safe_float(tp.get("qty_pct"), 100.0)
            if tp_price <= 0:
                continue
            tp_qty = filled_qty * (tp_qty_pct / 100.0)
            if tp_qty <= 0:
                continue
            try:
                tp_order = await _cco(exchange, symbol, "take_profit", tp_side, round(tp_qty, 6), tp_price, pos_side)
                order_id = str(tp_order.get("id") or "")
                if not order_id:
                    logger.warning(f"[PositionMonitor] TP{i+1} for filled limit returned no order id: price={tp_price}")
                    continue
                tp_ids[i] = order_id
                tp["order_id"] = order_id
                tp_changed = True
                logger.info(f"[PositionMonitor] TP{i+1} created for filled limit: price={tp_price}")
            except ccxt.BaseError as e:
                logger.error(f"[PositionMonitor] Failed to create TP{i+1} for filled limit: {e}")
            except Exception as e:
                logger.error(f"[PositionMonitor] Failed to create TP{i+1} for filled limit: {e}")

        if tp_changed:
            position.take_profit_order_ids_json = json.dumps(tp_ids, ensure_ascii=False)
            position.take_profit_json = json.dumps(tp_levels, ensure_ascii=False, default=str)

    if not position.stop_loss_order_id:
        sl_price = safe_float(position.stop_loss)
        if sl_price > 0:
            from exchange import _create_conditional_order as _cco
            try:
                sl_order = await _cco(exchange, symbol, "stop_loss", tp_side, filled_qty, sl_price, pos_side)
                position.stop_loss_order_id = str(sl_order.get("id") or "")
                logger.info(f"[PositionMonitor] SL created for filled limit: price={sl_price}")
            except ccxt.BaseError as e:
                logger.error(f"[PositionMonitor] Failed to create SL for filled limit: {e}")
            except Exception as e:
                logger.error(f"[PositionMonitor] Failed to create SL for filled limit: {e}")


async def _verify_protective_orders(session, position: PositionModel, exchange_config: dict) -> bool:
    """Verify TP/SL orders still exist on exchange; re-place missing ones.

    Returns True if any orders were re-placed.
    Called periodically during reconciliation to protect against
    exchange-side order cancellations (e.g. after server restart, disconnect).
    """
    from exchange import (
        _create_conditional_order,
        _get_or_create_exchange,
        _resolve_symbol,
        get_open_orders,
    )

    raw_tp_ids = [str(oid or "") for oid in loads_list(position.take_profit_order_ids_json)]
    tp_levels = loads_list(position.take_profit_json)
    tp_ids: list[str] = []
    for i, level in enumerate(tp_levels):
        oid = ""
        if isinstance(level, dict) and level.get("order_id"):
            oid = str(level["order_id"])
        elif i < len(raw_tp_ids):
            oid = raw_tp_ids[i]
        tp_ids.append(oid)
    if not tp_levels:
        tp_ids = [oid for oid in raw_tp_ids if oid]

    sl_id = str(position.stop_loss_order_id or "")

    needs_sl = safe_float(position.stop_loss) > 0
    needs_tp = len(tp_ids) > 0 or bool(tp_levels)

    if not needs_sl and not needs_tp:
        return False

    try:
        open_orders = await get_open_orders(
            position.ticker,
            {**exchange_config, "raise_on_error": True, "require_algo_orders": True},
        )
    except Exception as e:
        logger.warning(f"[PositionMonitor] Skipping protective order verification for {position.ticker}: {e}")
        return False

    open_ids = {str(o.get("id") or "") for o in open_orders if o.get("id")}

    # Check SL
    sl_missing = needs_sl and (not sl_id or sl_id not in open_ids)
    if sl_missing:
        if sl_id:
            logger.warning(
                f"[PositionMonitor] CRITICAL: SL order {sl_id[:8]} for {position.ticker} "
                f"NOT found on exchange - re-placing"
            )
        else:
            logger.warning(
                f"[PositionMonitor] CRITICAL: {position.ticker} has no SL order id "
                f"despite stop_loss={position.stop_loss} - placing protection"
            )

    # Check TP
    missing_tp_indices = []
    for i, tp_id in enumerate(tp_ids):
        level = tp_levels[i] if i < len(tp_levels) and isinstance(tp_levels[i], dict) else {}
        level_status = str(level.get("status") or "pending").lower()
        if level_status in {"hit", "filled", "closed"}:
            continue
        if not tp_id or tp_id not in open_ids:
            missing_tp_indices.append(i)
            if tp_id:
                logger.warning(
                    f"[PositionMonitor] CRITICAL: TP{i+1} order {tp_id[:8]} for {position.ticker} "
                    f"NOT found on exchange - re-placing"
                )
            else:
                logger.warning(
                    f"[PositionMonitor] CRITICAL: TP{i+1} for {position.ticker} has no order id - placing protection"
                )

    if not sl_missing and not missing_tp_indices:
        return False

    side = "buy" if str(position.direction or "").lower() == "long" else "sell"
    tp_close_side = "sell" if side == "buy" else "buy"
    pos_side = "long" if side == "buy" else "short"

    exchange = _get_or_create_exchange(
        exchange_id=exchange_config.get("exchange", ""),
        api_key=exchange_config.get("api_key", ""),
        api_secret=exchange_config.get("api_secret", ""),
        password=exchange_config.get("password", ""),
        live=bool(exchange_config.get("live_trading", False)),
        sandbox=bool(exchange_config.get("sandbox_mode", False)),
        market_type=exchange_config.get("market_type", ""),
    )

    try:
        symbol = await asyncio.to_thread(
            _resolve_symbol,
            exchange,
            position.ticker,
            exchange_config.get("market_type", ""),
        )

        remaining_qty = safe_float(position.remaining_quantity, safe_float(position.quantity, 0.0))
        if remaining_qty <= 0:
            return False

        re_placed = False

        # Re-place missing TP orders
        if missing_tp_indices and tp_levels:
            tp_changed = False
            for i in missing_tp_indices:
                if i >= len(tp_levels):
                    continue
                tp = tp_levels[i]
                level_status = str(tp.get("status") or "pending").lower()
                if level_status in {"hit", "filled", "closed"}:
                    continue
                tp_price = safe_float(tp.get("price"))
                tp_qty_pct = safe_float(tp.get("qty_pct"), 100.0)
                if tp_price <= 0:
                    continue
                tp_qty = remaining_qty * (tp_qty_pct / 100.0)
                if tp_qty <= 0:
                    continue
                try:
                    tp_order = await _create_conditional_order(
                        exchange, symbol, "take_profit", tp_close_side,
                        round(tp_qty, 6), tp_price, pos_side,
                    )
                    new_id = str(tp_order.get("id") or "")
                    if new_id:
                        tp["order_id"] = new_id
                        tp_changed = True
                        if i < len(tp_ids):
                            tp_ids[i] = new_id
                        else:
                            tp_ids.append(new_id)
                        logger.info(
                            f"[PositionMonitor] TP{i+1} re-placed for {position.ticker} @ {tp_price}, "
                            f"new order id={new_id[:8]}"
                        )
                        re_placed = True
                except Exception as e:
                    logger.error(
                        f"[PositionMonitor] Failed to re-place TP{i+1} for {position.ticker}: {e}"
                    )

            if tp_changed:
                position.take_profit_order_ids_json = json.dumps(tp_ids, ensure_ascii=False)
                position.take_profit_json = json.dumps(tp_levels, ensure_ascii=False, default=str)

        # Re-place missing SL order
        if sl_missing:
            sl_price = safe_float(position.stop_loss)
            if sl_price > 0:
                try:
                    sl_order = await _create_conditional_order(
                        exchange, symbol, "stop_loss", tp_close_side,
                        remaining_qty, sl_price, pos_side,
                    )
                    new_sl_id = str(sl_order.get("id") or "")
                    if new_sl_id:
                        position.stop_loss_order_id = new_sl_id
                        logger.info(
                            f"[PositionMonitor] SL re-placed for {position.ticker} @ {sl_price}, "
                            f"new order id={new_sl_id[:8]}"
                        )
                        re_placed = True
                    else:
                        position.stop_loss_order_id = ""
                        logger.error(
                            f"[PositionMonitor] Failed to re-place SL for {position.ticker}: "
                            f"no order id returned"
                        )
                except Exception as e:
                    position.stop_loss_order_id = ""
                    logger.error(
                        f"[PositionMonitor] Failed to re-place SL for {position.ticker}: {e}"
                    )

        return re_placed

    except Exception as e:
        logger.error(f"[PositionMonitor] Protective order verification error for {position.ticker}: {e}")
        return False


async def _reconcile_exchange_position(session, position: PositionModel, exchange_config: dict) -> dict:
    from exchange import get_open_positions, get_recent_orders, get_ticker, place_protective_stop

    stats = {"updated": 0, "partials": 0, "closed": 0, "adjusted": 0}

    await _check_pending_limit_orders(session, position, exchange_config)
    if str(position.status or "").lower() == "closed":
        stats["closed"] += 1
        return stats

    checked_exchange_config = {**exchange_config, "raise_on_error": True}
    try:
        exchange_positions = await get_open_positions(checked_exchange_config)
    except Exception as exc:
        logger.warning(
            f"[PositionMonitor] Skipping exchange position reconciliation for {position.ticker}; "
            f"open-position query failed: {exc}"
        )
        ticker = await get_ticker(position.ticker, exchange_config)
        mark_price = safe_float(ticker.get("last") or position.last_price)
        if mark_price > 0:
            _update_unrealized(position, mark_price)
            stats["updated"] += 1
        return stats

    match = _find_exchange_position(position, exchange_positions)

    if match:
        _GHOST_POSITION_TRACKER.pop(position.id, None)
        _save_ghost_tracker()
        _sync_open_position_from_exchange(position, match)
        mark_price = safe_float(match.get("mark_price") or match.get("markPrice") or match.get("entry_price"))
        if mark_price > 0:
            _update_unrealized(position, mark_price)
            stats["updated"] += 1
        if await _maybe_adjust_trailing_stop(position, exchange_config, match, place_protective_stop):
            stats["adjusted"] += 1
            await session.flush()
        try:
            tp_orders = await get_recent_orders(position.ticker, 20, checked_exchange_config)
        except Exception as exc:
            logger.warning(f"[PositionMonitor] Skipping TP order reconciliation for {position.ticker}; recent-order query failed: {exc}")
            tp_orders = []
        tp_hit_levels = _detect_tp_hits_from_orders(position, tp_orders)
        if tp_hit_levels:
            tp_levels = loads_list(position.take_profit_json)
            if await _adjust_trailing_stop_on_tp_hit(position, tp_levels, tp_hit_levels, exchange_config, place_protective_stop):
                stats["adjusted"] += 1
                await session.flush()
            # Record account risk and close position if fully filled
            if safe_float(position.remaining_quantity) <= 0:
                exit_price = safe_float(tp_hit_levels[-1].get("price")) if tp_hit_levels else safe_float(position.entry_price)
                await record_position_close_trade_async(
                    session=session,
                    position=position,
                    exit_price=exit_price,
                    close_reason="take_profit",
                    order_status="exchange_closed",
                    order_details={"trigger": "take_profit", "levels": tp_hit_levels},
                )
                stats["closed"] += 1

        now = utcnow()
        last_verify = _PROTECTIVE_ORDERS_LAST_VERIFY.get(position.id)
        if not last_verify or (now - last_verify).total_seconds() >= _PROTECTIVE_ORDERS_VERIFY_INTERVAL:
            _PROTECTIVE_ORDERS_LAST_VERIFY[position.id] = now
            if await _verify_protective_orders(session, position, exchange_config):
                stats["adjusted"] += 1
                await session.flush()

        return stats

    try:
        order = await _find_recent_close_order(position, checked_exchange_config, get_recent_orders)
    except Exception as exc:
        logger.warning(
            f"[PositionMonitor] Skipping missing-position handling for {position.ticker}; "
            f"recent-order query failed: {exc}"
        )
        ticker = await get_ticker(position.ticker, exchange_config)
        mark_price = safe_float(ticker.get("last") or position.last_price)
        if mark_price > 0:
            _update_unrealized(position, mark_price)
            stats["updated"] += 1
        return stats

    if not order:
        # P0-CRITICAL: NEVER trust batch list absence alone to increment ghost counter.
        # OKX and other exchanges may return partial position lists. Every time a
        # position is absent from the batch list, we MUST verify with a targeted
        # single-position fetch. Only increment the ghost counter when both the
        # batch list and the single-fetch confirm the position is gone.
        positions_data_reliable = len(exchange_positions) > 0

        now = utcnow()
        ghost_entry = _GHOST_POSITION_TRACKER.get(position.id)

        if not positions_data_reliable:
            # Empty batch lists are not enough on their own, but a targeted
            # single-position miss is enough to clean stale DB-only positions.
            from exchange import fetch_single_position
            try:
                single_pos = await fetch_single_position(position.ticker, checked_exchange_config)
                if single_pos is not None:
                    logger.warning(
                        f"[P0-CRITICAL] Empty positions list but single-fetch found {position.ticker}! "
                        f"Exchange API inconsistency detected — position is safe"
                    )
                    match = single_pos
                    _GHOST_POSITION_TRACKER.pop(position.id, None)
                    _save_ghost_tracker()
                    mark_price = safe_float(match.get("mark_price") or match.get("markPrice") or match.get("entry_price"))
                    if mark_price > 0:
                        _update_unrealized(position, mark_price)
                        stats["updated"] += 1
                    return stats
            except Exception as single_exc:
                logger.warning(
                    f"[P0-CRITICAL] Empty positions list but single-position verification failed "
                    f"for {position.ticker}: {single_exc}. Keeping DB state unchanged."
                )
                ticker = await get_ticker(position.ticker, exchange_config)
                mark_price = safe_float(ticker.get("last") or position.last_price)
                if mark_price > 0:
                    _update_unrealized(position, mark_price)
                    stats["updated"] += 1
                return stats

            if str(position.status or "").lower() == "pending" and position.entry_order_id:
                logger.info(
                    f"[PositionMonitor] Empty exchange positions and no filled exposure for pending "
                    f"limit order {position.entry_order_id} on {position.ticker}; keeping pending order state."
                )
                ticker = await get_ticker(position.ticker, exchange_config)
                mark_price = safe_float(ticker.get("last") or position.last_price)
                if mark_price > 0:
                    _update_unrealized(position, mark_price)
                    stats["updated"] += 1
                return stats

            # P0-FIX: Even if status is not "pending", check if there's a pending limit order
            # that hasn't timed out yet. Don't close the position if the order is still valid.
            if position.entry_order_id and str(position.entry_order_id or "").strip():
                limit_timeout = _position_limit_timeout_secs(position)
                opened_at = position.opened_at
                if opened_at:
                    if opened_at.tzinfo is None:
                        opened_at = opened_at.replace(tzinfo=timezone.utc)
                    age_secs = (utcnow() - opened_at).total_seconds()
                    if age_secs < limit_timeout:
                        logger.info(
                            f"[P0-FIX] Position {position.ticker} (status={position.status}) has pending limit order "
                            f"{position.entry_order_id} (age={age_secs:.0f}s < timeout={limit_timeout}s). "
                            f"Keeping position open — order may still fill."
                        )
                        ticker = await get_ticker(position.ticker, exchange_config)
                        mark_price = safe_float(ticker.get("last") or position.last_price)
                        if mark_price > 0:
                            _update_unrealized(position, mark_price)
                            stats["updated"] += 1
                        return stats
                    else:
                        logger.info(
                            f"[P0-FIX] Position {position.ticker} limit order {position.entry_order_id} "
                            f"has EXCEEDED timeout (age={age_secs:.0f}s > timeout={limit_timeout}s)."
                        )

            await _close_db_position_without_exchange_exposure(
                session,
                position,
                "exchange_position_not_found",
            )
            logger.warning(
                f"[PositionMonitor] Closed stale DB position {position.id} for {position.ticker}: "
                "get_open_positions returned empty and targeted single-position verification also found no exposure."
            )
            stats["closed"] += 1
            return stats

        # Exchange data is non-empty (positions_data_reliable=True) and position not
        # found in batch list. Before incrementing ghost counter, ALWAYS verify with
        # single-position fetch. Exchanges like OKX can return 4-5 out of 10 positions,
        # missing the one we're looking for — that's NOT proof it's gone.
        from exchange import fetch_single_position
        try:
            single_verify = await fetch_single_position(position.ticker, checked_exchange_config)
            if single_verify is not None:
                # Position IS on the exchange! Batch list was incomplete.
                logger.warning(
                    f"[P0-CRITICAL] Position {position.ticker} not in batch list "
                    f"(got {len(exchange_positions)} positions) but single-fetch found it! "
                    f"Exchange API returned incomplete data — marking as safe"
                )
                _GHOST_POSITION_TRACKER.pop(position.id, None)
                _save_ghost_tracker()
                mark_price = safe_float(single_verify.get("mark_price") or single_verify.get("markPrice") or single_verify.get("entry_price"))
                if mark_price > 0:
                    _update_unrealized(position, mark_price)
                    stats["updated"] += 1
                return stats
        except Exception as single_exc:
            # Single-fetch API call failed — cannot confirm position is gone.
            # Do NOT increment ghost counter on API errors; wait for next cycle.
            logger.warning(
                f"[P0-CRITICAL] Single-position verification failed for {position.ticker}: {single_exc}. "
                f"Batch list had {len(exchange_positions)} positions but single-fetch error. "
                f"Deferring ghost decision to next cycle."
            )
            ticker = await get_ticker(position.ticker, exchange_config)
            mark_price = safe_float(ticker.get("last") or position.last_price)
            if mark_price > 0:
                _update_unrealized(position, mark_price)
                stats["updated"] += 1
            return stats

        # P0-FIX: Before incrementing ghost counter, check if this position has a
        # pending limit order that hasn't timed out yet. Ghost detection should NOT
        # trigger for pending limit orders — the order may still fill.
        if position.entry_order_id and str(position.entry_order_id or "").strip():
            limit_timeout = _position_limit_timeout_secs(position)
            opened_at = position.opened_at
            if opened_at:
                if opened_at.tzinfo is None:
                    opened_at = opened_at.replace(tzinfo=timezone.utc)
                age_secs = (utcnow() - opened_at).total_seconds()
                if age_secs < limit_timeout:
                    logger.info(
                        f"[P0-FIX] Position {position.ticker} has pending limit order {position.entry_order_id} "
                        f"(age={age_secs:.0f}s < timeout={limit_timeout}s). "
                        f"Skipping ghost detection — order may still fill."
                    )
                    ticker = await get_ticker(position.ticker, exchange_config)
                    mark_price = safe_float(ticker.get("last") or position.last_price)
                    if mark_price > 0:
                        _update_unrealized(position, mark_price)
                        stats["updated"] += 1
                    return stats
                else:
                    logger.info(
                        f"[P0-FIX] Position {position.ticker} limit order {position.entry_order_id} "
                        f"has EXCEEDED timeout (age={age_secs:.0f}s > timeout={limit_timeout}s). "
                        f"Proceeding with ghost detection."
                    )

        # Only increment ghost counter when BOTH batch list and single-fetch confirm
        # the position is not on the exchange. This is the only safe path to +1.
        ghost_entry = ghost_entry or {"fail_count": 0, "first_missing_at": now, "last_check": now}
        ghost_entry["fail_count"] += 1
        ghost_entry.setdefault("first_missing_at", ghost_entry.get("last_check", now))
        ghost_entry["last_check"] = now
        _GHOST_POSITION_TRACKER[position.id] = ghost_entry
        _save_ghost_tracker()

        logger.info(
            f"[GhostSafe] {position.ticker} confirmed absent by both batch+single-fetch. "
            f"Ghost counter={ghost_entry['fail_count']}, threshold={_calculate_ghost_threshold(position)}"
        )

        missing_since = ghost_entry.get("first_missing_at", now)
        missing_elapsed = (now - missing_since).total_seconds() if missing_since else 0.0

        # P0-FIX: Use dynamic threshold based on position value
        dynamic_threshold = _calculate_ghost_threshold(position)

        if ghost_entry["fail_count"] >= dynamic_threshold and missing_elapsed >= _GHOST_MIN_ELAPSED_SECS:
            # P0-CRITICAL: Final verification before ghost-closing.
            # Re-fetch the position from exchange one more time to confirm it's really gone.
            from exchange import fetch_single_position
            try:
                final_check = await fetch_single_position(position.ticker, checked_exchange_config)
                if final_check is not None:
                    logger.warning(
                        f"[P0-CRITICAL] ABORTED ghost close for {position.ticker}: "
                        f"position found on re-verification! Exchange API was returning incomplete data. "
                        f"Resetting ghost counter."
                    )
                    _GHOST_POSITION_TRACKER.pop(position.id, None)
                    _save_ghost_tracker()
                    mark_price = safe_float(final_check.get("mark_price") or final_check.get("markPrice") or final_check.get("entry_price"))
                    if mark_price > 0:
                        _update_unrealized(position, mark_price)
                        stats["updated"] += 1
                    return stats
            except Exception as verify_exc:
                logger.warning(
                    f"[P0-CRITICAL] Final position verification failed for {position.ticker}: {verify_exc}. "
                    f"Aborting ghost close — cannot safely confirm position is gone."
                )
                return stats

            # P0-FIX: Log with dynamic threshold info
            contract_size = _position_contract_size(position) or 1.0
            position_value = (
                safe_float(position.entry_price, 0.0)
                * safe_float(position.quantity, 0.0)
                * contract_size
            )
            logger.warning(
                f"[P0-FIX] GHOST POSITION: {position.id[:8]} on {position.ticker} "
                f"(value=${position_value:.2f}, threshold={dynamic_threshold}) "
                f"not found on exchange after {ghost_entry['fail_count']} attempts over "
                f"{missing_elapsed:.0f}s - forcing close"
            )
            # SAFETY: Only close if we can get a valid market price
            try:
                ticker = await get_ticker(position.ticker, exchange_config)
                exit_price = safe_float(ticker.get("last"))
                if not exit_price or exit_price <= 0:
                    logger.warning(
                        f"[P0-FIX] Ghost position {position.id[:8]} ticker fetch returned invalid price, "
                        f"deferring close to next cycle"
                    )
                    return stats
                # Validate price is within 50% of entry (sanity check against stale/corrupt data)
                entry_price = safe_float(position.entry_price, 0.0)
                if entry_price > 0 and (exit_price < entry_price * 0.5 or exit_price > entry_price * 2.0):
                    logger.warning(
                        f"[P0-FIX] Ghost position {position.id[:8]} exit price ${exit_price} too far from "
                        f"entry ${entry_price}, deferring close to next cycle"
                    )
                    return stats
            except Exception as e:
                logger.warning(
                    f"[P0-FIX] Ghost position {position.id[:8]} ticker fetch failed: {e}, "
                    f"deferring close to next cycle"
                )
                return stats

            await record_position_close_trade_async(
                session=session,
                position=position,
                exit_price=exit_price,
                close_reason="ghost_position_auto_close",
                order_status="ghost_closed",
                order_details={
                    "fail_count": ghost_entry["fail_count"],
                    "dynamic_threshold": dynamic_threshold,
                    "position_value_usdt": position_value,
                    "missing_elapsed_secs": missing_elapsed,
                },
            )
            _GHOST_POSITION_TRACKER.pop(position.id, None)
            _save_ghost_tracker()
            stats["closed"] += 1

            logger.warning(
                f"[PositionMonitor] Ghost-closed {position.ticker} in DB but left reduce-only "
                f"protective orders untouched. This avoids creating a naked exchange position "
                f"if the exchange API later reports the position again."
            )

            return stats

        ticker = await get_ticker(position.ticker, exchange_config)
        mark_price = safe_float(ticker.get("last") or position.last_price)
        if mark_price > 0:
            _update_unrealized(position, mark_price)
            stats["updated"] += 1
        return stats

    try:
        from exchange import fetch_single_position
        residual_position = await fetch_single_position(position.ticker, checked_exchange_config)
    except Exception as exc:
        logger.warning(
            f"[PositionMonitor] Close order found for {position.ticker}, but residual position "
            f"verification failed: {exc}. Keeping DB position open and protection active."
        )
        ticker = await get_ticker(position.ticker, exchange_config)
        mark_price = safe_float(ticker.get("last") or position.last_price)
        if mark_price > 0:
            _update_unrealized(position, mark_price)
            stats["updated"] += 1
        return stats

    if (
        residual_position is not None
        and _exchange_position_side_matches_position(position, residual_position)
        and _exchange_position_contracts(residual_position) > 0
    ):
        contracts = _sync_open_position_from_exchange(position, residual_position)
        logger.error(
            f"[PositionMonitor] CRITICAL: Close order detected for {position.ticker}, "
            f"but exchange still reports {contracts} contracts. Keeping position OPEN "
            f"and preserving/rebuilding protection."
        )
        if await _verify_protective_orders(session, position, exchange_config):
            stats["adjusted"] += 1
            await session.flush()
        stats["updated"] += 1
        return stats

    exit_price = safe_float((order or {}).get("average") or (order or {}).get("price"))
    close_reason = _close_reason_for_order(position, order)

    if exit_price <= 0:
        ticker = await get_ticker(position.ticker, exchange_config)
        exit_price = safe_float(ticker.get("last") or position.last_price or position.entry_price)
        close_reason = "exchange_closed_unmatched"

    # P0-FIX: If we still can't get an exit price, use entry_price as absolute fallback
    # to prevent positions staying "open" forever when price data is unavailable
    if exit_price <= 0:
        entry_fallback = safe_float(position.entry_price)
        if entry_fallback > 0:
            logger.warning(
                f"[P0-CRITICAL] No exit price available for {position.ticker}, "
                f"using entry price ${entry_fallback} as fallback to prevent orphan position"
            )
            exit_price = entry_fallback
            close_reason = "exchange_closed_unmatched"

    if exit_price > 0:
        await record_position_close_trade_async(
            session=session,
            position=position,
            exit_price=exit_price,
            close_reason=close_reason,
            order_status="exchange_closed",
            order_details=order or {"trigger": close_reason},
        )
        stats["closed"] += 1

        # P0-FIX: Cancel leftover protective orders (SL/TP) when closing a position.
        # On hedge-mode exchanges, leftover orders can block margin or create unwanted fills.
        try:
            from exchange import cancel_order
            tp_order_ids = loads_list(position.take_profit_order_ids_json)
            for order_id in tp_order_ids:
                if order_id:
                    try:
                        await cancel_order(str(order_id), position.ticker, exchange_config)
                    except Exception:
                        pass  # Best-effort cancel
            if position.stop_loss_order_id:
                try:
                    await cancel_order(str(position.stop_loss_order_id), position.ticker, exchange_config)
                except Exception:
                    pass  # Best-effort cancel
        except Exception as cancel_exc:
            logger.debug(f"[PositionMonitor] Best-effort order cancel skipped for {position.ticker}: {cancel_exc}")

    return stats


def _detect_tp_hits_from_orders(position: PositionModel, orders: list[dict]) -> list[dict]:
    tp_order_ids = {str(order_id) for order_id in loads_list(position.take_profit_order_ids_json) if order_id}
    tp_levels = loads_list(position.take_profit_json)
    for level in tp_levels:
        if isinstance(level, dict) and level.get("order_id"):
            tp_order_ids.add(str(level.get("order_id")))
    hit_levels = []
    total_filled_qty_pct = 0.0

    for order in orders:
        order_id = str(order.get("id") or "")
        if order_id not in tp_order_ids:
            continue
        if str(order.get("status") or "").lower() not in {"closed", "filled"}:
            continue

        order_price = safe_float(order.get("average") or order.get("price"))
        order_filled_qty = safe_float(order.get("filled") or 0)
        order_remaining_qty = safe_float(order.get("remaining") or 0)

        for i, level in enumerate(tp_levels):
            level_price = safe_float(level.get("price"))
            level_status = str(level.get("status") or "pending").lower()
            if level_status in {"hit", "filled", "closed"}:
                continue
            if level_price > 0 and (abs(order_price - level_price) / level_price < 0.001 or order_id in tp_order_ids):
                level_qty_pct = safe_float(level.get("qty_pct"), 100.0)

                hit_info = {
                    "level": i + 1,
                    "price": level_price,
                    "qty_pct": level_qty_pct,
                    "status": "hit",
                    "order_id": order_id,
                    "filled_qty": order_filled_qty,
                    "remaining_qty": order_remaining_qty,
                }
                hit_levels.append(hit_info)

                if order_filled_qty > 0 and safe_float(position.quantity) > 0:
                    actual_filled_pct = (order_filled_qty / safe_float(position.quantity)) * 100
                    hit_info["actual_filled_pct"] = actual_filled_pct
                    total_filled_qty_pct += actual_filled_pct

                level["status"] = "hit"
                level["hit_at"] = utcnow().isoformat()
                level["filled_qty"] = order_filled_qty
                break

    if hit_levels:
        position.take_profit_json = json.dumps(tp_levels, ensure_ascii=False, default=str)

        opened_qty = safe_float(position.quantity)
        current_remaining = _effective_remaining_quantity(position, opened_qty)
        if total_filled_qty_pct > 0 and current_remaining > 0:
            filled_qty_total = (total_filled_qty_pct / 100.0) * safe_float(position.quantity)
            new_remaining = max(0, current_remaining - filled_qty_total)
            position.remaining_quantity = new_remaining

            entry_price = safe_float(position.entry_price)
            leverage = safe_float(position.leverage, 1.0)
            if entry_price > 0:
                for hit in hit_levels:
                    tp_price = safe_float(hit.get("price"))
                    if tp_price > 0:
                        tp_pnl_pct = _price_pnl_pct(position.direction, entry_price, tp_price, leverage)
                        weight = safe_float(hit.get("actual_filled_pct") or hit.get("qty_pct"), 100.0) / 100.0
                        position.realized_pnl_pct = round(safe_float(position.realized_pnl_pct) + (tp_pnl_pct * weight), 6)

            logger.info(
                f"[PositionMonitor] TP partial fills: {total_filled_qty_pct:.1f}% filled, "
                f"remaining_qty updated from {current_remaining} to {new_remaining}, "
                f"realized_pnl_pct={position.realized_pnl_pct:.2f}%"
            )

    return hit_levels


def _find_exchange_position(position: PositionModel, exchange_positions: list[dict]) -> dict | None:
    target = _symbol_key(position.ticker)
    direction = str(position.direction or "").lower()
    SIDE_LONG = {"long", "buy"}
    SIDE_SHORT = {"short", "sell"}
    for item in exchange_positions:
        symbol = _symbol_key(item.get("symbol"))
        side = str(item.get("side") or "").lower()
        if target != symbol:
            continue
        if direction and side:
            if direction in SIDE_LONG and side not in SIDE_LONG:
                continue
            if direction in SIDE_SHORT and side not in SIDE_SHORT:
                continue
        return item
    return None


async def _find_recent_close_order(position: PositionModel, exchange_config: dict, get_recent_orders) -> dict | None:
    orders = await get_recent_orders(position.ticker, 50, exchange_config)
    tp_order_ids = [str(order_id or "") for order_id in loads_list(position.take_profit_order_ids_json)]
    tp_levels = loads_list(position.take_profit_json)
    order_ids = set()
    for index in range(max(len(tp_order_ids), len(tp_levels))):
        level = tp_levels[index] if index < len(tp_levels) and isinstance(tp_levels[index], dict) else {}
        order_id = str(level.get("order_id") or (tp_order_ids[index] if index < len(tp_order_ids) else "") or "")
        if not order_id:
            continue
        level_status = str(level.get("status") or "pending").lower()
        if level_status not in {"hit", "filled", "closed"}:
            order_ids.add(order_id)
    if position.stop_loss_order_id:
        order_ids.add(position.stop_loss_order_id)

    remaining_qty = safe_float(position.remaining_quantity or position.quantity)

    for order in orders:
        if str(order.get("id") or "") in order_ids and _order_has_close_status(order):
            filled_qty = safe_float(order.get("filled") or 0)
            if filled_qty <= 0:
                continue
            if remaining_qty > 0:
                filled_pct = (filled_qty / remaining_qty) * 100
                if filled_pct >= 90:
                    return order
                else:
                    continue
            return order
    if order_ids:
        return None

    for order in orders:
        if _order_matches_position_close(position, order):
            filled_qty = safe_float(order.get("filled") or 0)
            if filled_qty <= 0:
                continue
            if remaining_qty > 0:
                filled_pct = (filled_qty / remaining_qty) * 100
                if filled_pct >= 90:
                    return order
                else:
                    continue
            return order
    return None


def _order_matches_position_close(position: PositionModel, order: dict) -> bool:
    if not _order_has_close_status(order):
        return False

    if not _symbols_match(position.ticker, order.get("symbol")):
        return False

    order_side = str(order.get("side") or "").lower()
    expected_side = "sell" if str(position.direction).lower() == "long" else "buy"
    if not order_side or order_side != expected_side:
        return False

    order_ts = safe_float(order.get("timestamp"))
    opened_at = position.opened_at
    if order_ts <= 0 or not opened_at:
        return False
    if opened_at.tzinfo is None:
        opened_at = opened_at.replace(tzinfo=timezone.utc)
    opened_ms = opened_at.timestamp() * 1000
    if order_ts < opened_ms:
        return False

    return True


def _order_has_close_status(order: dict) -> bool:
    status = str(order.get("status") or "").lower()
    filled = safe_float(order.get("filled") or 0)
    return status in {"closed", "filled"} or (status == "partial" and filled > 0)


def _symbols_match(left: str, right: str) -> bool:
    left_key = _symbol_key(left)
    right_key = _symbol_key(right)
    return bool(left_key and right_key and left_key == right_key)


def _close_reason_for_order(position: PositionModel, order: dict | None) -> str:
    if not order:
        return "exchange_closed_unmatched"
    order_id = str(order.get("id") or "")
    if position.stop_loss_order_id and order_id == position.stop_loss_order_id:
        return "stop_loss"
    tp_order_ids = {str(tp_order_id) for tp_order_id in loads_list(position.take_profit_order_ids_json) if tp_order_id}
    for level in loads_list(position.take_profit_json):
        if isinstance(level, dict) and level.get("order_id"):
            tp_order_ids.add(str(level.get("order_id")))
    if order_id in tp_order_ids:
        return "take_profit"
    return "exchange_closed"


def _update_unrealized(position: PositionModel, mark_price: float) -> None:
    opened_qty = max(safe_float(position.quantity), 0.0)
    remaining_qty = _effective_remaining_quantity(position, opened_qty)
    leverage = safe_float(position.leverage, 1.0)
    open_pnl = _price_pnl_pct(position.direction, position.entry_price, mark_price, leverage)
    entry_price = safe_float(position.entry_price)
    remaining_weight = remaining_qty / opened_qty if opened_qty > 0 else 1.0
    # Get contract_size from trailing_stop_config
    ts_config = loads_dict(position.trailing_stop_config_json)
    contract_size = safe_float(ts_config.get("_contract_size"), 1.0)
    if entry_price > 0 and remaining_qty > 0:
        # Margin = (entry_price * remaining_qty * contract_size) / leverage
        margin_used = (entry_price * remaining_qty * contract_size) / leverage
        position.unrealized_pnl_usdt = round(margin_used * (open_pnl / 100.0), 8)
    else:
        position.unrealized_pnl_usdt = 0.0
    position.last_price = mark_price
    position.current_pnl_pct = round(safe_float(position.realized_pnl_pct) + open_pnl * remaining_weight, 6)
    position.updated_at = utcnow()


async def _maybe_adjust_trailing_stop(position: PositionModel, exchange_config: dict, exchange_position: dict, place_protective_stop) -> bool:
    trailing_config = loads_dict(position.trailing_stop_config_json)
    trailing_mode = _resolve_trailing_mode(trailing_config, position)

    if trailing_mode == "none":
        return False

    mark_price = safe_float(exchange_position.get("mark_price") or exchange_position.get("markPrice"))
    if mark_price <= 0:
        return False

    direction = str(position.direction or "long").lower()
    entry_price = safe_float(position.entry_price)
    leverage = max(1.0, safe_float(position.leverage, 1.0))
    current_stop = safe_float(position.stop_loss)
    remaining_qty = _effective_remaining_quantity(position, safe_float(position.quantity))

    new_stop = None

    if trailing_mode == "moving":
        trail_pct = safe_float(first_valid(trailing_config.get("trail_pct"), settings.trailing_stop.trail_pct), 1.5)
        activation_pct = safe_float(
            first_valid(trailing_config.get("activation_profit_pct"), settings.trailing_stop.activation_profit_pct),
            0.5,
        )
        profit_pct = _price_pnl_pct(direction, entry_price, mark_price, leverage)
        if profit_pct < activation_pct:
            return False
        if direction == "short":
            new_stop = mark_price * (1 + trail_pct / 100.0)
        else:
            new_stop = mark_price * (1 - trail_pct / 100.0)

    elif trailing_mode == "profit_pct_trailing":
        activation_pct = safe_float(
            first_valid(trailing_config.get("activation_profit_pct"), settings.trailing_stop.activation_profit_pct),
            1.0,
        )
        trail_pct = safe_float(first_valid(trailing_config.get("trail_pct"), settings.trailing_stop.trail_pct), 0.5)
        profit_pct = _price_pnl_pct(direction, entry_price, mark_price, leverage)
        if profit_pct < activation_pct:
            return False
        if direction == "short":
            new_stop = mark_price * (1 + trail_pct / 100.0)
        else:
            new_stop = mark_price * (1 - trail_pct / 100.0)

    if new_stop is None or new_stop <= 0:
        return False

    if current_stop > 0:
        if direction == "short" and new_stop >= current_stop:
            return False
        if direction != "short" and new_stop <= current_stop:
            return False

    result = await place_protective_stop(
        ticker=position.ticker,
        direction=position.direction,
        quantity=remaining_qty,
        stop_price=new_stop,
        exchange_config=exchange_config,
        existing_order_id=position.stop_loss_order_id or None,
    )
    if result.get("status") in {"placed", "simulated"}:
        position.stop_loss = new_stop
        position.stop_loss_order_id = str(result.get("order_id") or position.stop_loss_order_id or "")
        position.updated_at = utcnow()
        logger.info(f"[PositionMonitor] Adjusted trailing stop for {position.ticker}: mode={trailing_mode}, new_stop={new_stop:.8f}")
        return True
    return False


async def _adjust_trailing_stop_on_tp_hit(
    position: PositionModel,
    tp_levels: list[dict],
    hit_levels: list[dict],
    exchange_config: dict,
    place_protective_stop,
    trailing_history: list[dict] | None = None,
) -> bool:
    """
    Adjust trailing stop when TP levels are hit.

    FIXED BUG: Correct step_trailing logic:
    - TP1 hit -> SL at entry + buffer
    - TP2 hit -> SL at TP1 + buffer
    - TP3 hit -> SL at TP2 + buffer
    - TP4 hit -> SL at TP3 + buffer

    FIXED BUG: Prevent duplicate triggers by checking current SL position.
    """
    from models import TrailingStopHistory  # noqa: F401 - Used for type annotation in future

    trailing_config = loads_dict(position.trailing_stop_config_json)
    trailing_mode = _resolve_trailing_mode(trailing_config, position)

    if trailing_mode not in {"breakeven_on_tp1", "step_trailing"}:
        return False

    if not hit_levels:
        return False

    direction = str(position.direction or "long").lower()
    entry_price = safe_float(position.entry_price)
    current_stop = safe_float(position.stop_loss)
    remaining_qty = _effective_remaining_quantity(position, safe_float(position.quantity))

    # Get buffer percentages from config
    breakeven_buffer = safe_float(trailing_config.get("breakeven_buffer_pct"), 0.2)
    step_buffer = safe_float(trailing_config.get("step_buffer_pct"), 0.3)

    new_stop = None
    tp_note = ""
    trigger_type = ""
    profit_locked_pct = 0.0

    # Sort TP levels by distance from entry (closest first)
    reverse_sort = direction == "short"
    all_levels = sorted(tp_levels, key=lambda x: safe_float(x.get("price")), reverse=reverse_sort)

    # Determine which TP levels have been hit
    hit_level_numbers = []
    for i, level in enumerate(all_levels):
        status = str(level.get("status") or "pending").lower()
        if status in {"hit", "filled", "closed"}:
            hit_level_numbers.append(i + 1)

    if not hit_level_numbers:
        return False

    highest_hit = max(hit_level_numbers)

    if trailing_mode == "breakeven_on_tp1":
        # Only trigger on TP1 hit
        if highest_hit >= 1:
            # Check if already at breakeven (avoid duplicate trigger)
            breakeven_target = entry_price * (1 + breakeven_buffer / 100.0) if direction == "short" else entry_price * (1 - breakeven_buffer / 100.0)
            if current_stop > 0:
                # Already moved to breakeven?
                if direction == "long" and current_stop >= entry_price * 0.998:
                    return False  # Already at/below entry (breakeven already set)
                if direction == "short" and current_stop <= entry_price * 1.002:
                    return False  # Already at/above entry (breakeven already set)

            new_stop = breakeven_target
            tp_note = f"TP1 hit — SL moved to breakeven + {breakeven_buffer}% buffer"
            trigger_type = "tp1_hit"

            # Calculate profit locked
            tp1_price = safe_float(all_levels[0].get("price"))
            tp1_qty = safe_float(all_levels[0].get("qty_pct"), 25.0)
            if tp1_price > 0 and entry_price > 0:
                profit_pct = abs(tp1_price - entry_price) / entry_price * 100
                profit_locked_pct = profit_pct * tp1_qty / 100.0

    elif trailing_mode == "step_trailing":
        # Progressive profit locking
        if highest_hit == 1:
            # TP1 hit -> move to breakeven + buffer
            breakeven_target = entry_price * (1 + breakeven_buffer / 100.0) if direction == "short" else entry_price * (1 - breakeven_buffer / 100.0)

            # Check if already at breakeven
            if current_stop > 0:
                if direction == "long" and current_stop >= entry_price * 0.998:
                    return False
                if direction == "short" and current_stop <= entry_price * 1.002:
                    return False

            new_stop = breakeven_target
            tp_note = f"TP1 hit — SL moved to breakeven + {breakeven_buffer}% buffer"
            trigger_type = "tp1_hit"

            # Calculate profit locked from TP1
            tp1_price = safe_float(all_levels[0].get("price"))
            tp1_qty = safe_float(all_levels[0].get("qty_pct"), 25.0)
            if tp1_price > 0 and entry_price > 0:
                profit_pct = abs(tp1_price - entry_price) / entry_price * 100
                profit_locked_pct = profit_pct * tp1_qty / 100.0

        elif highest_hit >= 2:
            # TP(n) hit -> move SL to TP(n-1) + buffer
            # highest_hit=2 (TP2 hit) -> prev_level_idx=0 (TP1)
            prev_level_idx = highest_hit - 2
            if prev_level_idx < len(all_levels):
                prev_tp_price = safe_float(all_levels[prev_level_idx].get("price"))
                if prev_tp_price > 0:
                    # Add buffer above TP(n-1) for short, below for long
                    target_with_buffer = prev_tp_price * (1 + step_buffer / 100.0) if direction == "short" else prev_tp_price * (1 - step_buffer / 100.0)

                    # Check if already at or beyond this level
                    if current_stop > 0:
                        if direction == "long" and current_stop >= target_with_buffer * 0.998:
                            return False  # Already at/beyond this TP level
                        if direction == "short" and current_stop <= target_with_buffer * 1.002:
                            return False

                    new_stop = target_with_buffer
                    tp_note = f"TP{highest_hit} hit — SL moved to TP{highest_hit - 1} + {step_buffer}% buffer"
                    trigger_type = f"tp{highest_hit}_hit"

                    # Calculate cumulative profit locked
                    profit_locked_pct = 0.0
                    for i in range(highest_hit):
                        tp_price = safe_float(all_levels[i].get("price"))
                        tp_qty = safe_float(all_levels[i].get("qty_pct"), 25.0)
                        if tp_price > 0 and entry_price > 0:
                            profit_pct = abs(tp_price - entry_price) / entry_price * 100
                            profit_locked_pct += profit_pct * tp_qty / 100.0

    if new_stop is None or new_stop <= 0:
        return False

    # Validate new stop is better than current
    if current_stop > 0:
        if direction == "short" and new_stop >= current_stop:
            return False
        if direction != "short" and new_stop <= current_stop:
            return False

    result = await place_protective_stop(
        ticker=position.ticker,
        direction=position.direction,
        quantity=remaining_qty,
        stop_price=new_stop,
        exchange_config=exchange_config,
        existing_order_id=position.stop_loss_order_id or None,
    )

    if result.get("status") in {"placed", "simulated"}:
        position.stop_loss = new_stop
        position.stop_loss_order_id = str(result.get("order_id") or position.stop_loss_order_id or "")
        position.updated_at = utcnow()

        # Record trailing stop history
        history_entry = {
            "position_id": str(position.id or ""),
            "trigger_type": trigger_type,
            "old_sl": current_stop,
            "new_sl": new_stop,
            "trigger_price": safe_float(all_levels[highest_hit - 1].get("price")) if highest_hit <= len(all_levels) else 0.0,
            "profit_locked_pct": profit_locked_pct,
            "timestamp": utcnow().isoformat(),
            "success": True,
            "reasoning": tp_note,
        }
        if trailing_history is not None:
            trailing_history.append(history_entry)

        logger.info(f"[PositionMonitor] {tp_note} for {position.ticker}: new_stop={new_stop:.8f}, profit_locked={profit_locked_pct:.2f}%")
        return True

    return False


async def check_position_risk(position: dict, config: dict) -> dict:
    """Check basic risk metrics for a position dict."""
    entry_price = safe_float(position.get("entryPrice") or position.get("entry_price"))
    mark_price = safe_float(position.get("markPrice") or position.get("mark_price"))
    liquidation_price = safe_float(position.get("liquidationPrice") or position.get("liquidation_price"))
    leverage = safe_float(position.get("leverage"), 1.0)

    if not entry_price or not mark_price:
        return {"risk_level": "unknown"}

    side = str(position.get("side") or "long").lower()
    pnl_pct = _price_pnl_pct(side, entry_price, mark_price, leverage)

    liq_distance = 0.0
    if liquidation_price > 0:
        if side == "long":
            liq_distance = ((mark_price - liquidation_price) / mark_price) * 100
        else:
            liq_distance = ((liquidation_price - mark_price) / mark_price) * 100

    risk_level = "low"
    warnings = []
    if liq_distance and liq_distance < 5:
        risk_level = "critical"
        warnings.append(f"Liquidation within {liq_distance:.1f}%")
    elif liq_distance and liq_distance < 10:
        risk_level = "high"
        warnings.append(f"Liquidation within {liq_distance:.1f}%")
    elif pnl_pct < -5:
        risk_level = "high"
        warnings.append(f"Position down {abs(pnl_pct):.1f}%")
    elif leverage > 20:
        risk_level = "medium"
        warnings.append(f"High leverage: {leverage}x")

    return {
        "risk_level": risk_level,
        "pnl_pct": round(pnl_pct, 2),
        "liquidation_distance_pct": round(liq_distance, 2),
        "leverage": leverage,
        "warnings": warnings,
    }


async def _check_black_swan_event(session: AsyncSession, ticker: str, current_price: float) -> dict[str, Any]:
    """
    Detect black swan events (extreme market conditions).

    Checks for:
    - Extreme price drops (>10% in 1h)
    - Exchange halts/suspensions
    - Liquidation cascades
    - Funding rate extremes

    Returns dict with event status and recommended actions.
    """
    from enhanced_market_data import fetch_fear_greed_index, fetch_liquidation_heatmap

    result = {
        "is_black_swan": False,
        "severity": "none",
        "reasons": [],
        "recommended_action": "continue",
        "should_close_positions": False,
        "should_pause_trading": False,
    }

    reasons = []

    # Check Fear & Greed - extreme fear indicates panic
    fg_data = await fetch_fear_greed_index()
    fg_value = fg_data.get("value", 50)
    if fg_value <= 10:
        reasons.append(f"Extreme Fear (FGI={fg_value})")
        result["severity"] = "critical"

    # Check liquidation heatmap for cascades
    liq_data = await fetch_liquidation_heatmap(ticker)
    liq_volume = liq_data.get("total_liquidation_volume_24h", 0)
    if liq_volume > 500_000_000:  # > $500M liquidations
        reasons.append(f"Massive liquidations (${liq_volume/1e6:.0f}M)")
        result["severity"] = "critical"

    # Check recent trades for price crashes (would need to fetch)
    # For now, use simple price check

    if len(reasons) >= 2:
        result["is_black_swan"] = True
        result["reasons"] = reasons
        result["should_close_positions"] = result["severity"] == "critical"
        result["should_pause_trading"] = True
        result["recommended_action"] = "close_all_positions"

        logger.warning(
            f"[PositionMonitor] BLACK SWAN DETECTED for {ticker}: "
            f"severity={result['severity']}, reasons={reasons}"
        )

    return result


async def _adjust_sl_for_volatility(
    position: PositionModel,
    exchange_config: dict,
    current_atr_pct: float,
    place_protective_stop,
) -> bool:
    """
    Dynamically adjust stop loss when volatility spikes.

    When ATR increases significantly, widen SL to avoid premature stops.
    This prevents getting stopped out during normal volatility expansions.

    Returns True if SL was adjusted.
    """
    entry_price = safe_float(position.entry_price)
    current_stop = safe_float(position.stop_loss)
    original_atr_pct = safe_float(getattr(position, "original_atr_pct", None), 0.0)
    if original_atr_pct <= 0:
        # Fallback: estimate from entry price (assume 2% default ATR)
        original_atr_pct = 2.0

    if entry_price <= 0 or current_stop <= 0 or current_atr_pct <= 0:
        return False

    # Calculate volatility ratio (current vs original)
    volatility_ratio = current_atr_pct / original_atr_pct if original_atr_pct > 0 else 1.0

    # Only adjust if volatility has increased significantly (>2x)
    if volatility_ratio < 2.0:
        return False

    direction = str(position.direction or "long").lower()
    remaining_qty = _effective_remaining_quantity(position, safe_float(position.quantity))

    # Calculate new SL based on increased volatility
    # widen by proportion of volatility increase
    sl_distance_pct = abs(current_stop - entry_price) / entry_price * 100
    new_sl_distance_pct = sl_distance_pct * min(2.0, volatility_ratio / 2)

    if direction == "long":
        new_stop = entry_price * (1 - new_sl_distance_pct / 100.0)
        # Only accept if new stop is wider (lower = more room before stop)
        if new_stop >= current_stop:
            return False
    else:
        new_stop = entry_price * (1 + new_sl_distance_pct / 100.0)
        # Only accept if new stop is wider (higher = more room before stop)
        if new_stop <= current_stop:
            return False

    # Place new stop
    result = await place_protective_stop(
        ticker=position.ticker,
        direction=position.direction,
        quantity=remaining_qty,
        stop_price=new_stop,
        exchange_config=exchange_config,
        existing_order_id=position.stop_loss_order_id or None,
    )

    if result.get("status") in {"placed", "simulated"}:
        position.stop_loss = new_stop
        position.stop_loss_order_id = str(result.get("order_id") or position.stop_loss_order_id or "")
        position.updated_at = utcnow()
        logger.info(
            f"[PositionMonitor] Volatility-adjusted SL for {position.ticker}: "
            f"old={current_stop:.4f}, new={new_stop:.4f}, vol_ratio={volatility_ratio:.2f}"
        )
        return True

    return False


async def monitor_black_swan_events(session: AsyncSession) -> dict[str, Any]:
    """
    Monitor for black swan events across all open positions.

    Smart handling:
    - Profitable positions: Enable trailing stop to protect gains, continue watching
    - Losing positions: Close immediately to limit losses

    Returns summary of detected events and actions taken.
    """
    from sqlalchemy import select

    from core.database import PositionModel

    stmt = select(PositionModel).where(
        PositionModel.status.in_(["open", "pending"])
    )
    result = await session.execute(stmt)
    positions = result.scalars().all()

    if not positions:
        return {"positions_checked": 0, "events_detected": 0}

    events_summary = {
        "positions_checked": len(positions),
        "events_detected": 0,
        "positions_closed": 0,
        "positions_trailing_enabled": 0,
        "actions": [],
    }

    tickers = {pos.ticker for pos in positions}

    for ticker in tickers:
        ticker_positions = [p for p in positions if p.ticker == ticker]

        from exchange import get_ticker
        try:
            ticker_data = await get_ticker(ticker)
            current_price = safe_float(ticker_data.get("last") or ticker_data.get("price") or 0)
        except ccxt.BaseError:
            continue
        except Exception:
            continue

        if current_price <= 0:
            continue

        swan_result = await _check_black_swan_event(session, ticker, current_price)

        if swan_result.get("is_black_swan"):
            events_summary["events_detected"] += 1
            events_summary["actions"].append({
                "ticker": ticker,
                "severity": swan_result.get("severity"),
                "reasons": swan_result.get("reasons"),
            })

            for pos in ticker_positions:
                try:
                    entry_price = safe_float(pos.entry_price)
                    pnl_pct = _price_pnl_pct(
                        str(pos.direction or "long").lower(),
                        entry_price,
                        current_price,
                        1.0,
                    )

                    if pnl_pct > 0:
                        # Profitable position: Enable aggressive trailing stop
                        await _enable_emergency_trailing_stop(
                            pos, current_price, session
                        )
                        events_summary["positions_trailing_enabled"] += 1
                        logger.warning(
                            f"[PositionMonitor] Black swan: enabled emergency trailing stop "
                            f"for profitable position {pos.id[:8]} on {ticker} "
                            f"(pnl={pnl_pct:+.2f}%)"
                        )
                        events_summary["actions"].append({
                            "position_id": pos.id[:8],
                            "ticker": ticker,
                            "action": "trailing_stop_enabled",
                            "pnl_pct": pnl_pct,
                            "reason": "Profitable during black swan - protect gains",
                        })
                    else:
                        # Losing position: Close immediately on exchange first, then DB
                        black_swan_closed_exchange = False
                        if pos.live_trading:
                            try:
                                exchange_config = await _exchange_config_for_position(session, pos, {})
                                from exchange import _get_or_create_exchange
                                exchange = _get_or_create_exchange(
                                    exchange_id=exchange_config.get("exchange") or exchange_config.get("name") or settings.exchange.name,
                                    api_key=exchange_config.get("api_key") or settings.exchange.api_key,
                                    api_secret=exchange_config.get("api_secret") or settings.exchange.api_secret,
                                    password=exchange_config.get("password") or settings.exchange.password,
                                    live=True,
                                    sandbox=bool(exchange_config.get("sandbox_mode", settings.exchange.sandbox_mode)),
                                    market_type=exchange_config.get("market_type") or settings.exchange.market_type,
                                    margin_mode=exchange_config.get("margin_mode") or settings.risk.margin_mode,
                                )
                                from exchange import _close_position as _exchange_close
                                close_result = await _exchange_close(
                                    exchange, pos.ticker,
                                    position_side=str(pos.direction).lower() if pos.direction else None,
                                )
                                if close_result.get("status") == "closed":
                                    black_swan_closed_exchange = True
                                    close_exit_price = safe_float(close_result.get("exit_price"))
                                    if close_exit_price > 0:
                                        current_price = close_exit_price
                                    logger.warning(
                                        f"[PositionMonitor] Black swan: closed {pos.id[:8]} on exchange "
                                        f"(exit_price={current_price})"
                                    )
                                else:
                                    logger.error(
                                        f"[PositionMonitor] CRITICAL: Black swan exchange close failed for "
                                        f"{pos.ticker}: {close_result.get('reason')}. "
                                        f"Position still open on exchange! Manual intervention required."
                                    )
                            except Exception as exc:
                                logger.error(
                                    f"[PositionMonitor] CRITICAL: Black swan exchange close exception for "
                                    f"{pos.ticker}: {exc}. Position still open on exchange!"
                                )
                        else:
                            black_swan_closed_exchange = True  # Paper trading always succeeds

                        if not black_swan_closed_exchange:
                            events_summary["actions"].append({
                                "position_id": pos.id[:8],
                                "ticker": ticker,
                                "action": "close_failed_position_kept_open",
                                "pnl_pct": pnl_pct,
                                "reason": "Exchange close not confirmed; keeping DB position open and protection active",
                            })
                            continue

                        try:
                            await close_position_async(
                                session=session,
                                position=pos,
                                exit_price=current_price,
                                close_reason="black_swan_loss_protection",
                            )
                        except Exception as db_exc:
                            logger.error(
                                f"[PositionMonitor] Black swan DB close failed for {pos.id[:8]}: {db_exc}. "
                                f"Exchange position closed; DB reconciliation required."
                            )
                            continue
                        events_summary["positions_closed"] += 1
                        logger.warning(
                            f"[PositionMonitor] Black swan: closed losing position "
                            f"{pos.id[:8]} on {ticker} (pnl={pnl_pct:+.2f}%)"
                        )
                        events_summary["actions"].append({
                            "position_id": pos.id[:8],
                            "ticker": ticker,
                            "action": "closed",
                            "pnl_pct": pnl_pct,
                            "reason": "Losing during black swan - limit losses",
                        })

                except (ValueError, TypeError) as e:
                    logger.error(f"[PositionMonitor] Failed to handle position: {e}")
                except Exception as e:
                    logger.error(f"[PositionMonitor] Failed to handle position: {e}")

    if events_summary["events_detected"] > 0:
        logger.warning(
            f"[PositionMonitor] Black swan handling complete: "
            f"detected={events_summary['events_detected']}, "
            f"closed={events_summary['positions_closed']}, "
            f"trailing={events_summary['positions_trailing_enabled']}"
        )

    await session.flush()
    return events_summary


async def _enable_emergency_trailing_stop(
    position: PositionModel,
    current_price: float,
    session: AsyncSession,
) -> bool:
    """
    Enable emergency trailing stop for a profitable position during black swan.

    Uses aggressive trailing (tight distance) to lock in profits while
    allowing position to continue if price keeps moving favorably.
    """
    direction = str(position.direction or "long").lower()
    entry_price = safe_float(position.entry_price)
    leverage = max(1.0, safe_float(position.leverage, 1.0))
    pnl_pct = _price_pnl_pct(direction, entry_price, current_price, leverage)

    if pnl_pct <= 0:
        return False

    # Calculate emergency trailing stop
    # Place SL at breakeven + small buffer to guarantee profit protection
    buffer_pct = min(0.5, pnl_pct * 0.3)  # 30% of profit as buffer, max 0.5%

    if direction == "long":
        emergency_sl = entry_price * (1 + buffer_pct / 100.0)
        # Move SL up to protect profit
        current_sl = safe_float(position.stop_loss)
        if current_sl > 0 and emergency_sl <= current_sl:
            emergency_sl = current_sl * (1 + 0.2 / 100.0)  # Slightly higher
    else:
        emergency_sl = entry_price * (1 + buffer_pct / 100.0)
        current_sl = safe_float(position.stop_loss)
        if current_sl > 0 and emergency_sl <= current_sl:
            emergency_sl = current_sl * (1 + 0.2 / 100.0)

    # Update position with emergency trailing config
    original_config = loads_dict(position.trailing_stop_config_json)
    emergency_config = {
        **original_config,
        "mode": "profit_pct_trailing",
        "activation_profit_pct": 0.0,  # Activate immediately
        "trail_pct": 0.5,  # Tight trailing
        "trailing_step_pct": 0.2,
        "_emergency_override_original_mode": original_config.get("mode", "none"),
    }

    new_stop_order_id = str(position.stop_loss_order_id or "")
    if position.live_trading:
        exchange_config = _get_exchange_config_for_position(position)
        if not exchange_config:
            logger.warning(f"[PositionMonitor] Cannot enable emergency trailing for {position.ticker}: missing exchange config")
            return False
        try:
            from exchange import place_protective_stop
            remaining_qty = _effective_remaining_quantity(position, safe_float(position.quantity))
            result = await place_protective_stop(
                ticker=position.ticker,
                direction=position.direction,
                quantity=remaining_qty,
                stop_price=emergency_sl,
                exchange_config=exchange_config,
                existing_order_id=position.stop_loss_order_id or None,
            )
        except Exception as e:
            logger.warning(f"[PositionMonitor] Failed to update exchange SL: {e}")
            return False
        if result.get("status") not in {"placed", "simulated"}:
            logger.warning(f"[PositionMonitor] Failed to enable emergency trailing for {position.ticker}: {result}")
            return False
        new_stop_order_id = str(result.get("order_id") or new_stop_order_id)

    position.trailing_stop_config_json = json.dumps(emergency_config)
    position.stop_loss = emergency_sl
    position.stop_loss_order_id = new_stop_order_id
    position.updated_at = utcnow()

    logger.info(
        f"[PositionMonitor] Emergency trailing stop enabled for {position.ticker}: "
        f"SL={emergency_sl:.4f}, pnl={pnl_pct:+.2f}%"
    )

    await session.flush()
    return True
