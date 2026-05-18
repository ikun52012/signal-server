"""
Signal Server - Analytics Module (Enhanced)
Performance analytics and trade statistics.
"""
import json
from collections import defaultdict
from datetime import timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database import TradeModel
from core.utils.datetime import utcnow


async def calculate_performance(
    session: AsyncSession,
    days: int = 30,
    user_id: str | None = None,
) -> dict[str, Any]:
    """
    Calculate comprehensive performance metrics.
    
    Uses mainstream quantitative finance methodology:
    - Sharpe/Sortino: Annualized based on actual trades-per-day frequency
    - Max Drawdown: Peak-to-trough percentage decline from equity high
    - Profit Factor: Gross profit / gross loss
    - Win Rate: Winning trades / total closed trades
    """
    cutoff = utcnow() - timedelta(days=days)

    # Build query
    query = select(TradeModel).where(TradeModel.timestamp >= cutoff)
    if user_id:
        query = query.where(TradeModel.user_id == user_id)

    result = await session.execute(query.order_by(TradeModel.timestamp))
    trades: list[Any] = list(result.scalars().all())

    if not trades:
        return _empty_performance()

    # Calculate metrics - only count executed trades that entered the market
    executed_trades = [t for t in trades if bool(getattr(t, "execute", False))]
    # Filter out pending/rejected orders - only count filled/closed trades
    filled_or_closed = [t for t in executed_trades if _is_filled_or_closed(t)]
    closed_trades = [t for t in filled_or_closed if _is_closed_trade(t)]
    open_trades = len(filled_or_closed) - len(closed_trades)
    total_trades = len(filled_or_closed)

    # PnL calculations
    pnls = [float(getattr(t, "pnl_pct", 0.0)) for t in closed_trades if getattr(t, "pnl_pct", None) is not None]
    total_pnl = sum(pnls) if pnls else 0

    # Win/Loss
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    breakeven = [p for p in pnls if p == 0]

    win_rate = (len(wins) / len(pnls) * 100) if pnls else 0

    # Average win/loss
    avg_win = sum(wins) / len(wins) if wins else 0
    avg_loss = sum(losses) / len(losses) if losses else 0

    # Risk/Reward
    risk_reward = abs(avg_win / avg_loss) if avg_loss != 0 else 0

    # Profit factor
    gross_profit = sum(wins) if wins else 0
    gross_loss = abs(sum(losses)) if losses else 0
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0

    # Drawdown
    equity_curve = _calculate_equity_curve(pnls)
    max_drawdown = _calculate_max_drawdown(equity_curve)

    # Sharpe Ratio (annualized, using average trades-per-day)
    if len(pnls) > 1:
        import statistics
        std = statistics.stdev(pnls) if len(pnls) > 1 else 0
        avg_pnl = sum(pnls) / len(pnls)
        # Calculate average trades per day for proper annualization
        trade_dates = set()
        for t in closed_trades:
            ts = getattr(t, 'timestamp', None)
            if ts:
                try:
                    trade_dates.add(ts.strftime("%Y-%m-%d"))
                except Exception:
                    pass
        avg_trades_per_day = len(trade_dates) / max(days, 1) if trade_dates else (len(pnls) / max(days, 1))
        annualization_factor = (avg_trades_per_day * 252) ** 0.5 if avg_trades_per_day > 0 else (252 ** 0.5)
        sharpe = (avg_pnl / std * annualization_factor) if std > 0 else 0
    else:
        sharpe = 0

    # Sortino Ratio (annualized, using same trades-per-day)
    negative_returns = [p for p in pnls if p < 0]
    if negative_returns:
        import statistics
        downside_std = statistics.stdev(negative_returns) if len(negative_returns) > 1 else abs(negative_returns[0])
        avg_pnl = sum(pnls) / len(pnls)
        sortino = (avg_pnl / downside_std * annualization_factor) if downside_std > 0 else 0
    else:
        sortino = sharpe

    # Consecutive wins/losses
    max_consec_wins, max_consec_losses = _calculate_consecutive(pnls)

    # Best/worst trades
    best_trade = max(pnls) if pnls else 0
    worst_trade = min(pnls) if pnls else 0

    # AI stats
    ai_stats = await _calculate_ai_stats(trades)

    return {
        "total_trades": total_trades,
        "executed_trades": len(executed_trades),
        "closed_trades": len(closed_trades),
        "open_trades": open_trades,
        "total_pnl_pct": round(total_pnl, 2),
        "win_rate": round(win_rate, 1),
        "winning_trades": len(wins),
        "losing_trades": len(losses),
        "breakeven_trades": len(breakeven),
        "avg_win_pct": round(avg_win, 2),
        "avg_loss_pct": round(avg_loss, 2),
        "risk_reward_ratio": round(risk_reward, 2),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown_pct": round(max_drawdown, 2),
        "sharpe_ratio": round(sharpe, 2),
        "sortino_ratio": round(sortino, 2),
        "best_trade_pct": round(best_trade, 2),
        "worst_trade_pct": round(worst_trade, 2),
        "max_consecutive_wins": max_consec_wins,
        "max_consecutive_losses": max_consec_losses,
        "equity_curve": equity_curve,
        "ai_stats": ai_stats,
    }


async def get_daily_pnl(
    session: AsyncSession,
    days: int = 30,
    user_id: str | None = None,
) -> list[dict[str, float | str]]:
    """Get daily PnL breakdown."""
    cutoff = utcnow() - timedelta(days=days)

    query = select(TradeModel).where(TradeModel.timestamp >= cutoff)
    if user_id:
        query = query.where(TradeModel.user_id == user_id)

    result = await session.execute(query)
    trades: list[Any] = list(result.scalars().all())

    # Group by day
    daily_pnl: dict[str, float] = defaultdict(float)
    for trade in trades:
        pnl_pct = getattr(trade, "pnl_pct", None)
        timestamp = getattr(trade, "timestamp", None)
        if pnl_pct is not None and timestamp is not None and _is_closed_trade(trade):
            day = timestamp.strftime("%Y-%m-%d")
            daily_pnl[day] += float(pnl_pct)

    # Fill missing days
    all_days = []
    for i in range(days):
        day = (utcnow() - timedelta(days=i)).strftime("%Y-%m-%d")
        all_days.append(day)

    return [
        {"date": day, "pnl": round(daily_pnl.get(day, 0), 2)}
        for day in sorted(all_days)
    ]


async def get_trade_distribution(
    session: AsyncSession,
    user_id: str | None = None,
) -> dict[str, Any]:
    """Get trade distribution by ticker and direction."""
    query = select(TradeModel)
    if user_id:
        query = query.where(TradeModel.user_id == user_id)

    result = await session.execute(query)
    trades: list[Any] = list(result.scalars().all())

    by_ticker: dict[str, dict[str, float | int]] = {}
    by_direction: dict[str, int] = {"long": 0, "short": 0}

    for trade in trades:
        ticker = getattr(trade, "ticker", None)
        direction_value = getattr(trade, "direction", None)
        if ticker and direction_value:
            ticker_key = str(ticker)
            direction = "long" if "long" in str(direction_value).lower() else "short"
            by_ticker.setdefault(ticker_key, {"long": 0, "short": 0, "pnl": 0.0})
            by_ticker[ticker_key][direction] += 1
            by_direction[direction] += 1
            pnl_pct = getattr(trade, "pnl_pct", None)
            if pnl_pct and _is_closed_trade(trade):
                by_ticker[ticker_key]["pnl"] += float(pnl_pct)

    return {
        "by_ticker": dict(by_ticker),
        "by_direction": by_direction,
    }


def _empty_performance() -> dict[str, Any]:
    """Return empty performance metrics."""
    return {
        "total_trades": 0,
        "executed_trades": 0,
        "closed_trades": 0,
        "open_trades": 0,
        "total_pnl_pct": 0,
        "win_rate": 0,
        "winning_trades": 0,
        "losing_trades": 0,
        "breakeven_trades": 0,
        "avg_win_pct": 0,
        "avg_loss_pct": 0,
        "risk_reward_ratio": 0,
        "profit_factor": 0,
        "max_drawdown_pct": 0,
        "sharpe_ratio": 0,
        "sortino_ratio": 0,
        "best_trade_pct": 0,
        "worst_trade_pct": 0,
        "max_consecutive_wins": 0,
        "max_consecutive_losses": 0,
        "equity_curve": [],
        "ai_stats": {},
    }


def _is_filled_or_closed(trade: Any) -> bool:
    """
    Check if trade is filled (entered market) or closed (exited market).
    Excludes pending, rejected, cancelled orders.
    """
    status = str(getattr(trade, "order_status", "") or "").lower()
    # Valid statuses: filled (entered), simulated (paper entered), closed (exited)
    valid_statuses = {
        "filled", "simulated", "closed", "paper_closed", "exchange_closed",
        "tp_hit", "sl_hit", "limit_filled"
    }
    return status in valid_statuses


def _is_closed_trade(trade: Any) -> bool:
    status = str(getattr(trade, "order_status", "") or "").lower()
    direction = str(getattr(trade, "direction", "") or "").lower()
    if direction.startswith("close_"):
        return True
    if status in {"closed", "paper_closed", "exchange_closed", "tp_hit", "sl_hit"}:
        return True
    try:
        payload = json.loads(trade.payload_json) if trade.payload_json else {}
        return payload.get("position_event") == "closed" or bool(payload.get("close_reason"))
    except (TypeError, ValueError, json.JSONDecodeError, AttributeError):
        return False
    except Exception:
        return False


def _calculate_equity_curve(pnls: list[float]) -> list[dict[str, float | int]]:
    """Calculate cumulative equity curve from trade PnL percentages.
    
    Returns a list of dicts with trade number, per-trade PnL,
    and cumulative PnL for chart rendering.
    """
    curve: list[dict[str, float | int]] = []
    cumulative = 0.0
    for i, pnl in enumerate(pnls):
        cumulative += pnl
        curve.append({
            "trade": i + 1,
            "pnl": pnl,
            "cumulative_pnl": round(cumulative, 2),
        })
    return curve


def _calculate_max_drawdown(equity_curve: list[dict[str, float | int]]) -> float:
    """Calculate maximum drawdown percentage from equity curve.
    
    Uses the standard quant formula: max(peak - trough) / peak * 100.
    Returns the peak-to-trough decline as a percentage of the peak.
    """
    if not equity_curve:
        return 0

    # Build equity series starting at 100 (like a portfolio starting at $100)
    equity = 100.0
    peak = 100.0
    max_dd_pct = 0.0

    for point in equity_curve:
        pnl_pct = point["pnl"]
        equity *= (1 + pnl_pct / 100.0)
        if equity > peak:
            peak = equity
        # Drawdown as a percentage of peak
        dd_pct = ((peak - equity) / peak) * 100 if peak > 0 else 0
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct

    return round(max_dd_pct, 2)


def _calculate_consecutive(pnls: list[float]) -> tuple[int, int]:
    """Calculate max consecutive wins and losses."""
    if not pnls:
        return 0, 0

    max_wins = 0
    max_losses = 0
    current_wins = 0
    current_losses = 0

    for pnl in pnls:
        if pnl > 0:
            current_wins += 1
            current_losses = 0
            max_wins = max(max_wins, current_wins)
        elif pnl < 0:
            current_losses += 1
            current_wins = 0
            max_losses = max(max_losses, current_losses)
        else:
            current_wins = 0
            current_losses = 0

    return max_wins, max_losses


async def _calculate_ai_stats(trades: list[Any]) -> dict[str, float | int]:
    """Calculate AI performance statistics."""
    high_conf_trades: list[float] = []
    low_conf_trades: list[float] = []
    all_confidences: list[float] = []

    for trade in trades:
        if not _is_closed_trade(trade):
            continue
        try:
            payload_raw = json.loads(getattr(trade, "payload_json", "")) if getattr(trade, "payload_json", "") else {}
            if not isinstance(payload_raw, dict):
                continue
            analysis = payload_raw.get("analysis", {})
            if not isinstance(analysis, dict):
                continue
            confidence = analysis.get("confidence")

            if confidence is not None:
                confidence_value = float(confidence)
                all_confidences.append(confidence_value)

                pnl_value = float(getattr(trade, "pnl_pct", 0.0) or 0.0)
                if confidence_value >= 0.7:
                    high_conf_trades.append(pnl_value)
                elif confidence_value < 0.5:
                    low_conf_trades.append(pnl_value)
        except (TypeError, json.JSONDecodeError, ValueError):
            pass

    def win_rate(trade_list: list[float]) -> float:
        if not trade_list:
            return 0.0
        wins = sum(1 for p in trade_list if p > 0)
        return (wins / len(trade_list)) * 100

    return {
        "high_confidence_trades": len(high_conf_trades),
        "low_confidence_trades": len(low_conf_trades),
        "high_confidence_win_rate": win_rate(high_conf_trades),
        "low_confidence_win_rate": win_rate(low_conf_trades),
        "avg_confidence": sum(all_confidences) / len(all_confidences) if all_confidences else 0,
    }


# Cache invalidation
_performance_cache: dict[str, object] = {}
_cache_time: dict[str, float] = {}


def invalidate_performance_cache(user_id: str | None = None):
    """Invalidate performance cache."""
    global _performance_cache, _cache_time
    key = user_id or "global"
    _performance_cache.pop(key, None)
    _cache_time.pop(key, None)
