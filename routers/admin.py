"""
Signal Server - Admin Router
Admin panel routes for user management, settings, and monitoring.
"""
import json
import os
import re
import secrets
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, cast

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import FileResponse, Response
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import require_admin
from core.config import settings
from core.database import (
    AdminAuditLogModel,
    AdminSettingModel,
    InviteCodeModel,
    PaymentModel,
    RedeemCodeModel,
    SubscriptionModel,
    SubscriptionPlanModel,
    UserModel,
    WebhookEventModel,
    deactivate_user_subscriptions,
    get_admin_setting,
    get_all_users,
    get_db,
    get_user_by_id,
    set_admin_setting,
    update_user_password_hash,
)
from core.request_utils import client_ip as get_client_ip
from core.request_utils import public_base_url
from core.security import (
    generate_webhook_secret,
    hash_password,
    is_placeholder_webhook_secret,
    validate_password_strength,
)
from core.utils.datetime import to_utc, utcnow

router = APIRouter(prefix="/api/admin", tags=["admin"])


# ─────────────────────────────────────────────
# Request Models
# ─────────────────────────────────────────────

class CreateUserRequest(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    email: str = Field(min_length=5, max_length=254)
    password: str = Field(min_length=8, max_length=256)
    role: str = Field(default="user")
    balance_usdt: float = Field(default=0)
    live_trading_allowed: bool = Field(default=False)
    max_leverage: int = Field(default=20, ge=1, le=125)
    max_position_pct: float = Field(default=10.0, ge=0.1, le=100)


class UpdateUserRequest(BaseModel):
    username: str = Field(min_length=3, max_length=32)
    email: str = Field(min_length=5, max_length=254)
    role: str = Field(default="user")
    is_active: bool = Field(default=True)
    balance_usdt: float = Field(default=0)
    live_trading_allowed: bool = Field(default=False)
    max_leverage: int = Field(default=20, ge=1, le=125)
    max_position_pct: float = Field(default=10.0, ge=0.1, le=100)


class CreatePlanRequest(BaseModel):
    name: str = Field(min_length=1, max_length=100)
    description: str = Field(default="")
    price_usdt: float = Field(ge=0)
    duration_days: int = Field(ge=1)
    features: list[str] = Field(default_factory=list)
    max_signals_per_day: int = Field(default=0)
    is_active: bool = Field(default=True)


class CreateInviteCodeRequest(BaseModel):
    code: str = Field(default="", max_length=80)
    max_uses: int = Field(default=1, ge=1)
    note: str = Field(default="")
    expires_days: int = Field(default=30)
    expires_at: str = Field(default="")


class CreateRedeemCodeRequest(BaseModel):
    code: str = Field(default="", max_length=80)
    plan_id: str | None = None
    duration_days: int = Field(default=0)
    balance_usdt: float = Field(default=0)
    note: str = Field(default="")
    expires_days: int = Field(default=30)
    expires_at: str = Field(default="")


class SetPasswordRequest(BaseModel):
    password: str = Field(min_length=8, max_length=256)


class GrantSubscriptionRequest(BaseModel):
    plan_id: str = Field(min_length=1, max_length=80)
    duration_days: int = Field(default=0, ge=0)
    status: str = Field(default="active", max_length=20)


class PaymentAddressRequest(BaseModel):
    network: str = Field(min_length=2, max_length=30)
    address: str = Field(min_length=1, max_length=200)
    currency: str = Field(default="USDT", min_length=2, max_length=12)


class ExternalAPIKeysRequest(BaseModel):
    whale_alert_api_key: str | None = Field(default="", max_length=100, description="Whale Alert API Key (free tier: 500/day)")
    etherscan_api_key: str | None = Field(default="", max_length=100, description="Etherscan API Key (free tier: 5 calls/sec)")
    glassnode_api_key: str | None = Field(default="", max_length=100, description="Glassnode API Key (paid)")
    cryptoquant_api_key: str | None = Field(default="", max_length=100, description="CryptoQuant API Key (paid)")


class EnhancedFiltersRequest(BaseModel):
    enhanced_filters_enabled: bool = Field(default=True, description="Enable whale/correlated assets/OI checks")
    whale_threshold_usd: float = Field(default=1_000_000, ge=100_000, description="Whale transfer threshold in USD")
    correlated_threshold_pct: float = Field(default=5.0, ge=1.0, le=20.0, description="Correlated asset change threshold %")
    oi_change_threshold_pct: float = Field(default=15.0, ge=5.0, le=50.0, description="OI change threshold %")


class RegistrationSettingsRequest(BaseModel):
    invite_required: bool = False


class TradingControlRequest(BaseModel):
    mode: str = Field(default="enabled", description="enabled, read_only, paused, emergency_stop")
    reason: str = Field(default="", max_length=500)


def _generate_code(prefix: str) -> str:
    token = secrets.token_urlsafe(9).upper().replace("-", "").replace("_", "")
    return f"{prefix}-{token[:12]}"


def _parse_expiry(expires_at: str = "", expires_days: int = 30) -> datetime | None:
    if expires_at:
        try:
            parsed = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
            normalized = cast(datetime, to_utc(parsed))
            return normalized
        except ValueError as err:
            raise HTTPException(400, "Invalid expires_at date") from err
    if expires_days:
        now = cast(datetime, utcnow())
        return now + timedelta(days=expires_days)
    return None


def _validate_role(role: str) -> str:
    role = str(role or "user").lower().strip()
    if role not in {"user", "admin"}:
        raise HTTPException(400, "Role must be user or admin")
    return role


def _validate_subscription_status(status: str) -> str:
    status = str(status or "active").lower().strip()
    if status not in {"active", "pending", "cancelled", "expired"}:
        raise HTTPException(400, "Invalid subscription status")
    return status


async def _get_user_by_username_any(db: AsyncSession, username: str) -> UserModel | None:
    result = await db.execute(
        select(UserModel).where(UserModel.username == str(username or "").lower().strip())
    )
    return result.scalar_one_or_none()


async def _get_user_by_email_any(db: AsyncSession, email: str) -> UserModel | None:
    result = await db.execute(
        select(UserModel).where(UserModel.email == str(email or "").lower().strip())
    )
    return result.scalar_one_or_none()


def _raise_if_deleted(user: UserModel) -> None:
    if user.deleted_at is not None:
        raise HTTPException(400, "User is deleted. Restore the account before changing it")


async def _admin_count(db: AsyncSession) -> int:
    result = await db.execute(
        select(func.count())
        .select_from(UserModel)
        .where(
            UserModel.role == "admin",
            UserModel.is_active.is_(True),
            UserModel.deleted_at.is_(None),
        )
    )
    return int(result.scalar() or 0)


# ─────────────────────────────────────────────
# User Management
# ─────────────────────────────────────────────

@router.get("/users")
async def list_users(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List all users with subscriptions (optimized batch query)."""

    users = await get_all_users(db)

    user_ids = [u.id for u in users]

    sub_results = await db.execute(
        select(SubscriptionModel, SubscriptionPlanModel)
        .join(SubscriptionPlanModel, SubscriptionModel.plan_id == SubscriptionPlanModel.id, isouter=True)
        .where(
            SubscriptionModel.user_id.in_(user_ids),
            SubscriptionModel.status == "active",
            SubscriptionModel.end_date >= utcnow(),
        )
    )

    subscriptions_by_user = {}
    for sub, plan in sub_results.all():
        if sub.user_id not in subscriptions_by_user:
            subscriptions_by_user[sub.user_id] = {
                "id": sub.id,
                "plan_id": sub.plan_id,
                "plan_name": plan.name if plan else sub.plan_id,
                "status": sub.status,
                "end_date": sub.end_date.isoformat() if sub.end_date else None,
            }

    output = []
    for u in users:
        output.append({
            "id": u.id,
            "username": u.username,
            "email": u.email,
            "role": u.role,
            "balance_usdt": u.balance_usdt or 0,
            "is_active": u.is_active,
            "created_at": u.created_at.isoformat() if u.created_at else None,
            "last_login": u.last_login.isoformat() if u.last_login else None,
            "live_trading_allowed": u.live_trading_allowed,
            "max_leverage": u.max_leverage,
            "max_position_pct": u.max_position_pct,
            "totp_enabled": bool(getattr(u, "totp_enabled", False)),
            "subscription": subscriptions_by_user.get(u.id),
        })
    return output


@router.post("/users")
async def create_user(
    req: CreateUserRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Create a new user from admin panel."""
    from core.database import create_user as db_create_user

    # Check for existing, including soft-deleted rows that still hold unique keys.
    existing_username = await _get_user_by_username_any(db, req.username)
    if existing_username:
        detail = "Username belongs to a deleted account. Restore it before reusing this username" if existing_username.deleted_at else "Username already exists"
        raise HTTPException(400, detail)
    existing_email = await _get_user_by_email_any(db, req.email)
    if existing_email:
        detail = "Email belongs to a deleted account. Restore it before reusing this email" if existing_email.deleted_at else "Email already registered"
        raise HTTPException(400, detail)
    ok, reason = validate_password_strength(req.password, username=req.username, email=req.email)
    if not ok:
        raise HTTPException(400, reason)
    role = _validate_role(req.role)

    # Create user
    pw_hash = hash_password(req.password)
    user = await db_create_user(
        db,
        req.username,
        req.email,
        pw_hash,
        role,
    )

    # Update additional fields
    user.balance_usdt = req.balance_usdt
    user.live_trading_allowed = req.live_trading_allowed
    user.max_leverage = req.max_leverage
    user.max_position_pct = req.max_position_pct

    await db.commit()

    # Audit log
    await _add_audit_log(db, admin, "create_user", "user", user.id, f"Created user {user.username}", request)

    return {"id": user.id, "username": user.username}


@router.put("/users/{user_id}")
@router.put("/user/{user_id}")
async def update_user(
    user_id: str,
    req: UpdateUserRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update a user."""
    user = await get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    _raise_if_deleted(user)

    # Check for conflicts
    if user.username != req.username:
        existing = await _get_user_by_username_any(db, req.username)
        if existing and existing.id != user.id:
            detail = "Username belongs to a deleted account. Restore it before reusing this username" if existing.deleted_at else "Username already exists"
            raise HTTPException(400, detail)

    if user.email != req.email:
        existing = await _get_user_by_email_any(db, req.email)
        if existing and existing.id != user.id:
            detail = "Email belongs to a deleted account. Restore it before reusing this email" if existing.deleted_at else "Email already registered"
            raise HTTPException(400, detail)

    new_role = _validate_role(req.role)
    is_active_admin = user.role == "admin" and bool(user.is_active)
    if is_active_admin and new_role != "admin" and await _admin_count(db) <= 1:
        raise HTTPException(400, "Cannot demote the last admin account")
    if is_active_admin and not req.is_active and await _admin_count(db) <= 1:
        raise HTTPException(400, "Cannot disable the last admin account")
    if user.id == admin.get("sub") and (new_role != "admin" or not req.is_active):
        raise HTTPException(400, "Use another admin account to change your own admin access")

    old_role = user.role
    old_active = bool(user.is_active)

    # Update fields
    user.username = req.username.lower().strip()
    user.email = req.email.lower().strip()
    user.role = new_role
    user.is_active = req.is_active
    user.balance_usdt = req.balance_usdt
    user.live_trading_allowed = req.live_trading_allowed
    user.max_leverage = req.max_leverage
    user.max_position_pct = req.max_position_pct

    # Bump token version if auth-relevant fields changed.
    if old_role != user.role or old_active != bool(user.is_active):
        user.token_version = (user.token_version or 0) + 1

    await db.commit()

    # Audit log
    await _add_audit_log(db, admin, "update_user", "user", user_id, f"Updated user {user.username}", request)

    return {
        "status": "ok",
        "user": {
            "id": user.id,
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "is_active": user.is_active,
            "balance_usdt": user.balance_usdt,
            "live_trading_allowed": user.live_trading_allowed,
            "max_leverage": user.max_leverage,
            "max_position_pct": user.max_position_pct,
        },
    }


@router.delete("/users/{user_id}")
@router.delete("/user/{user_id}")
async def delete_user(
    user_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Soft delete a user (preserves historical data)."""
    user = await get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    if user.deleted_at is not None:
        raise HTTPException(400, "User already deleted")
    if user.id == admin.get("sub"):
        raise HTTPException(400, "Use another admin account to delete your own user")

    # Prevent deleting last admin
    if user.role == "admin" and bool(user.is_active):
        if await _admin_count(db) <= 1:
            raise HTTPException(400, "Cannot delete the last admin account")

    # Soft delete: set deleted_at and deactivate
    user.deleted_at = utcnow()
    user.is_active = False
    user.token_version = (user.token_version or 0) + 1

    # Invalidate active subscriptions
    await db.execute(
        update(SubscriptionModel)
        .where(SubscriptionModel.user_id == user_id, SubscriptionModel.status == "active")
        .values(status="cancelled")
    )

    await db.commit()

    # Audit log
    await _add_audit_log(db, admin, "delete_user", "user", user_id, f"Soft deleted user {user.username}", request)

    return {"status": "ok", "message": "User soft deleted. Historical data preserved."}


@router.post("/user/{user_id}/restore")
async def restore_user(
    user_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Restore a soft-deleted user."""
    from core.database import restore_user as db_restore_user

    user = await get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    if user.deleted_at is None:
        raise HTTPException(400, "User is not deleted")

    success = await db_restore_user(db, user_id)
    if not success:
        raise HTTPException(500, "Failed to restore user")

    await db.commit()
    await _add_audit_log(db, admin, "restore_user", "user", user_id, f"Restored user {user.username}", request)

    return {"status": "ok", "message": f"User {user.username} restored"}


@router.post("/user/{user_id}/toggle")
async def toggle_user(
    user_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Toggle a user's active state."""
    user = await get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    _raise_if_deleted(user)
    if user.role == "admin" and (user.id == admin.get("sub") or await _admin_count(db) <= 1):
        raise HTTPException(400, "Admin accounts must be updated explicitly from another admin account")
    user.is_active = not bool(user.is_active)
    user.token_version = (user.token_version or 0) + 1
    await db.commit()
    await _add_audit_log(db, admin, "toggle_user", "user", user_id, f"Set active={user.is_active} for {user.username}", request)
    return {"status": "ok", "is_active": user.is_active}


@router.post("/user/{user_id}/password")
async def set_user_password(
    user_id: str,
    req: SetPasswordRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Set a user's password from the admin panel."""
    user = await get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    _raise_if_deleted(user)
    ok, reason = validate_password_strength(req.password, username=user.username, email=user.email)
    if not ok:
        raise HTTPException(400, reason)
    await update_user_password_hash(db, user_id, hash_password(req.password))
    await db.commit()
    await _add_audit_log(db, admin, "set_password", "user", user_id, f"Set password for {user.username}", request)
    return {"status": "ok"}


@router.post("/user/{user_id}/subscription")
async def grant_user_subscription(
    user_id: str,
    req: GrantSubscriptionRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Grant or create a subscription for a user."""
    user = await get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    _raise_if_deleted(user)
    plan = await db.get(SubscriptionPlanModel, req.plan_id)
    if not plan:
        raise HTTPException(404, "Plan not found")
    status = _validate_subscription_status(req.status)

    now = utcnow()
    duration_days = req.duration_days or plan.duration_days
    sub = SubscriptionModel(
        user_id=user_id,
        plan_id=plan.id,
        status=status,
        start_date=now if status == "active" else None,
        end_date=(now + timedelta(days=duration_days)) if status == "active" else None,
    )
    db.add(sub)
    await db.flush()
    if status == "active":
        await deactivate_user_subscriptions(db, user_id, exclude_subscription_id=sub.id)
    await db.commit()
    await _add_audit_log(db, admin, "grant_subscription", "user", user_id, f"Granted {plan.name} to {user.username}", request)
    return {"status": "ok", "subscription_id": sub.id}


@router.post("/users/{user_id}/reset-password")
async def reset_user_password(
    user_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Reset a user's password to a random value.

    The temporary password is returned in the response over HTTPS.
    Admins must deliver it to the user immediately and instruct them to change it.
    """
    user = await get_user_by_id(db, user_id)
    if not user:
        raise HTTPException(404, "User not found")
    _raise_if_deleted(user)

    new_password = secrets.token_urlsafe(12)
    pw_hash = hash_password(new_password)

    await update_user_password_hash(db, user_id, pw_hash)

    await _add_audit_log(db, admin, "reset_password", "user", user_id, f"Reset password for {user.username}", request)

    logger.info(f"[Admin] Password reset for user {user.username} by {admin['username']}")

    return Response(
        content=json.dumps({
            "status": "success",
            "message": "Password has been reset. Deliver this temporary password to the user immediately.",
            "user_id": user_id,
            "username": user.username,
            "temporary_password": new_password,
        }),
        media_type="application/json",
        headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
    )


# ─────────────────────────────────────────────
# Subscription Plans
# ─────────────────────────────────────────────

@router.get("/plans")
async def list_plans(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List all subscription plans."""
    result = await db.execute(select(SubscriptionPlanModel))
    plans = result.scalars().all()
    return [
        {
            "id": p.id,
            "name": p.name,
            "description": p.description,
            "price_usdt": p.price_usdt,
            "duration_days": p.duration_days,
            "features": json.loads(p.features_json) if p.features_json else [],
            "is_active": p.is_active,
            "max_signals_per_day": p.max_signals_per_day,
        }
        for p in plans
    ]


@router.post("/plans")
async def create_plan(
    req: CreatePlanRequest,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Create a subscription plan."""
    plan = SubscriptionPlanModel(
        name=req.name,
        description=req.description,
        price_usdt=req.price_usdt,
        duration_days=req.duration_days,
        features_json=json.dumps(req.features),
        max_signals_per_day=req.max_signals_per_day,
        is_active=req.is_active,
    )
    db.add(plan)
    await db.commit()
    return {"id": plan.id, "name": plan.name}


@router.put("/plans/{plan_id}")
async def update_plan(
    plan_id: str,
    req: CreatePlanRequest,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update a subscription plan."""
    result = await db.execute(
        select(SubscriptionPlanModel).where(SubscriptionPlanModel.id == plan_id)
    )
    plan = result.scalar_one_or_none()
    if not plan:
        raise HTTPException(404, "Plan not found")

    plan.name = req.name
    plan.description = req.description
    plan.price_usdt = req.price_usdt
    plan.duration_days = req.duration_days
    plan.features_json = json.dumps(req.features)
    plan.max_signals_per_day = req.max_signals_per_day
    plan.is_active = req.is_active

    await db.commit()
    return {"status": "ok"}


@router.delete("/plans/{plan_id}")
async def delete_plan(
    plan_id: str,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Delete an unused plan, or deactivate it if historical rows reference it."""
    plan = await db.get(SubscriptionPlanModel, plan_id)
    if not plan:
        raise HTTPException(404, "Plan not found")

    sub_count = await db.scalar(
        select(func.count()).select_from(SubscriptionModel).where(SubscriptionModel.plan_id == plan_id)
    )
    redeem_count = await db.scalar(
        select(func.count()).select_from(RedeemCodeModel).where(RedeemCodeModel.plan_id == plan_id)
    )
    if int(sub_count or 0) or int(redeem_count or 0):
        plan.is_active = False
        result = {"status": "deactivated", "reason": "Plan has historical subscriptions or card codes"}
    else:
        await db.delete(plan)
        result = {"status": "deleted"}
    await db.commit()
    return result


# ─────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────

@router.get("/settings")
async def get_settings(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get admin settings."""
    result = await db.execute(select(AdminSettingModel))
    settings_list = result.scalars().all()
    return {s.key: s.value for s in settings_list}


@router.put("/settings")
async def update_settings(
    settings_data: dict,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update admin settings.

    Includes validation for critical settings to prevent accidental misconfiguration.
    """
    if not isinstance(settings_data, dict):
        raise HTTPException(400, "Settings data must be a JSON object")

    MAX_SETTINGS_KEYS = 100
    if len(settings_data) > MAX_SETTINGS_KEYS:
        raise HTTPException(400, f"Too many settings keys (max {MAX_SETTINGS_KEYS})")

    MAX_KEY_LENGTH = 100
    MAX_VALUE_LENGTH = 10_000
    SAFE_KEY_PATTERN = re.compile(r"^[a-zA-Z0-9_\-\.]+$")

    # Protected settings that require special handling
    PROTECTED_KEYS = {"webhook_secret", "app_encryption_key"}

    # Validate critical risk settings
    RISK_SETTING_RANGES = {
        "max_daily_loss_pct": (0.1, 50.0),  # Min 0.1%, Max 50%
        "max_correlated_exposure_pct": (10.0, 200.0),  # Min 10%, Max 200%
        "risk_per_trade_pct": (0.1, 25.0),  # Min 0.1%, Max 25%
        "max_position_pct": (1.0, 100.0),  # Min 1%, Max 100%
    }

    for key, value in settings_data.items():
        if not isinstance(key, str):
            raise HTTPException(400, f"Setting key must be a string: {key!r}")
        if len(key) > MAX_KEY_LENGTH:
            raise HTTPException(400, f"Setting key too long (max {MAX_KEY_LENGTH} chars): {key[:50]}...")
        if not SAFE_KEY_PATTERN.match(key):
            raise HTTPException(400, f"Setting key contains invalid characters: {key!r}")

        # Block protected settings from bulk update
        if key in PROTECTED_KEYS:
            raise HTTPException(
                400,
                f"Setting '{key}' cannot be updated via bulk settings. Use the dedicated endpoint."
            )

        # Validate risk setting ranges
        if key in RISK_SETTING_RANGES:
            try:
                num_value = float(value)
                min_val, max_val = RISK_SETTING_RANGES[key]
                if num_value < min_val or num_value > max_val:
                    raise HTTPException(
                        400,
                        f"Setting '{key}' must be between {min_val} and {max_val}, got {num_value}"
                    )
            except (TypeError, ValueError) as exc:
                raise HTTPException(400, f"Setting '{key}' must be a number, got {value!r}") from exc

        value_str = str(value)
        if len(value_str) > MAX_VALUE_LENGTH:
            raise HTTPException(400, f"Setting value too long (max {MAX_VALUE_LENGTH} chars): {key}")
        await set_admin_setting(db, key, value_str)

    await db.commit()

    # Audit log
    await _add_audit_log(db, admin, "update_settings", "settings", "", f"Updated {len(settings_data)} admin settings", request)

    return {"status": "ok"}


@router.post("/reload-config")
async def reload_config(
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Hot-reload configuration from database without restarting the server."""
    from core.hot_reload import reload_settings_from_db

    try:
        changed = await reload_settings_from_db(db)
        await _add_audit_log(db, admin, "reload_config", "settings", "", f"Reloaded {len(changed)} setting(s)", request)
        return {
            "status": "ok",
            "reloaded": len(changed),
            "changed": {k: {"old": str(v[0]), "new": str(v[1])} for k, v in changed.items()},
        }
    except Exception as exc:
        logger.error(f"[Admin] Config reload failed: {exc}")
        raise HTTPException(500, f"Config reload failed: {exc}") from exc


@router.get("/webhook-config")
async def get_webhook_config(
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get webhook configuration."""
    secret = await get_admin_setting(db, "webhook_secret", "")

    if is_placeholder_webhook_secret(secret):
        secret = generate_webhook_secret()
        await set_admin_setting(db, "webhook_secret", secret)
        await db.commit()

    # Build template
    base_url = public_base_url(request) if request else ""
    template = json.dumps({
        "secret": secret,
        "ticker": "{{ticker}}",
        "exchange": "{{exchange}}",
        "direction": "long",
        "price": "{{close}}",
        "timeframe": "{{interval}}",
        "strategy": "{{strategy.order.comment}}",
        "message": "{{strategy.order.action}} {{ticker}} @ {{close}}",
    }, indent=2)

    return {
        "webhook_url": f"{base_url}/webhook",
        "secret": secret,
        "template": template,
    }


# ─────────────────────────────────────────────
# Webhook Events
# ─────────────────────────────────────────────

@router.get("/webhook-events")
async def list_webhook_events(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List recent webhook events."""
    result = await db.execute(
        select(WebhookEventModel)
        .order_by(WebhookEventModel.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    events = result.scalars().all()
    return [
        {
            "id": e.id,
            "user_id": e.user_id,
            "ticker": e.ticker,
            "direction": e.direction,
            "status": e.status,
            "status_code": e.status_code,
            "reason": e.reason,
            "client_ip": e.client_ip,
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }
        for e in events
    ]


# ─────────────────────────────────────────────
# Audit Logs
# ─────────────────────────────────────────────

@router.get("/audit-logs")
async def list_audit_logs(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List admin audit logs."""
    result = await db.execute(
        select(AdminAuditLogModel)
        .order_by(AdminAuditLogModel.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    logs = result.scalars().all()
    return [
        {
            "id": log_entry.id,
            "admin_id": log_entry.admin_id,
            "admin_username": log_entry.admin_username,
            "action": log_entry.action,
            "target_type": log_entry.target_type,
            "target_id": log_entry.target_id,
            "summary": log_entry.summary,
            "client_ip": log_entry.client_ip,
            "created_at": log_entry.created_at.isoformat() if log_entry.created_at else None,
        }
        for log_entry in logs
    ]


# ─────────────────────────────────────────────
# Invite Codes
# ─────────────────────────────────────────────

@router.get("/invite-codes")
async def list_invite_codes(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List all invite codes."""
    result = await db.execute(select(InviteCodeModel))
    codes = result.scalars().all()
    return [
        {
            "code": c.code,
            "note": c.note,
            "max_uses": c.max_uses,
            "used_count": c.used_count,
            "is_active": c.is_active,
            "expires_at": c.expires_at.isoformat() if c.expires_at else None,
        }
        for c in codes
    ]


@router.post("/invite-codes")
async def create_invite_code(
    req: CreateInviteCodeRequest,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Create an invite code."""
    code_value = (req.code or _generate_code("INV")).upper().strip()
    if await db.get(InviteCodeModel, code_value):
        raise HTTPException(400, "Invite code already exists")

    code = InviteCodeModel(
        code=code_value,
        note=req.note,
        max_uses=req.max_uses,
        expires_at=_parse_expiry(req.expires_at, req.expires_days),
        created_by=admin.get("sub"),
    )
    db.add(code)
    await db.commit()
    return {"code": code.code}


@router.get("/registration")
async def get_registration_settings(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    invite_required = await get_admin_setting(db, "registration_invite_required", "false")
    return {"invite_required": invite_required.lower() == "true"}


@router.post("/registration")
async def save_registration_settings(
    req: RegistrationSettingsRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    await set_admin_setting(db, "registration_invite_required", "true" if req.invite_required else "false")
    await db.commit()
    await _add_audit_log(db, admin, "update_registration", "settings", "", f"Invite required={req.invite_required}", request)
    return {"status": "ok", "invite_required": req.invite_required}


@router.get("/redeem-codes")
async def list_redeem_codes(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(RedeemCodeModel, SubscriptionPlanModel, UserModel)
        .join(SubscriptionPlanModel, RedeemCodeModel.plan_id == SubscriptionPlanModel.id, isouter=True)
        .join(UserModel, RedeemCodeModel.redeemed_by == UserModel.id, isouter=True)
        .order_by(RedeemCodeModel.created_at.desc())
    )
    return [
        {
            "code": code.code,
            "plan_id": code.plan_id,
            "plan_name": plan.name if plan else "",
            "duration_days": code.duration_days,
            "balance_usdt": code.balance_usdt,
            "note": code.note,
            "is_active": code.is_active,
            "redeemed_by": code.redeemed_by,
            "redeemed_by_username": redeemed.username if redeemed else "",
            "redeemed_at": code.redeemed_at.isoformat() if code.redeemed_at else None,
            "expires_at": code.expires_at.isoformat() if code.expires_at else None,
        }
        for code, plan, redeemed in result.all()
    ]


@router.post("/redeem-codes")
async def create_redeem_code(
    req: CreateRedeemCodeRequest,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    if req.plan_id:
        plan = await db.get(SubscriptionPlanModel, req.plan_id)
        if not plan:
            raise HTTPException(404, "Plan not found")
    if not req.plan_id and req.balance_usdt <= 0:
        raise HTTPException(400, "Choose a plan or balance amount")
    code_value = (req.code or _generate_code("CARD")).upper().strip()
    if await db.get(RedeemCodeModel, code_value):
        raise HTTPException(400, "Redeem code already exists")
    code = RedeemCodeModel(
        code=code_value,
        plan_id=req.plan_id or None,
        duration_days=req.duration_days,
        balance_usdt=req.balance_usdt,
        note=req.note,
        expires_at=_parse_expiry(req.expires_at, req.expires_days),
        created_by=admin.get("sub"),
    )
    db.add(code)
    await db.commit()
    return {"code": code.code}


@router.get("/payment-addresses")
async def list_payment_addresses(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    from payment import SUPPORTED_NETWORKS, get_payment_address

    addresses = {}
    for network, info in SUPPORTED_NETWORKS.items():
        address = await get_payment_address(db, info["currency"], network)
        addresses[network] = {"network": network, "currency": info["currency"], "address": address or ""}
    return addresses


@router.post("/payment-addresses")
async def save_payment_address(
    req: PaymentAddressRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    from payment import set_payment_address

    network = req.network.upper().strip()
    currency = req.currency.upper().strip()
    await set_payment_address(db, currency, network, req.address.strip())
    await _add_audit_log(db, admin, "save_payment_address", "payment", network, f"Updated {currency}/{network} address", request)
    return {"status": "ok"}


@router.get("/payments")
async def list_admin_payments(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(PaymentModel, UserModel)
        .join(UserModel, PaymentModel.user_id == UserModel.id, isouter=True)
        .order_by(PaymentModel.created_at.desc())
    )
    return [
        {
            "id": p.id,
            "user_id": p.user_id,
            "username": u.username if u else "",
            "subscription_id": p.subscription_id,
            "amount": p.amount,
            "currency": p.currency,
            "network": p.network,
            "tx_hash": p.tx_hash,
            "wallet_address": p.wallet_address,
            "status": p.status,
            "created_at": p.created_at.isoformat() if p.created_at else None,
            "confirmed_at": p.confirmed_at.isoformat() if p.confirmed_at else None,
            "expires_at": p.expires_at.isoformat() if p.expires_at else None,
        }
        for p, u in result.all()
    ]


@router.post("/payment/{payment_id}/confirm")
async def confirm_payment(
    payment_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    payment = await db.get(PaymentModel, payment_id)
    if not payment:
        raise HTTPException(404, "Payment not found")
    if payment.status == "confirmed":
        return {"status": "confirmed"}
    if payment.status == "rejected":
        raise HTTPException(400, "Rejected payments cannot be confirmed")
    await _activate_payment_subscription(db, payment)
    await db.commit()
    await _add_audit_log(db, admin, "confirm_payment", "payment", payment_id, f"Confirmed payment {payment_id}", request)
    return {"status": "confirmed"}


@router.post("/payment/{payment_id}/reject")
async def reject_payment(
    payment_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    payment = await db.get(PaymentModel, payment_id)
    if not payment:
        raise HTTPException(404, "Payment not found")
    if payment.status == "confirmed":
        raise HTTPException(400, "Confirmed payments cannot be rejected")
    payment.status = "rejected"
    await db.commit()
    await _add_audit_log(db, admin, "reject_payment", "payment", payment_id, f"Rejected payment {payment_id}", request)
    return {"status": "rejected"}


@router.post("/payment/{payment_id}/verify")
async def verify_payment(
    payment_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    payment = await db.get(PaymentModel, payment_id)
    if not payment:
        raise HTTPException(404, "Payment not found")
    if payment.status == "confirmed":
        return {"status": "confirmed", "verification": {"verified": True, "status": "confirmed"}}
    if payment.status == "rejected":
        raise HTTPException(400, "Rejected payments cannot be verified")
    if not payment.tx_hash:
        raise HTTPException(400, "Payment has no transaction hash")

    from chain_verify import verify_payment_tx

    verification = await verify_payment_tx(
        tx_hash=payment.tx_hash,
        network=payment.network,
        expected_amount=payment.amount,
        expected_address=payment.wallet_address,
    )
    if verification.get("verified"):
        await _activate_payment_subscription(db, payment)
        await db.commit()
        status = "confirmed"
    else:
        status = verification.get("status", "pending")
    await _add_audit_log(db, admin, "verify_payment", "payment", payment_id, f"Verification status={status}", request)
    return {"status": status, "verification": verification}


@router.get("/system")
async def get_system_status(
    request: Request,
    admin: dict = Depends(require_admin),
):
    """Return lightweight system/admin diagnostics for the admin dashboard."""
    data_dir = Path(__file__).resolve().parent.parent / "data"
    logs_dir = Path(__file__).resolve().parent.parent / "logs"
    return {
        "version": settings.app_version,
        "commit": "",
        "webhook_url": f"{public_base_url(request)}/webhook",
        "live_trading": settings.exchange.live_trading,
        "exchange_sandbox_mode": settings.exchange.sandbox_mode,
        "storage": {
            "data": {"path": str(data_dir), "writable": os_access_writable(data_dir)},
            "logs": {"path": str(logs_dir), "writable": os_access_writable(logs_dir)},
        },
    }


@router.get("/ai-costs")
async def get_ai_costs(admin: dict = Depends(require_admin)):
    """Get AI API usage and cost summary."""
    from core.ai_cost_tracker import ai_costs
    return {
        "summary": ai_costs.get_summary(),
        "recent": ai_costs.get_recent(50),
    }


@router.get("/trading-controls")
async def get_trading_controls(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get global trading controls and kill-switch state."""
    from core.trading_control import get_trading_control_state

    return await get_trading_control_state(db)


@router.post("/trading-controls")
async def update_trading_controls(
    req: TradingControlRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update global trading mode."""
    from core.trading_control import set_trading_control_state

    state = await set_trading_control_state(
        db,
        mode=req.mode,
        reason=req.reason,
        updated_by=admin.get("username") or admin.get("sub") or "",
    )
    await _add_audit_log(
        db,
        admin,
        "update_trading_controls",
        "settings",
        "trading_controls",
        f"Set trading mode to {state['mode']}",
        request,
    )
    await db.commit()
    return state


@router.post("/trading-controls/emergency-stop")
async def emergency_stop_trading(
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Immediately block all new trade execution."""
    from core.trading_control import set_trading_control_state

    state = await set_trading_control_state(
        db,
        mode="emergency_stop",
        reason="Emergency stop activated from admin console",
        updated_by=admin.get("username") or admin.get("sub") or "",
    )
    await _add_audit_log(db, admin, "emergency_stop", "settings", "trading_controls", "Activated emergency stop", request)
    await db.commit()
    return state


@router.post("/trading-controls/resume")
async def resume_trading(
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Resume new trade execution."""
    from core.trading_control import set_trading_control_state

    state = await set_trading_control_state(
        db,
        mode="enabled",
        reason="Trading resumed from admin console",
        updated_by=admin.get("username") or admin.get("sub") or "",
    )
    await _add_audit_log(db, admin, "resume_trading", "settings", "trading_controls", "Resumed trading", request)
    await db.commit()
    return state


@router.get("/order-events")
async def get_order_events(
    status: str | None = None,
    limit: int = Query(100, ge=1, le=500),
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """List recent order events for reconciliation/audit."""
    from services.order_reconciler import list_order_events

    events = await list_order_events(db, status=status, limit=limit)
    return {
        "events": [
            {
                "id": event.id,
                "user_id": event.user_id,
                "position_id": event.position_id,
                "trade_id": event.trade_id,
                "ticker": event.ticker,
                "direction": event.direction,
                "status": event.status,
                "retry_state": event.retry_state,
                "attempt_count": event.attempt_count,
                "last_error": event.last_error,
                "client_order_id": event.client_order_id,
                "exchange_order_id": event.exchange_order_id,
                "next_retry_at": event.next_retry_at.isoformat() if event.next_retry_at else None,
                "created_at": event.created_at.isoformat() if event.created_at else None,
                "updated_at": event.updated_at.isoformat() if event.updated_at else None,
            }
            for event in events
        ],
        "count": len(events),
    }


@router.post("/order-events/reconcile")
async def reconcile_order_events(
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Promote stale retryable order events into manual review."""
    from services.order_reconciler import run_order_reconciliation

    result = await run_order_reconciliation(db)
    await _add_audit_log(
        db,
        admin,
        "reconcile_order_events",
        "order_event",
        "",
        f"Checked {result.get('checked', 0)} order events",
        request,
    )
    await db.commit()
    return result


@router.post("/order-events/{event_id}/approve")
async def approve_order_event(
    event_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Approve a manual review order event for re-execution."""
    from services.order_reconciler import approve_order_event as _approve
    body = await request.json() if request.body else {}
    admin_notes = body.get("admin_notes", "")
    result = await _approve(db, event_id, admin_notes)
    await _add_audit_log(
        db, admin, "approve_order_event", "order_event", event_id,
        f"Approved order event {event_id}", request,
    )
    await db.commit()
    return result


@router.post("/order-events/{event_id}/reject")
async def reject_order_event(
    event_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Reject a manual review order event permanently."""
    from services.order_reconciler import reject_order_event as _reject
    body = await request.json() if request.body else {}
    admin_notes = body.get("admin_notes", "")
    result = await _reject(db, event_id, admin_notes)
    await _add_audit_log(
        db, admin, "reject_order_event", "order_event", event_id,
        f"Rejected order event {event_id}", request,
    )
    await db.commit()
    return result


@router.post("/order-events/{event_id}/retry")
async def retry_order_event(
    event_id: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Queue a manual review order event for retry."""
    from services.order_reconciler import retry_order_event as _retry
    body = await request.json() if request.body else {}
    admin_notes = body.get("admin_notes", "")
    result = await _retry(db, event_id, admin_notes)
    await _add_audit_log(
        db, admin, "retry_order_event", "order_event", event_id,
        f"Queued order event {event_id} for retry", request,
    )
    await db.commit()
    return result


@router.get("/order-execution-settings")
async def get_order_execution_settings(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get order execution auto-approve/auto-reject settings."""
    from core.runtime_settings import get_order_execution_settings
    return await get_order_execution_settings(db)


@router.post("/order-execution-settings")
async def update_order_execution_settings(
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update order execution auto-approve/auto-reject settings."""
    from core.runtime_settings import save_order_execution_settings
    body = await request.json()
    result = await save_order_execution_settings(db, body)
    await _add_audit_log(
        db, admin, "update_order_execution_settings", "settings", "order_execution",
        "Updated order execution settings", request,
    )
    await db.commit()
    return result


@router.get("/backups")
async def get_backups(
    admin: dict = Depends(require_admin),
):
    from backups import list_backups

    backups = await list_backups()
    return [
        {
            "filename": Path(b.get("file", "")).name,
            "name": b.get("name"),
            "size": int(float(b.get("size_mb") or 0) * 1024 * 1024),
            "created_at": b.get("created_at"),
            "note": b.get("note", ""),
        }
        for b in backups
    ]


@router.post("/backups")
async def create_admin_backup(
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    from backups import create_backup

    backup = await create_backup()
    await _add_audit_log(db, admin, "create_backup", "backup", backup.get("backup_name", ""), "Created backup", request)
    return {"filename": Path(backup.get("file", "")).name, **backup}


@router.get("/backups/{filename}")
async def download_backup(
    filename: str,
    admin: dict = Depends(require_admin),
):
    from backups import backup_path

    safe_name = Path(filename).name
    target = backup_path / safe_name
    if not target.exists() or target.suffix != ".zip":
        raise HTTPException(404, "Backup not found")
    return FileResponse(target, filename=safe_name)


@router.post("/backups/{filename}/restore")
async def stage_backup_restore(
    filename: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    from backups import stage_restore

    backup_name = Path(filename).stem
    result = stage_restore(backup_name)
    if result.get("status") == "error":
        raise HTTPException(404, result.get("reason", "Backup not found"))
    await _add_audit_log(db, admin, "stage_restore", "backup", backup_name, "Staged backup restore", request)
    return {"status": "staged", "message": result.get("instructions", ""), **result}


@router.post("/backups/{filename}/restore-pg")
async def restore_postgresql_backup(
    filename: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Restore a PostgreSQL backup using pg_restore.

    SAFETY: This endpoint requires explicit confirmation and blocks if there
    are open positions. After restore, the application MUST be restarted to
    reload the restored data into memory.
    """
    from backups import restore_postgresql

    # Parse confirmation from request body
    body = await request.json() if request.method == "POST" else {}
    confirm = body.get("confirm", "").lower()
    if confirm != "restore":
        raise HTTPException(
            400,
            "PostgreSQL restore requires explicit confirmation. "
            "Send POST with body: {\"confirm\": \"restore\"}. "
            "WARNING: This will overwrite the current database. "
            "You MUST restart the application after restore."
        )

    # Check for open positions - block restore if any exist
    from sqlalchemy import func, select

    from core.database import PositionModel
    result = await db.execute(
        select(func.count(PositionModel.id)).where(PositionModel.status == "open")
    )
    open_count = result.scalar() or 0
    if open_count > 0:
        raise HTTPException(
            400,
            f"Cannot restore while {open_count} positions are open. "
            f"Close all positions before restoring."
        )

    backup_name = Path(filename).stem
    result = await restore_postgresql(backup_name)
    if result.get("status") == "error":
        raise HTTPException(400, result.get("reason", "Restore failed"))

    await _add_audit_log(db, admin, "restore_postgresql", "backup", backup_name, "Restored PostgreSQL backup", request)
    return {
        **result,
        "warning": "Database restored. You MUST restart the application to reload data into memory.",
    }


@router.get("/position-monitor")
async def get_position_monitor(
    admin: dict = Depends(require_admin),
):
    from position_monitor import get_monitor_state

    return await get_monitor_state()


@router.post("/position-monitor/run")
async def run_position_monitor(
    admin: dict = Depends(require_admin),
):
    from position_monitor import run_position_monitor_once

    return await run_position_monitor_once({})


# ─────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────

def os_access_writable(path: Path) -> bool:
    try:
        path.mkdir(parents=True, exist_ok=True)
        return os.access(path, os.W_OK)
    except (OSError, PermissionError):
        return False
    except Exception:
        return False


async def _activate_payment_subscription(db: AsyncSession, payment: PaymentModel) -> None:
    now = utcnow()
    payment.status = "confirmed"
    payment.confirmed_at = now
    if payment.subscription_id:
        subscription = await db.get(SubscriptionModel, payment.subscription_id)
        if subscription:
            plan = await db.get(SubscriptionPlanModel, subscription.plan_id)
            duration_days = plan.duration_days if plan else 30
            subscription.status = "active"
            subscription.start_date = now
            subscription.end_date = now + timedelta(days=duration_days)
            await db.flush()
            await deactivate_user_subscriptions(db, subscription.user_id, exclude_subscription_id=subscription.id)

async def _add_audit_log(
    db: AsyncSession,
    admin: dict,
    action: str,
    target_type: str,
    target_id: str,
    summary: str,
    request: Request | None,
):
    """Add an audit log entry."""
    admin_client_ip = ""
    if request:
        admin_client_ip = get_client_ip(request, default="")

    log = AdminAuditLogModel(
        admin_id=admin.get("sub"),
        admin_username=admin.get("username", ""),
        action=action,
        target_type=target_type,
        target_id=target_id,
        summary=summary,
        client_ip=admin_client_ip,
    )
    db.add(log)


# ─────────────────────────────────────────────
# Pre-Filter Thresholds & Statistics
# ─────────────────────────────────────────────

class FilterThresholdsRequest(BaseModel):
    atr_pct_max: float | None = None
    spread_pct_max: float | None = None
    volume_24h_min: float | None = None
    price_change_1h_max: float | None = None
    rsi_long_max: float | None = None
    rsi_short_min: float | None = None
    funding_rate_threshold: float | None = None
    orderbook_long_min: float | None = None
    orderbook_short_max: float | None = None
    signal_saturation_max: int | None = None
    ema_diff_pct_min: float | None = None
    consecutive_loss_max: int | None = None
    cooldown_seconds: int | None = None
    cooldown_win_multiplier: float | None = None
    cooldown_loss_multiplier: float | None = None
    price_deviation_pct_max: float | None = None
    oi_change_pct_max: float | None = None
    correlated_asset_change_max: float | None = None
    whale_threshold_usd: float | None = None
    liquidation_distance_pct_min: float | None = None
    long_short_ratio_extreme_high: float | None = None
    long_short_ratio_extreme_low: float | None = None
    basis_pct_max: float | None = None
    fear_greed_extreme_threshold: int | None = None
    cvd_divergence_threshold: float | None = None
    volatility_regime_multiplier: float | None = None
    position_reduce_on_loss_pct: float | None = None
    dynamic_cooldown_enabled: bool | None = None
    min_pass_score: float | None = None
    data_completeness_soft_fail_count: int | None = None
    max_same_direction_positions: int | None = None
    max_correlated_exposure_pct: float | None = None
    max_live_missing_data_checks: int | None = None
    block_live_on_risk_check_error: bool | None = None
    margin_mode: str | None = None


class RiskThresholdsRequest(BaseModel):
    max_same_direction_positions: int | None = None
    max_correlated_exposure_pct: float | None = None
    max_live_missing_data_checks: int | None = None
    block_live_on_risk_check_error: bool | None = None
    max_daily_trades: int | None = None
    max_daily_loss_pct: float | None = None
    max_position_pct: float | None = None
    risk_per_trade_pct: float | None = None
    margin_mode: str | None = None


@router.get("/filter-thresholds")
async def get_filter_thresholds(
    admin: dict = Depends(require_admin),
):
    """Get current pre-filter thresholds."""
    from pre_filter import FILTER_WEIGHTS, get_thresholds
    thresholds = get_thresholds()
    return {
        "thresholds": thresholds.to_dict(),
        "weights": FILTER_WEIGHTS,
        "dynamic": thresholds.DYNAMIC_THRESHOLDS,
    }


@router.post("/filter-thresholds")
async def update_filter_thresholds(
    req: FilterThresholdsRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update pre-filter thresholds."""
    from pre_filter import get_thresholds
    thresholds = get_thresholds()

    updates = {}
    for key, value in req.model_dump(exclude_none=True).items():
        thresholds.set_custom(key, value)
        updates[key] = value

    current_raw = await get_admin_setting(db, "prefilter_thresholds", "")
    try:
        persisted = json.loads(current_raw) if current_raw else {}
        if not isinstance(persisted, dict):
            persisted = {}
    except (TypeError, ValueError, json.JSONDecodeError):
        persisted = {}
    except Exception:
        persisted = {}

    persisted.update(updates)

    await set_admin_setting(db, "prefilter_thresholds", json.dumps(persisted))

    # P2-FIX: Propagate correlation risk keys to settings.risk so they take
    # effect immediately (signal_processor reads from settings.risk, not pre_filter)
    _RISK_PROPAGATE_KEYS = {"max_correlated_exposure_pct", "max_same_direction_positions", "max_live_missing_data_checks", "block_live_on_risk_check_error", "margin_mode"}
    for key in updates:
        if key in _RISK_PROPAGATE_KEYS:
            setattr(settings.risk, key, updates[key])
            await set_admin_setting(db, key, str(updates[key]))

    await db.commit()

    await _add_audit_log(db, admin, "update_filter_thresholds", "settings", "", f"Updated {len(updates)} thresholds", request)

    return {"status": "success", "updated": updates}


@router.get("/risk-thresholds")
async def get_risk_thresholds(
    admin: dict = Depends(require_admin),
):
    """Get current risk thresholds including correlation risk limits."""
    return {
        "max_same_direction_positions": settings.risk.max_same_direction_positions,
        "max_correlated_exposure_pct": settings.risk.max_correlated_exposure_pct,
        "max_live_missing_data_checks": settings.risk.max_live_missing_data_checks,
        "block_live_on_risk_check_error": settings.risk.block_live_on_risk_check_error,
        "max_daily_trades": settings.risk.max_daily_trades,
        "max_daily_loss_pct": settings.risk.max_daily_loss_pct,
        "max_position_pct": settings.risk.max_position_pct,
        "risk_per_trade_pct": settings.risk.risk_per_trade_pct,
        "margin_mode": settings.risk.margin_mode,
        "live_data_quality_mode": settings.risk.live_data_quality_mode,
    }


@router.post("/risk-thresholds")
async def update_risk_thresholds(
    req: RiskThresholdsRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update risk thresholds including correlation risk limits.

    These settings are persisted to database and can be hot-reloaded.
    """
    updates = req.model_dump(exclude_none=True)
    if not updates:
        return {"status": "success", "updated": {}}

    # Persist each setting to admin_settings table
    for key, value in updates.items():
        await set_admin_setting(db, key, str(value))
        setattr(settings.risk, key, value)

    await db.commit()
    await _add_audit_log(db, admin, "update_risk_thresholds", "settings", "", f"Updated {len(updates)} risk thresholds", request)

    return {"status": "success", "updated": updates}


@router.get("/filter-stats")
async def get_filter_statistics(
    admin: dict = Depends(require_admin),
):
    """Get pre-filter blocking statistics."""
    from pre_filter import get_filter_stats
    stats = get_filter_stats()

    summary = {}
    for check_name, ticker_counts in stats.items():
        total = sum(ticker_counts.values())
        summary[check_name] = {
            "total_blocks": total,
            "top_tickers": sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:5],
        }

    return {"statistics": stats, "summary": summary}


@router.post("/filter-stats/reset")
async def reset_filter_statistics(
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Reset pre-filter blocking statistics."""
    from pre_filter import reset_filter_stats
    reset_filter_stats()

    await _add_audit_log(db, admin, "reset_filter_stats", "settings", "", "Reset filter statistics", request)

    return {"status": "success", "message": "Filter statistics reset"}


# ─────────────────────────────────────────────
# External API Keys Management
# ─────────────────────────────────────────────

EXTERNAL_API_KEYS_SETTING = "external_api_keys"
ENHANCED_FILTERS_SETTING = "enhanced_filters"


@router.get("/external-api-keys")
async def get_external_api_keys(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get external API key configurations (masked for security)."""
    from core.database import get_admin_setting

    raw = await get_admin_setting(db, EXTERNAL_API_KEYS_SETTING, "")

    keys = {}
    if raw:
        try:
            from core.security import decrypt_settings_payload
            data = json.loads(raw)
            decrypted = decrypt_settings_payload(data)
            if isinstance(decrypted, dict):
                keys = decrypted
        except Exception as e:
            logger.debug(f"[Admin] Failed to decrypt external API keys: {e}")

    def mask_key(key: str) -> str:
        if not key or len(key) < 8:
            return ""
        return key[:4] + "..." + key[-4:]

    return {
        "whale_alert_api_key": mask_key(keys.get("whale_alert_api_key", "")),
        "whale_alert_configured": bool(keys.get("whale_alert_api_key")),
        "etherscan_api_key": mask_key(keys.get("etherscan_api_key", "")),
        "etherscan_configured": bool(keys.get("etherscan_api_key")),
        "glassnode_api_key": mask_key(keys.get("glassnode_api_key", "")),
        "glassnode_configured": bool(keys.get("glassnode_api_key")),
        "cryptoquant_api_key": mask_key(keys.get("cryptoquant_api_key", "")),
        "cryptoquant_configured": bool(keys.get("cryptoquant_api_key")),
        "description": {
            "whale_alert": "Free tier: 500 requests/day. Tracks large crypto transfers.",
            "etherscan": "Free tier: 5 calls/sec. Tracks ETH/USDT on-chain transactions.",
            "glassnode": "Paid. Comprehensive on-chain analytics (exchange flows, holder metrics).",
            "cryptoquant": "Paid. Exchange reserve tracking and market indicators.",
        },
    }


@router.post("/external-api-keys")
async def update_external_api_keys(
    req: ExternalAPIKeysRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update external API key configurations."""
    from core.database import get_admin_setting, set_admin_setting

    # Decrypt existing keys
    raw = await get_admin_setting(db, EXTERNAL_API_KEYS_SETTING, "")

    existing = {}
    if raw:
        try:
            from core.security import decrypt_settings_payload
            data = json.loads(raw)
            decrypted = decrypt_settings_payload(data)
            if isinstance(decrypted, dict):
                existing = decrypted
        except Exception as e:
            logger.debug(f"[Admin] Failed to decrypt existing external API keys: {e}")

    # Update with new values (only non-empty)
    updated = {}
    for key_name in ["whale_alert_api_key", "etherscan_api_key", "glassnode_api_key", "cryptoquant_api_key"]:
        new_value = getattr(req, key_name, None)
        if new_value and new_value.strip():
            updated[key_name] = new_value.strip()
        elif existing.get(key_name):
            updated[key_name] = existing[key_name]

    # Encrypt and save
    from core.security import encrypt_settings_payload
    encrypted = encrypt_settings_payload(updated)
    await set_admin_setting(db, EXTERNAL_API_KEYS_SETTING, json.dumps(encrypted))
    await db.commit()

    # Update in-memory secure storage (NOT environment variables)
    from core.security import set_secure_api_key
    for key_name, key_value in updated.items():
        set_secure_api_key(key_name, key_value)

    await _add_audit_log(db, admin, "update_external_api_keys", "settings", "", f"Updated {len(updated)} API keys", request)

    return {"status": "success", "message": "External API keys updated", "configured_keys": list(updated.keys())}


@router.delete("/external-api-keys/{key_name}")
async def delete_external_api_key(
    key_name: str,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Delete a specific external API key."""
    from core.database import get_admin_setting, set_admin_setting
    from core.security import clear_secure_api_key, decrypt_settings_payload, encrypt_settings_payload

    valid_keys = {"whale_alert_api_key", "etherscan_api_key", "glassnode_api_key", "cryptoquant_api_key"}
    if key_name not in valid_keys:
        raise HTTPException(400, f"Invalid key name: {key_name}")

    raw = await get_admin_setting(db, EXTERNAL_API_KEYS_SETTING, "")

    existing = {}
    if raw:
        try:
            data = json.loads(raw)
            decrypted = decrypt_settings_payload(data)
            if isinstance(decrypted, dict):
                existing = decrypted
        except Exception as e:
            logger.debug(f"[Admin] Failed to decrypt keys for deletion: {e}")

    if key_name in existing:
        del existing[key_name]

    encrypted = encrypt_settings_payload(existing)
    await set_admin_setting(db, EXTERNAL_API_KEYS_SETTING, json.dumps(encrypted))
    await db.commit()

    # Clear from in-memory secure storage
    clear_secure_api_key(key_name)

    await _add_audit_log(db, admin, "delete_external_api_key", "settings", key_name, f"Deleted {key_name}", request)

    return {"status": "success", "message": f"{key_name} deleted"}


# ─────────────────────────────────────────────
# Enhanced Filters Settings
# ─────────────────────────────────────────────

@router.get("/enhanced-filters")
async def get_enhanced_filters_settings(
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Get enhanced filter settings (whale, correlated assets, OI)."""
    from core.database import get_admin_setting

    raw = await get_admin_setting(db, ENHANCED_FILTERS_SETTING, "")

    settings_data = {
        "enhanced_filters_enabled": True,
        "whale_threshold_usd": 1_000_000,
        "correlated_threshold_pct": 5.0,
        "oi_change_threshold_pct": 15.0,
    }

    if raw:
        try:
            loaded = json.loads(raw)
            if isinstance(loaded, dict):
                settings_data.update(loaded)
        except Exception as e:
            logger.debug(f"[Admin] Failed to parse enhanced filters settings: {e}")

    return {
        **settings_data,
        "description": {
            "enhanced_filters_enabled": "Enable whale activity, correlated assets, and OI change checks",
            "whale_threshold_usd": "Minimum USD value to consider as whale transfer (default $1M)",
            "correlated_threshold_pct": "BTC/ETH change % threshold for correlated check (default 5%)",
            "oi_change_threshold_pct": "Open Interest change % threshold (default 15%)",
        },
    }


@router.post("/enhanced-filters")
async def update_enhanced_filters_settings(
    req: EnhancedFiltersRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Update enhanced filter settings."""
    from core.database import set_admin_setting

    settings_data = {
        "enhanced_filters_enabled": req.enhanced_filters_enabled,
        "whale_threshold_usd": req.whale_threshold_usd,
        "correlated_threshold_pct": req.correlated_threshold_pct,
        "oi_change_threshold_pct": req.oi_change_threshold_pct,
    }

    await set_admin_setting(db, ENHANCED_FILTERS_SETTING, json.dumps(settings_data))
    await db.commit()

    # Update runtime settings
    os.environ["ENHANCED_FILTERS_ENABLED"] = str(req.enhanced_filters_enabled).lower()

    # Update pre_filter thresholds
    from pre_filter import get_thresholds
    thresholds = get_thresholds()
    thresholds.set_custom("oi_change_pct_max", req.oi_change_threshold_pct)
    thresholds.set_custom("correlated_asset_change_max", req.correlated_threshold_pct)

    await _add_audit_log(db, admin, "update_enhanced_filters", "settings", "", "Updated enhanced filter settings", request)

    return {"status": "success", "message": "Enhanced filters updated", "settings": settings_data}


# ─────────────────────────────────────────────
# Update Management
# ─────────────────────────────────────────────

GITHUB_REPO = "ikun52012/QuantPilot-AI"
GITHUB_RELEASES_API = f"https://api.github.com/repos/{GITHUB_REPO}/releases/latest"
UPDATE_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "updater"
UPDATE_REQUEST_DIR = UPDATE_DATA_DIR / "requests"
UPDATE_STATUS_DIR = UPDATE_DATA_DIR / "status"
UPDATE_HEALTH_FILE = UPDATE_STATUS_DIR / "updater-health.json"
UPDATE_HEARTBEAT_TTL_SECS = 30


class UpdateRequest(BaseModel):
    confirm: bool = Field(default=False, description="Must be true to execute update")
    backup_before_update: bool = Field(default=True, description="Create backup before update")


def _ensure_update_dirs() -> None:
    UPDATE_REQUEST_DIR.mkdir(parents=True, exist_ok=True)
    UPDATE_STATUS_DIR.mkdir(parents=True, exist_ok=True)


def _update_control_enabled() -> bool:
    return os.getenv("AUTO_UPDATE_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def _parse_iso_timestamp(value: str) -> datetime | None:
    if not value:
        return None
    try:
        normalized = cast(datetime, to_utc(datetime.fromisoformat(value.replace("Z", "+00:00"))))
        return normalized
    except ValueError:
        return None


def _now_iso() -> str:
    now = cast(datetime, utcnow())
    return now.isoformat().replace("+00:00", "Z")


def _read_update_payload(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else None
    except (OSError, json.JSONDecodeError):
        return None


def _write_update_payload(path: Path, payload: dict[str, Any]) -> None:
    _ensure_update_dirs()
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _status_file(task_id: str) -> Path:
    return UPDATE_STATUS_DIR / f"{task_id}.json"


def _request_file(task_id: str) -> Path:
    return UPDATE_REQUEST_DIR / f"{task_id}.json"


def _version_tuple(value: str) -> tuple[int, int, int]:
    parts = [int(part) for part in re.findall(r"\d+", value or "")[:3]]
    while len(parts) < 3:
        parts.append(0)
    return (parts[0], parts[1], parts[2])


def _docker_image_for_version(version: str, updater: bool = False) -> str:
    repo = f"{GITHUB_REPO.lower()}-updater" if updater else GITHUB_REPO.lower()
    normalized = str(version or "").strip().lstrip("v")
    if not normalized:
        raise ValueError("version is required for docker image tag")
    return f"ghcr.io/{repo}:v{normalized}"


def _next_update_task_id() -> str:
    return f"upd_{utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"


def _updater_health() -> dict:
    health = _read_update_payload(UPDATE_HEALTH_FILE) or {}
    last_seen = _parse_iso_timestamp(str(health.get("updated_at", "")))
    healthy = False
    if last_seen is not None:
        healthy = (utcnow() - last_seen).total_seconds() <= UPDATE_HEARTBEAT_TTL_SECS
    return {
        **health,
        "healthy": healthy,
    }


def _update_supported() -> bool:
    health = _updater_health()
    return _update_control_enabled() and bool(health.get("healthy"))


def _latest_update_task() -> dict | None:
    _ensure_update_dirs()
    status_files = sorted(UPDATE_STATUS_DIR.glob("upd_*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
    for path in status_files:
        payload = _read_update_payload(path)
        if payload:
            return payload
    return None


def _active_update_task() -> dict | None:
    _ensure_update_dirs()
    status_files = sorted(UPDATE_STATUS_DIR.glob("upd_*.json"), key=lambda item: item.stat().st_mtime, reverse=True)
    for path in status_files:
        payload = _read_update_payload(path)
        if payload and payload.get("status") in {"queued", "running"}:
            return payload
    return None


async def _fetch_latest_release_data() -> dict:
    import httpx

    current_version = settings.app_version

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                GITHUB_RELEASES_API,
                headers={"Accept": "application/vnd.github.v3+json"},
            )

        if response.status_code == 404:
            return {
                "status": "error",
                "message": "No published GitHub release found",
                "current_version": current_version,
                "latest_version": current_version,
                "has_update": False,
            }

        if response.status_code != 200:
            return {
                "status": "error",
                "message": f"GitHub API returned {response.status_code}",
                "current_version": current_version,
                "latest_version": current_version,
                "has_update": False,
            }

        latest_release = response.json()
        if not isinstance(latest_release, dict):
            return {
                "status": "error",
                "message": "Unexpected GitHub response",
                "current_version": current_version,
                "latest_version": current_version,
                "has_update": False,
            }

        latest_version = str(latest_release.get("tag_name") or "").strip().lstrip("v")
        if not latest_version:
            return {
                "status": "error",
                "message": "Latest release tag is missing",
                "current_version": current_version,
                "latest_version": current_version,
                "has_update": False,
            }

        has_update = _version_tuple(latest_version) > _version_tuple(current_version)

        return {
            "status": "success",
            "current_version": current_version,
            "latest_version": latest_version,
            "has_update": has_update,
            "release_url": latest_release.get("html_url", ""),
            "release_name": latest_release.get("name", latest_version),
            "release_body": latest_release.get("body", ""),
            "published_at": latest_release.get("published_at", ""),
            "download_url": f"https://github.com/{GITHUB_REPO}/releases/tag/v{latest_version}",
            "docker_image": _docker_image_for_version(latest_version),
            "updater_image": _docker_image_for_version(latest_version, updater=True),
        }
    except httpx.TimeoutException:
        return {
            "status": "error",
            "message": "GitHub API timeout",
            "current_version": current_version,
            "latest_version": current_version,
            "has_update": False,
        }
    except Exception as e:
        logger.error(f"[Admin] Check update failed: {e}")
        return {
            "status": "error",
            "message": str(e),
            "current_version": current_version,
            "latest_version": current_version,
            "has_update": False,
        }


@router.get("/check-update")
async def check_for_update(
    admin: dict = Depends(require_admin),
):
    """Check GitHub Releases for latest version."""
    release = await _fetch_latest_release_data()
    return {
        **release,
        "one_click_supported": _update_supported(),
        "one_click_enabled": _update_control_enabled(),
        "updater_healthy": _updater_health().get("healthy", False),
    }


@router.post("/perform-update")
async def perform_update(
    req: UpdateRequest,
    request: Request,
    admin: dict = Depends(require_admin),
    db: AsyncSession = Depends(get_db),
):
    """Queue a one-click Docker update handled by the updater sidecar."""

    if not req.confirm:
        raise HTTPException(400, "Update must be confirmed")

    if not _update_supported():
        raise HTTPException(400, "One-click update is not available for this deployment")

    active_task = _active_update_task()
    if active_task:
        raise HTTPException(409, f"Update task {active_task.get('task_id', 'unknown')} is already {active_task.get('status', 'running')}")

    release = await _fetch_latest_release_data()
    if release.get("status") != "success":
        raise HTTPException(502, release.get("message", "Unable to check latest release"))
    if not release.get("has_update"):
        raise HTTPException(400, "No newer release is available")

    current_version = str(release.get("current_version") or settings.app_version)
    latest_version = str(release.get("latest_version") or current_version)

    backup_result = None
    if req.backup_before_update:
        from backups import create_backup

        backup_result = await create_backup(note=f"Pre-update backup before v{latest_version}")
        if backup_result.get("status") != "ok":
            raise HTTPException(500, backup_result.get("reason", "Backup failed before update"))

    task_id = _next_update_task_id()
    queued_at = _now_iso()
    task_payload = {
        "task_id": task_id,
        "status": "queued",
        "created_at": queued_at,
        "updated_at": queued_at,
        "current_version": current_version,
        "target_version": latest_version,
        "target_image": release.get("docker_image", _docker_image_for_version(latest_version)),
        "target_updater_image": release.get("updater_image", _docker_image_for_version(latest_version, updater=True)),
        "release_name": release.get("release_name", latest_version),
        "release_url": release.get("release_url", ""),
        "message": f"Update to v{latest_version} has been queued.",
        "backup": backup_result,
        "log": [f"Queued update request for v{latest_version}."],
    }

    if backup_result:
        task_payload["log"].append(f"Created backup {backup_result.get('backup_name', '')}.")

    try:
        _write_update_payload(_status_file(task_id), task_payload)
        _write_update_payload(_request_file(task_id), task_payload)
    except OSError as err:
        raise HTTPException(500, f"Failed to queue update task: {err}") from err

    await _add_audit_log(
        db,
        admin,
        "queue_update",
        "system",
        task_id,
        f"Queued update from v{current_version} to v{latest_version}",
        request,
    )
    await db.commit()

    return {
        "status": "queued",
        "message": "Update queued. The service will restart when rollout begins.",
        "task_id": task_id,
        "previous_version": current_version,
        "latest_version": latest_version,
        "backup": backup_result,
        "poll_url": f"/api/admin/update-task/{task_id}",
    }


@router.get("/update-status")
async def get_update_status(
    admin: dict = Depends(require_admin),
):
    """Get current update environment status."""
    health = _updater_health()
    latest_task = _latest_update_task()
    return {
        "deployment_mode": "docker-compose" if _update_control_enabled() else "manual",
        "current_version": settings.app_version,
        "github_repo": GITHUB_REPO,
        "one_click_enabled": _update_control_enabled(),
        "update_supported": _update_supported(),
        "updater_healthy": health.get("healthy", False),
        "updater_message": health.get("message", "Updater unavailable"),
        "updater_last_seen": health.get("updated_at"),
        "latest_task": latest_task,
    }


@router.get("/update-task/{task_id}")
async def get_update_task(
    task_id: str,
    admin: dict = Depends(require_admin),
):
    """Read the persisted state of an update task."""
    payload = _read_update_payload(_status_file(task_id))
    if payload is None:
        payload = _read_update_payload(_request_file(task_id))
    if payload is None:
        raise HTTPException(404, "Update task not found")
    return payload
