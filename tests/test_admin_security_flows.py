import pytest
from httpx import AsyncClient

from core.database import UserModel
from core.security import hash_password
from core.utils.datetime import utcnow
from routers.admin import _admin_count
from tests.test_admin_updates import _login_admin


@pytest.mark.asyncio
async def test_admin_reset_password_returns_temporary_password_securely(
    client: AsyncClient,
    db_session,
    test_admin_data,
    test_user_data,
):
    """Security: Password reset returns temporary password over HTTPS with cache-control headers."""
    headers = await _login_admin(client, db_session, test_admin_data)

    user = UserModel(
        username=test_user_data["username"].lower(),
        email=test_user_data["email"].lower(),
        password_hash=hash_password(test_user_data["password"]),
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()

    response = await client.post(f"/api/admin/users/{user.id}/reset-password", headers=headers)
    assert response.status_code == 200
    data = response.json()

    # Password MUST be returned so admin can deliver it to user
    assert "temporary_password" in data
    assert data["status"] == "success"
    assert data.get("user_id") == user.id
    assert data.get("username") == user.username
    assert len(data["temporary_password"]) >= 12  # Strong temporary password

    # Response must have cache-control headers to prevent caching
    assert "no-store" in response.headers.get("cache-control", "").lower()

    # Verify the temporary password actually works
    login = await client.post(
        "/api/auth/login",
        json={
            "username": test_user_data["username"],
            "password": data["temporary_password"],
        },
    )
    assert login.status_code == 200


@pytest.mark.asyncio
async def test_admin_count_ignores_disabled_soft_deleted_admins(
    db_session,
    test_admin_data,
):
    active_admin = UserModel(
        username=test_admin_data["username"].lower(),
        email=test_admin_data["email"].lower(),
        password_hash=hash_password(test_admin_data["password"]),
        role="admin",
        is_active=True,
    )
    deleted_admin = UserModel(
        username="deleted-admin",
        email="deleted-admin@example.com",
        password_hash=hash_password("AdminPass123!"),
        role="admin",
        is_active=False,
        deleted_at=utcnow(),
    )
    db_session.add_all([active_admin, deleted_admin])
    await db_session.commit()

    assert await _admin_count(db_session) == 1


@pytest.mark.asyncio
async def test_admin_create_user_rejects_soft_deleted_duplicate_username(
    client: AsyncClient,
    db_session,
    test_admin_data,
    test_user_data,
):
    headers = await _login_admin(client, db_session, test_admin_data)
    deleted_user = UserModel(
        username=test_user_data["username"].lower(),
        email=test_user_data["email"].lower(),
        password_hash=hash_password(test_user_data["password"]),
        role="user",
        is_active=False,
        deleted_at=utcnow(),
    )
    db_session.add(deleted_user)
    await db_session.commit()

    response = await client.post(
        "/api/admin/users",
        headers=headers,
        json={
            "username": test_user_data["username"],
            "email": "replacement@example.com",
            "password": test_user_data["password"],
            "role": "user",
            "balance_usdt": 0,
            "live_trading_allowed": False,
            "max_leverage": 20,
            "max_position_pct": 10,
        },
    )

    assert response.status_code == 400
    assert "deleted account" in response.json()["detail"].lower()
