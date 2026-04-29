"""
OAuth2 / OIDC аутентификация через Authentik.
Authorization code flow, сессия хранится в подписанной httponly-cookie.
"""
import json
import os
import secrets
import urllib.parse
import urllib.request
from typing import Optional

from fastapi import Request
from fastapi.responses import RedirectResponse
from itsdangerous import BadSignature, SignatureExpired, URLSafeTimedSerializer

AUTHENTIK_URL   = os.getenv("AUTHENTIK_URL", "").rstrip("/")
CLIENT_ID       = os.getenv("AUTHENTIK_CLIENT_ID", "")
CLIENT_SECRET   = os.getenv("AUTHENTIK_CLIENT_SECRET", "")
SESSION_SECRET  = os.getenv("SESSION_SECRET", "changeme-set-in-env")
APP_BASE_URL    = os.getenv("APP_BASE_URL", "http://localhost:62000").rstrip("/")
SLUG            = "jivo-inspector"

AUTHORIZE_URL = f"{AUTHENTIK_URL}/application/o/{SLUG}/authorize/"
TOKEN_URL     = f"{AUTHENTIK_URL}/application/o/{SLUG}/token/"
USERINFO_URL  = f"{AUTHENTIK_URL}/application/o/userinfo/"
REDIRECT_URI  = f"{APP_BASE_URL}/auth/callback"

COOKIE_NAME    = "ji_session"
COOKIE_MAX_AGE = 60 * 60 * 8  # 8 часов

_signer = URLSafeTimedSerializer(SESSION_SECRET, salt="ji-session")


# ---------------------------------------------------------------------------
# Session helpers
# ---------------------------------------------------------------------------

def make_session_cookie(data: dict) -> str:
    return _signer.dumps(data)


def read_session_cookie(value: str) -> Optional[dict]:
    try:
        return _signer.loads(value, max_age=COOKIE_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


def get_current_user(request: Request) -> Optional[dict]:
    value = request.cookies.get(COOKIE_NAME)
    if not value:
        return None
    return read_session_cookie(value)


# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------

class LoginRequired(Exception):
    """Поднимается когда пользователь не аутентифицирован."""
    def __init__(self, next_url: str = "/log"):
        self.next_url = next_url


def require_user(request: Request) -> dict:
    """
    FastAPI Depends-зависимость.
    Возвращает dict с данными пользователя или редиректит на Authentik.
    """
    user = get_current_user(request)
    if not user:
        raise LoginRequired(str(request.url))
    return user


# ---------------------------------------------------------------------------
# OAuth2 helpers
# ---------------------------------------------------------------------------

def build_authorize_url(next_url: str = "/log") -> tuple[str, str]:
    """Возвращает (url для редиректа, state-токен)."""
    state = secrets.token_urlsafe(16)
    params = urllib.parse.urlencode({
        "client_id":     CLIENT_ID,
        "response_type": "code",
        "redirect_uri":  REDIRECT_URI,
        "scope":         "openid profile email",
        "state":         state,
    })
    return f"{AUTHORIZE_URL}?{params}", state


def _post(url: str, data: dict) -> dict:
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def _get(url: str, access_token: str) -> dict:
    req = urllib.request.Request(url)
    req.add_header("Authorization", f"Bearer {access_token}")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())


def exchange_code(code: str) -> dict:
    """Обменивает authorization code на токены."""
    return _post(TOKEN_URL, {
        "grant_type":    "authorization_code",
        "code":          code,
        "redirect_uri":  REDIRECT_URI,
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    })


def fetch_userinfo(access_token: str) -> dict:
    """Получает профиль пользователя из Authentik."""
    return _get(USERINFO_URL, access_token)


def set_session(response: RedirectResponse, user_data: dict) -> RedirectResponse:
    """Записывает сессионную cookie в response."""
    response.set_cookie(
        COOKIE_NAME,
        make_session_cookie(user_data),
        max_age=COOKIE_MAX_AGE,
        httponly=True,
        samesite="lax",
    )
    return response
