from __future__ import annotations

from contextvars import ContextVar
from dataclasses import dataclass
import os
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

import jwt
from jwt import PyJWKClient
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from alphavault.constants import (
    ENV_CLOUDFLARE_ACCESS_AUD,
    ENV_CLOUDFLARE_ACCESS_TEAM_DOMAIN,
)

TRACE_ID_HEADER = "X-Trace-Id"
CF_RAY_HEADER = "Cf-Ray"
CF_ACCESS_JWT_HEADER = "Cf-Access-Jwt-Assertion"
CF_ACCESS_EMAIL_HEADER = "Cf-Access-Authenticated-User-Email"
CF_ACCESS_CLIENT_ID_HEADER = "CF-Access-Client-Id"

_CURRENT_REQUEST_META: ContextVar["McpRequestMeta | None"] = ContextVar(
    "alphavault_mcp_request_meta",
    default=None,
)
_CERTS_CLIENTS: dict[str, PyJWKClient] = {}


class McpAccessError(RuntimeError):
    pass


@dataclass(frozen=True)
class McpRequestMeta:
    trace_id: str
    request_path: str
    cf_ray: str
    auth_mode: str
    access_subject: str
    access_email: str
    access_aud: str


def _clean_text(value: object) -> str:
    return str(value or "").strip()


def _normalized_team_domain() -> str:
    raw = _clean_text(os.getenv(ENV_CLOUDFLARE_ACCESS_TEAM_DOMAIN))
    if not raw:
        return ""
    parsed = urlparse(raw if "://" in raw else f"https://{raw}")
    return _clean_text(parsed.netloc or parsed.path).rstrip("/")


def _issuer_url() -> str:
    team_domain = _normalized_team_domain()
    if not team_domain:
        return ""
    return f"https://{team_domain}"


def _certs_url() -> str:
    issuer_url = _issuer_url()
    if not issuer_url:
        return ""
    return f"{issuer_url}/cdn-cgi/access/certs"


def _configured_audiences() -> list[str]:
    raw = _clean_text(os.getenv(ENV_CLOUDFLARE_ACCESS_AUD))
    if not raw:
        return []
    return [item for item in (part.strip() for part in raw.split(",")) if item]


def _jwk_client() -> PyJWKClient | None:
    certs_url = _certs_url()
    if not certs_url:
        return None
    cached = _CERTS_CLIENTS.get(certs_url)
    if cached is not None:
        return cached
    client = PyJWKClient(certs_url)
    _CERTS_CLIENTS[certs_url] = client
    return client


def _audience_text(value: object) -> str:
    if isinstance(value, (list, tuple)):
        return ",".join(_clean_text(item) for item in value if _clean_text(item))
    return _clean_text(value)


def _decode_access_claims(token: str) -> dict[str, Any]:
    resolved_token = _clean_text(token)
    if not resolved_token:
        raise McpAccessError("missing_access_jwt")
    try:
        claims = jwt.decode(
            resolved_token,
            options={
                "verify_signature": False,
                "verify_aud": False,
                "verify_exp": False,
                "verify_iss": False,
            },
            algorithms=["RS256", "ES256"],
        )
    except Exception as err:
        raise McpAccessError(f"invalid_access_jwt:{type(err).__name__}") from err

    jwk_client = _jwk_client()
    audiences = _configured_audiences()
    issuer_url = _issuer_url()
    if jwk_client is None or not audiences or not issuer_url:
        return claims
    try:
        signing_key = jwk_client.get_signing_key_from_jwt(resolved_token)
        verified_claims = jwt.decode(
            resolved_token,
            signing_key.key,
            algorithms=["RS256", "ES256"],
            audience=audiences,
            issuer=issuer_url,
        )
    except Exception as err:
        raise McpAccessError(
            f"access_jwt_verification_failed:{type(err).__name__}"
        ) from err
    return dict(verified_claims)


def build_request_meta(request: Request) -> McpRequestMeta:
    trace_id = _clean_text(request.headers.get(TRACE_ID_HEADER)) or uuid4().hex
    jwt_token = _clean_text(request.headers.get(CF_ACCESS_JWT_HEADER))
    email_header = _clean_text(request.headers.get(CF_ACCESS_EMAIL_HEADER))
    client_id = _clean_text(request.headers.get(CF_ACCESS_CLIENT_ID_HEADER))
    auth_mode = ""
    claims: dict[str, Any] = {}

    if jwt_token:
        auth_mode = "access_jwt"
        claims = _decode_access_claims(jwt_token)
    elif email_header:
        auth_mode = "access_email_header"
    elif client_id:
        auth_mode = "service_token"
    else:
        raise McpAccessError("missing_access_headers")

    access_email = email_header or _clean_text(claims.get("email"))
    access_subject = (
        _clean_text(claims.get("sub")) or client_id or access_email or auth_mode
    )
    access_aud = _audience_text(claims.get("aud")) or ",".join(_configured_audiences())

    return McpRequestMeta(
        trace_id=trace_id,
        request_path=_clean_text(request.url.path),
        cf_ray=_clean_text(request.headers.get(CF_RAY_HEADER)),
        auth_mode=auth_mode,
        access_subject=access_subject,
        access_email=access_email,
        access_aud=access_aud,
    )


def require_current_request_meta() -> McpRequestMeta:
    meta = _CURRENT_REQUEST_META.get()
    if meta is None:
        raise RuntimeError("missing_mcp_request_meta")
    return meta


class McpRequestMetaMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next) -> Response:
        try:
            meta = build_request_meta(request)
        except McpAccessError as err:
            trace_id = _clean_text(request.headers.get(TRACE_ID_HEADER)) or uuid4().hex
            response = JSONResponse(
                {"ok": False, "error": str(err)},
                status_code=401,
            )
            response.headers[TRACE_ID_HEADER] = trace_id
            return response

        token = _CURRENT_REQUEST_META.set(meta)
        try:
            response = await call_next(request)
        finally:
            _CURRENT_REQUEST_META.reset(token)
        response.headers[TRACE_ID_HEADER] = meta.trace_id
        return response


__all__ = [
    "CF_ACCESS_EMAIL_HEADER",
    "CF_ACCESS_JWT_HEADER",
    "CF_RAY_HEADER",
    "McpAccessError",
    "McpRequestMeta",
    "McpRequestMetaMiddleware",
    "TRACE_ID_HEADER",
    "build_request_meta",
    "require_current_request_meta",
]
