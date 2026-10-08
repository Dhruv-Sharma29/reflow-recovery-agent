import hmac

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

from app.config import settings

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


def get_api_key(api_key_header: str | None = Security(api_key_header)) -> str | None:
    """Require a key, except for explicitly enabled local development."""
    if not settings.api_secret_key:
        if settings.environment == "development" and settings.allow_unauthenticated_development:
            return None
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Dashboard authentication is not configured",
        )

    if not api_key_header:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing X-API-Key header",
        )

    if not hmac.compare_digest(api_key_header.encode("utf-8"), settings.api_secret_key.encode("utf-8")):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials",
        )
    return api_key_header

