from __future__ import annotations

from fastapi import Header, HTTPException, status

from .config import get_settings


async def api_key_auth(x_api_key: str | None = Header(default=None)) -> None:
    settings = get_settings()
    # If API key not set, authentication is disabled
    if not settings.API_KEY:
        return
    if x_api_key != settings.API_KEY:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")
