#!/usr/bin/env python3
# app/core/bybit_async.py
from __future__ import annotations

import hmac
import hashlib
import time
from typing import Dict, Any, Optional

import aiohttp
import asyncio
import os
from decimal import Decimal

BYBIT_BASE = os.getenv("BYBIT_BASE", "https://api.bybit.com").rstrip("/")
API_KEY = os.getenv("BYBIT_MAIN_TRADE_KEY") or os.getenv("BYBIT_MAIN_READ_KEY") or ""
API_SECRET = os.getenv("BYBIT_MAIN_TRADE_SECRET") or os.getenv("BYBIT_MAIN_READ_SECRET") or ""

class BybitAsync:
    def __init__(self, api_key: str = API_KEY, api_secret: str = API_SECRET, base_url: str = BYBIT_BASE):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = base_url.rstrip("/")
        self._session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _sign(self, query: Dict[str, str]) -> Dict[str, str]:
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        items = sorted(query.items())
        query_str = "&".join(f"{k}={v}" for k, v in items)
        pre_sign = f"{ts}{self.api_key}{recv_window}{query_str}"
        sig = hmac.new(
            self.api_secret.encode("utf-8"),
            pre_sign.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        headers = {
            "X-BAPI-API-KEY": self.api_key,
            "X-BAPI-SIGN": sig,
            "X-BAPI-TIMESTAMP": ts,
            "X-BAPI-RECV-WINDOW": recv_window,
        }
        return headers

    async def get(self, path: str, query: Dict[str, Any]) -> Dict[str, Any]:
        session = await self._ensure_session()
        qs = {k: str(v) for k, v in query.items()}
        headers = self._sign(qs)
        url = f"{self.base_url}{path}"
        async with session.get(url, params=qs, headers=headers, timeout=7) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def post(self, path: str, body: Dict[str, Any]) -> Dict[str, Any]:
        session = await self._ensure_session()
        ts = str(int(time.time() * 1000))
        recv_window = "5000"
        payload = {k: v for k, v in body.items() if v is not None}
        # v5 POST sign: timestamp+api_key+recv_window+body_json (not shown fully here)
        # For now you can still use sync client for POST if you prefer; this is mostly for GET speed.
        raise NotImplementedError("POST async not wired yet")

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
