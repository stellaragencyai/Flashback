#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Minimal Bybit client

Thin wrapper on top of app.core.flashback_common.bybit_get/bybit_post.

Executor_v2 only needs `place_order` right now.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from app.core.flashback_common import bybit_get, bybit_post


class Bybit:
    def __init__(self, key_role: str = "trade") -> None:
        """
        key_role is kept for compatibility ("read" / "trade" / "transfer"),
        but flashback_common already wires the correct keys from env, so we
        just store it and do nothing fancy.
        """
        self.key_role = key_role

    def place_order(
        self,
        *,
        category: str,
        symbol: str,
        side: str,
        qty: float,
        orderType: str = "Market",
        **extra: Any,
    ) -> Dict[str, Any]:
        """
        Create a basic v5 order.
        Extra kwargs (e.g. reduceOnly, positionIdx, etc.) are passed through.
        """
        body: Dict[str, Any] = {
            "category": category,
            "symbol": symbol,
            "side": side,
            "qty": str(qty),
            "orderType": orderType,
        }
        body.update(extra)
        return bybit_post("/v5/order/create", body)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Generic GET if you need it elsewhere."""
        return bybit_get(path, params or {})

    def post(self, path: str, body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Generic POST if you need it elsewhere."""
        return bybit_post(path, body or {})
