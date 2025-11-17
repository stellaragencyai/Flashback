#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Sizing helpers (stub-friendly)

Provides:
    - bayesian_size(symbol, side, price, strat_name)
    - risk_capped_qty(symbol, raw_qty)

Current behavior:
    * bayesian_size: fixed % of equity using DEFAULT_TRADE_NOTIONAL_PCT.
    * risk_capped_qty: snap to qtyStep and enforce at least one step.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import os

from app.core.flashback_common import get_equity_usdt, get_ticks


def _default_notional_pct() -> Decimal:
    raw = os.getenv("DEFAULT_TRADE_NOTIONAL_PCT", "1.0")
    try:
        return Decimal(str(raw))
    except Exception:
        return Decimal("1.0")


def bayesian_size(
    symbol: str,
    side: str,
    price: float,
    strat_name: str,
) -> Decimal:
    """
    Dumb initial sizing: fixed % of equity.
    Replace later with your Bayesian / expectancy-based logic.
    """
    eq = get_equity_usdt()
    pct = _default_notional_pct()
    if eq <= 0 or pct <= 0:
        return Decimal("0")

    if price <= 0:
        return Decimal("0")

    notional = eq * pct / Decimal("100")
    qty = Decimal(notional) / Decimal(str(price))
    return qty


def risk_capped_qty(symbol: str, raw_qty: Decimal) -> Decimal:
    """
    Enforce min-size using instrument qtyStep and a very basic min rule.
    """
    if raw_qty is None or raw_qty <= 0:
        return Decimal("0")

    tick, step, _min_notional = get_ticks(symbol)

    # Snap to step
    snapped = (raw_qty / step).quantize(Decimal("0")) * step

    if snapped <= 0:
        snapped = step

    return snapped
