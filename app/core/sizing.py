#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Sizing Helpers

Purpose
-------
Centralized sizing utilities used by the executor:

    from app.core.sizing import bayesian_size, risk_capped_qty

These functions work in *risk terms*:
    risk_usd = qty * stop_distance

and respect symbol-specific step size via flashback_common helpers.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Tuple

from app.core.flashback_common import get_ticks, qdown


def _to_decimal(x) -> Decimal:
    try:
        if isinstance(x, Decimal):
            return x
        return Decimal(str(x))
    except Exception:
        return Decimal("0")


def bayesian_size(
    symbol: str,
    equity_usd: Decimal,
    risk_pct: float,
    stop_distance: Decimal,
) -> Tuple[Decimal, Decimal]:
    """
    Basic risk-based sizing:

        risk_usd = equity_usd * (risk_pct / 100)
        qty_raw  = risk_usd / stop_distance
        qty      = qdown(qty_raw, step)

    Args:
        symbol:         e.g. "BTCUSDT"
        equity_usd:     current account equity in USD
        risk_pct:       % of equity to risk (e.g. 0.25 for 0.25%)
        stop_distance:  distance between entry and SL in price units

    Returns:
        (qty, risk_usd)
    """
    equity = _to_decimal(equity_usd)
    if equity <= 0:
        return Decimal("0"), Decimal("0")

    sd = _to_decimal(stop_distance)
    if sd <= 0:
        return Decimal("0"), Decimal("0")

    pct = Decimal(str(risk_pct)) / Decimal("100")
    risk_usd = equity * pct
    if risk_usd <= 0:
        return Decimal("0"), Decimal("0")

    _tick, step, _min_notional = get_ticks(symbol)

    qty_raw = risk_usd / sd
    qty = qdown(qty_raw, step)

    if qty <= 0:
        return Decimal("0"), Decimal("0")

    return qty, risk_usd


def risk_capped_qty(
    symbol: str,
    qty: Decimal,
    equity_usd: Decimal,
    max_risk_pct: float,
    stop_distance: Decimal,
) -> Tuple[Decimal, Decimal]:
    """
    Take a proposed quantity and make sure the implied risk does NOT exceed
    max_risk_pct of equity. If it does, we shrink the qty.

    Args:
        symbol:         e.g. "BTCUSDT"
        qty:            proposed position size (contract units / coin size)
        equity_usd:     account equity in USD
        max_risk_pct:   max % of equity you're willing to risk
        stop_distance:  distance between entry and SL in price units

    Returns:
        (qty_adj, risk_usd_adj)
    """
    equity = _to_decimal(equity_usd)
    q = _to_decimal(qty)
    sd = _to_decimal(stop_distance)

    if equity <= 0 or q <= 0 or sd <= 0:
        return Decimal("0"), Decimal("0")

    # Current implied risk
    implied_risk = q * sd

    max_risk = equity * (Decimal(str(max_risk_pct)) / Decimal("100"))
    if max_risk <= 0:
        # no cap
        return q, implied_risk

    if implied_risk <= max_risk:
        # Already within cap
        return q, implied_risk

    # Need to shrink qty
    factor = max_risk / implied_risk
    q_new = q * factor

    _tick, step, _min_notional = get_ticks(symbol)
    q_new = qdown(q_new, step)

    if q_new <= 0:
        return Decimal("0"), Decimal("0")

    risk_new = q_new * sd
    return q_new, risk_new
