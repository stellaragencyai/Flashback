# app/core/events.py
# Canonical event models for the Flashback ecosystem.

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Dict, Optional, Literal, List


SideStr = Literal["buy", "sell"]
OrderStatusStr = Literal[
    "new",
    "partially_filled",
    "filled",
    "canceled",
    "rejected",
    "expired",
]
ExecTypeStr = Literal[
    "trade",
    "canceled",
    "rejected",
    "expired",
]
AccountLabel = str


# ---------------------------------------------------------------------------
# PositionEvent
# ---------------------------------------------------------------------------

@dataclass
class PositionEvent:
    """
    Normalized position view derived from Bybit raw rows or position_bus snapshot.
    """

    account_label: AccountLabel
    category: str  # e.g. "linear"
    symbol: str
    side: SideStr            # "buy" or "sell"
    size: Decimal
    avg_price: Decimal
    leverage: Optional[Decimal] = None
    unrealized_pnl: Optional[Decimal] = None
    updated_ms: Optional[int] = None

    raw: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# OrderEvent
# ---------------------------------------------------------------------------

@dataclass
class OrderEvent:
    """
    Order lifecycle event — creation/update cancel, etc.
    Usually mapped from Bybit v5 order stream when we wire it.
    """

    account_label: AccountLabel
    category: str            # "linear"
    symbol: str
    order_id: str
    order_link_id: Optional[str]

    side: SideStr
    qty: Decimal
    price: Optional[Decimal]

    status: OrderStatusStr
    created_ms: Optional[int] = None
    updated_ms: Optional[int] = None

    raw: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# ExecutionEvent
# ---------------------------------------------------------------------------

@dataclass
class ExecutionEvent:
    """
    Fill / trade-level event.
    """

    account_label: AccountLabel
    category: str        # "linear"
    symbol: str
    order_id: str
    exec_id: str

    side: SideStr
    exec_qty: Decimal
    exec_price: Decimal
    exec_value: Optional[Decimal] = None
    fee: Optional[Decimal] = None
    fee_asset: Optional[str] = None

    is_maker: Optional[bool] = None
    ts_ms: Optional[int] = None

    raw: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# BalanceEvent
# ---------------------------------------------------------------------------

@dataclass
class BalanceEvent:
    """
    High-level equity / balance update.
    Can be derived from REST or future account WS stream.
    """

    account_label: AccountLabel
    coin: str           # e.g. "USDT"
    equity: Decimal
    available: Optional[Decimal] = None
    wallet_balance: Optional[Decimal] = None

    ts_ms: Optional[int] = None
    raw: Dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# GuardrailEvent
# ---------------------------------------------------------------------------

@dataclass
class GuardrailEvent:
    """
    Emitted by portfolio_guard or risk daemon whenever a rule triggers.
    """

    kind: Literal[
        "breaker_on",
        "breaker_off",
        "max_dd_hit",
        "max_risk_hit",
        "symbol_cap_hit",
        "mmr_trim",
    ]

    account_label: Optional[AccountLabel]  # None for global events
    reason: str
    details: Dict[str, Any] = field(default_factory=dict)
    ts_ms: int = 0


# ---------------------------------------------------------------------------
# HeartbeatEvent
# ---------------------------------------------------------------------------

@dataclass
class HeartbeatEvent:
    """
    Generic heartbeat from a component.
    Useful once we have a central health monitor.
    """

    component: str
    account_label: Optional[AccountLabel]
    ts_ms: int
    meta: Dict[str, Any] = field(default_factory=dict)
