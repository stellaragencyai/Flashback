#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Stack Status Tool

Purpose
-------
Quick health check for a SINGLE ACCOUNT_LABEL:

  • Heartbeats (from record_heartbeat in flashback_common)
  • positions_bus.json freshness
  • orderbook_bus.json freshness
  • trades_bus.json freshness
  • Basic account snapshot via ai_state_bus (equity, mmr_pct, open positions)

Usage
-----
From project root:

    export ACCOUNT_LABEL=main
    python -m app.tools.ai_stack_status

Env
---
ACCOUNT_LABEL                (default: "main")
AI_STATUS_MAX_AGE_SEC_POS    (default: "5")
AI_STATUS_MAX_AGE_SEC_OB     (default: "5")
AI_STATUS_MAX_AGE_SEC_TRADES (default: "10")
AI_STATUS_SEND_TG            (default: "false")
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict, Optional

import orjson

from app.core.flashback_common import send_tg
from app.core.ai_state_bus import build_ai_snapshot
from app.core.market_bus import (
    orderbook_bus_updated_ms,
    trades_bus_updated_ms,
)
from app.core.position_bus import get_snapshot as get_positions_snapshot_raw

# ---------------------------------------------------------------------------
# Env helpers
# ---------------------------------------------------------------------------

def _env_bool(name: str, default: str = "false") -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "y")


def _env_int(name: str, default: str) -> int:
    try:
        return int(os.getenv(name, default).strip())
    except Exception:
        return int(default)


ACCOUNT_LABEL: str = os.getenv("ACCOUNT_LABEL", "main").strip() or "main"

MAX_AGE_POS_SEC: int = _env_int("AI_STATUS_MAX_AGE_SEC_POS", "5")
MAX_AGE_OB_SEC: int = _env_int("AI_STATUS_MAX_AGE_SEC_OB", "5")
MAX_AGE_TRADES_SEC: int = _env_int("AI_STATUS_MAX_AGE_SEC_TRADES", "10")

SEND_TG: bool = _env_bool("AI_STATUS_SEND_TG", "false")


# ---------------------------------------------------------------------------
# Paths (heartbeats)
# ---------------------------------------------------------------------------

try:
    from app.core.config import settings
except Exception:  # pragma: no cover
    class _DummySettings:  # type: ignore
        ROOT: Path = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

HEARTBEAT_FILE: Path = STATE_DIR / "heartbeats.json"


# ---------------------------------------------------------------------------
# Heartbeat helpers
# ---------------------------------------------------------------------------

def _load_heartbeats() -> Dict[str, int]:
    try:
        if not HEARTBEAT_FILE.exists():
            return {}
        raw = HEARTBEAT_FILE.read_bytes()
        if not raw:
            return {}
        data = orjson.loads(raw)
        if not isinstance(data, dict):
            return {}
        out: Dict[str, int] = {}
        for k, v in data.items():
            try:
                out[str(k)] = int(v)
            except Exception:
                continue
        return out
    except Exception:
        return {}


def _age_sec(ts_ms: Optional[int]) -> Optional[float]:
    if ts_ms is None:
        return None
    try:
        ts = int(ts_ms)
    except Exception:
        return None
    if ts <= 0:
        return None
    now = int(time.time() * 1000)
    if now <= ts:
        return 0.0
    return (now - ts) / 1000.0


# ---------------------------------------------------------------------------
# Core status builder
# ---------------------------------------------------------------------------

def _status_positions_bus() -> Dict[str, Any]:
    snap, age = get_positions_snapshot_raw()
    out: Dict[str, Any] = {
        "ok": False,
        "age_sec": None,
        "labels": [],
    }
    if snap is None:
        return out
    out["age_sec"] = age
    labels = list((snap.get("labels") or {}).keys())
    out["labels"] = labels
    out["ok"] = (age is not None and age <= MAX_AGE_POS_SEC)
    return out


def _status_orderbook() -> Dict[str, Any]:
    upd = orderbook_bus_updated_ms()
    age = _age_sec(upd) if upd is not None else None
    return {
        "ok": (age is not None and age <= MAX_AGE_OB_SEC),
        "age_sec": age,
    }


def _status_trades() -> Dict[str, Any]:
    upd = trades_bus_updated_ms()
    age = _age_sec(upd) if upd is not None else None
    return {
        "ok": (age is not None and age <= MAX_AGE_TRADES_SEC),
        "age_sec": age,
    }


def _status_account() -> Dict[str, Any]:
    """
    Use ai_state_bus to fetch a basic account snapshot for this ACCOUNT_LABEL.
    """
    snap = build_ai_snapshot(
        focus_symbols=None,
        include_trades=False,
        trades_limit=20,
        include_orderbook=False,
    )
    acc = snap.get("account") or {}

    equity = acc.get("equity_usdt", "0")
    mmr = acc.get("mmr_pct", "0")
    pos_block = snap.get("positions") or {}
    open_count = len((pos_block.get("by_symbol") or {}).keys())

    return {
        "equity_usdt": str(equity),
        "mmr_pct": str(mmr),
        "open_positions": open_count,
    }


def _status_heartbeats() -> Dict[str, Any]:
    hb = _load_heartbeats()

    components = [
        "ws_switchboard",
        "tp_sl_manager",
        "ai_pilot",
        "ai_action_router",
        "supervisor_ai_stack",
    ]

    workers = [
        "supervisor_worker_ws_switchboard",
        "supervisor_worker_tp_sl_manager",
        "supervisor_worker_ai_pilot",
        "supervisor_worker_ai_action_router",
    ]

    rows: Dict[str, Dict[str, Any]] = {}

    for name in components + workers:
        ts = hb.get(name)
        age = _age_sec(ts) if ts is not None else None
        rows[name] = {
            "last_ms": ts,
            "age_sec": age,
        }

    return {
        "raw": hb,
        "entries": rows,
    }


def build_status() -> Dict[str, Any]:
    """
    Build a full AI stack status dict for current ACCOUNT_LABEL.
    """
    return {
        "account_label": ACCOUNT_LABEL,
        "positions_bus": _status_positions_bus(),
        "orderbook_bus": _status_orderbook(),
        "trades_bus": _status_trades(),
        "account": _status_account(),
        "heartbeats": _status_heartbeats(),
        "ts_ms": int(time.time() * 1000),
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

def _fmt_age(age: Optional[float]) -> str:
    if age is None:
        return "n/a"
    return f"{age:.1f}s"


def _render_status_human(status: Dict[str, Any]) -> str:
    acc = status.get("account") or {}
    pos_bus = status.get("positions_bus") or {}
    ob = status.get("orderbook_bus") or {}
    tr = status.get("trades_bus") or {}
    hb = status.get("heartbeats") or {}

    hb_rows = hb.get("entries") or {}

    lines = []
    lines.append(f"🔎 Flashback AI Stack Status — account={status.get('account_label')}")
    lines.append("")
    lines.append("Account:")
    lines.append(
        f"  • equity_usdt  : {acc.get('equity_usdt')}"
    )
    lines.append(
        f"  • mmr_pct      : {acc.get('mmr_pct')}"
    )
    lines.append(
        f"  • open_pos_cnt : {acc.get('open_positions')}"
    )
    lines.append("")
    lines.append("Buses:")
    lines.append(
        f"  • positions_bus : {'OK' if pos_bus.get('ok') else 'STALE'} "
        f"(age={_fmt_age(pos_bus.get('age_sec'))}, labels={pos_bus.get('labels')})"
    )
    lines.append(
        f"  • orderbook_bus : {'OK' if ob.get('ok') else 'STALE'} "
        f"(age={_fmt_age(ob.get('age_sec'))})"
    )
    lines.append(
        f"  • trades_bus    : {'OK' if tr.get('ok') else 'STALE'} "
        f"(age={_fmt_age(tr.get('age_sec'))})"
    )

    lines.append("")
    lines.append("Heartbeats (age):")
    for name, info in hb_rows.items():
        age = info.get("age_sec")
        lines.append(f"  • {name:30s}: {_fmt_age(age)}")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI entry
# ---------------------------------------------------------------------------

def main() -> None:
    status = build_status()
    text = _render_status_human(status)

    # Print to stdout
    print(text)

    # Optional Telegram send
    if SEND_TG:
        try:
            send_tg(text)
        except Exception:
            pass


if __name__ == "__main__":
    main()
