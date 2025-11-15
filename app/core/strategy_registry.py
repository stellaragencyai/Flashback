# app/core/strategy_registry.py
"""
Flashback — Strategy Registry

Single source of truth for:
- What each account/subaccount is "for"
- Risk profile / AI usage flags
- Metadata used by journals, executors, AI logger, dashboards.

Accounts:
  - MAIN          → discretionary, high risk, manual bias
  - flashback01   → Trend follower
  - flashback02   → Breakout
  - flashback03   → Mirror / capital sink
  - flashback04   → Momentum / scalper
  - flashback05   → Range / fade
  - flashback06   → Swing
  - flashback07   → CANARY AI (when ready)
  - flashback08   → Experimental
  - flashback09   → Higher timeframe
  - flashback10   → Sandbox / dev
"""

from __future__ import annotations

from typing import Dict, Any, List

# Core registry
# You can tweak later; this is just a sane starting point.
_STRATEGIES: Dict[str, Dict[str, Any]] = {
    "MAIN": {
        "account_label": "MAIN",
        "role": "main",
        "mode": "discretionary",
        "risk_profile": "high",
        "ai_enabled": False,
        "canary": False,
        "max_daily_loss_pct": 8.0,
        "max_concurrent_positions": 6,
        "notes": "Primary manual account; bots manage exits & guardrails.",
    },
    "flashback01": {
        "account_label": "flashback01",
        "role": "trend",
        "mode": "semi_auto",
        "risk_profile": "medium",
        "ai_enabled": False,
        "canary": False,
        "max_daily_loss_pct": 3.0,
        "max_concurrent_positions": 3,
        "notes": "Trend-following core system.",
    },
    "flashback02": {
        "account_label": "flashback02",
        "role": "breakout",
        "mode": "semi_auto",
        "risk_profile": "medium_high",
        "ai_enabled": True,
        "canary": False,
        "max_daily_loss_pct": 4.0,
        "max_concurrent_positions": 4,
        "notes": "Breakout engine; first to get serious AI gating.",
    },
    "flashback03": {
        "account_label": "flashback03",
        "role": "mirror_sink",
        "mode": "mirror",
        "risk_profile": "aligned",
        "ai_enabled": False,
        "canary": False,
        "max_daily_loss_pct": 8.0,
        "max_concurrent_positions": 6,
        "notes": "Copy / mirror of MAIN, plus deposit 10% routing target.",
    },
    "flashback04": {
        "account_label": "flashback04",
        "role": "momentum_scalp",
        "mode": "auto",
        "risk_profile": "high",
        "ai_enabled": True,
        "canary": False,
        "max_daily_loss_pct": 4.0,
        "max_concurrent_positions": 5,
        "notes": "Momentum + scalping profile.",
    },
    "flashback05": {
        "account_label": "flashback05",
        "role": "range_fade",
        "mode": "auto",
        "risk_profile": "medium",
        "ai_enabled": True,
        "canary": False,
        "max_daily_loss_pct": 3.0,
        "max_concurrent_positions": 4,
        "notes": "Range / mean reversion profile.",
    },
    "flashback06": {
        "account_label": "flashback06",
        "role": "swing",
        "mode": "auto",
        "risk_profile": "medium_low",
        "ai_enabled": True,
        "canary": False,
        "max_daily_loss_pct": 2.5,
        "max_concurrent_positions": 5,
        "notes": "Higher timeframe swing trades.",
    },
    "flashback07": {
        "account_label": "flashback07",
        "role": "canary_ai",
        "mode": "auto",
        "risk_profile": "low",
        "ai_enabled": True,
        "canary": True,
        "max_daily_loss_pct": 1.0,
        "max_concurrent_positions": 2,
        "notes": "First real AI deployment; tiny risk, heavy monitoring.",
    },
    "flashback08": {
        "account_label": "flashback08",
        "role": "experimental",
        "mode": "auto",
        "risk_profile": "high",
        "ai_enabled": True,
        "canary": False,
        "max_daily_loss_pct": 4.0,
        "max_concurrent_positions": 4,
        "notes": "R&D / experimental strategies.",
    },
    "flashback09": {
        "account_label": "flashback09",
        "role": "htf_trend",
        "mode": "auto",
        "risk_profile": "medium_low",
        "ai_enabled": True,
        "canary": False,
        "max_daily_loss_pct": 2.0,
        "max_concurrent_positions": 3,
        "notes": "4h / 1D directional bias engine.",
    },
    "flashback10": {
        "account_label": "flashback10",
        "role": "sandbox",
        "mode": "dev",
        "risk_profile": "low",
        "ai_enabled": False,
        "canary": False,
        "max_daily_loss_pct": 1.0,
        "max_concurrent_positions": 2,
        "notes": "Dev / testing; safe to break.",
    },
}


def get_strategy(account_label: str) -> Dict[str, Any]:
    """
    Return strategy config for a given account label.

    account_label examples:
      - 'MAIN'
      - 'flashback01' .. 'flashback10'
    """
    return _STRATEGIES.get(account_label, {
        "account_label": account_label,
        "role": "unknown",
        "mode": "unknown",
        "risk_profile": "unknown",
        "ai_enabled": False,
        "canary": False,
        "max_daily_loss_pct": 0.0,
        "max_concurrent_positions": 0,
        "notes": "No explicit strategy config found.",
    })


def list_strategies() -> List[Dict[str, Any]]:
    """Return all strategies as a list."""
    return list(_STRATEGIES.values())
