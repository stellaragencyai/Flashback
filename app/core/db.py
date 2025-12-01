import os

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)) or str(default)
    # strip inline comments and whitespace
    raw = raw.split("#", 1)[0].strip()
    try:
        return int(raw)
    except ValueError:
        return default

POLL_SECONDS   = _env_int("TPM_POLL_SECONDS", 2)
_ATR_CACHE_TTL = _env_int("TPM_ATR_CACHE_SEC", 60)

# ---------------------------------------------------------------------------
# Guard helpers (simple in-memory fallback)
#
# These are used by portfolio_guard to track daily PnL / guard state.
# For now this is purely in-memory so the executor can run without crashing.
# Later, we can wire this into a real SQLite table if needed.
# ---------------------------------------------------------------------------
from typing import Dict, Any

_guard_state: Dict[str, Any] = {
    "day": None,
    "realized_pnl_usd": 0.0,
}


def guard_load() -> Dict[str, Any]:
    """
    Load current guard state.

    Returns a dict, minimally containing:
        - "day": current session day (or None)
        - "realized_pnl_usd": cumulative realized PnL for the day
    """
    return dict(_guard_state)


def guard_update_pnl(delta_usd: float | int | None) -> None:
    """
    Increment the running realized PnL by `delta_usd`.

    This is a no-op if delta_usd is None or not numeric.
    """
    if delta_usd is None:
        return

    try:
        delta = float(delta_usd)
    except Exception:
        return

    current = float(_guard_state.get("realized_pnl_usd", 0.0))
    _guard_state["realized_pnl_usd"] = current + delta


def guard_reset_day() -> None:
    """
    Reset the guard state for a new day/session.
    """
    _guard_state["day"] = None
    _guard_state["realized_pnl_usd"] = 0.0
