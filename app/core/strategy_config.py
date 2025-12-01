# app/core/strategy_config.py
# Flashback — Strategy Profiles & Subaccount Mapping
#
# Single source of truth for:
#   - Which strategy runs on which subaccount
#   - Risk per trade, max positions, allowed symbols/TFs
#   - Entry/exit profiles (ties into Signal Engine + Executor)
#
# This is intentionally simple & declarative so we can:
#   • Query it from signal_engine (for filters, TFs, symbols)
#   • Query it from executor (for risk sizing, TP/SL profile)
#   • Log it into ai_store for learning (strategy_role, etc.)

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Literal, Optional

# Modes of automation for a strategy
AutomationMode = Literal["OFF", "PAPER", "LIVE"]

# Exit profiles (we already talked about Standard-5 etc.)
ExitProfile = Literal["STANDARD_5", "CUSTOM_5_TBD", "SCALP_3", "SWING_2"]

# Simple regime filter info for signals
@dataclass
class RegimeFilter:
    min_adx: float = 0.0          # e.g. 18–22 for trend-only
    max_atr_pct: float = 999.0    # optional cap
    min_vol_z: float = -999.0     # volume z-score floor
    max_vol_z: float = 999.0      # volume z-score ceiling


@dataclass
class StrategyProfile:
    id: str
    name: str
    description: str

    # Which subaccount this strategy is "designed for"
    # (executor uses this to route; optional so we can reuse profiles)
    preferred_sub_uid: Optional[str] = None

    # Trading universe
    symbols: List[str] = None
    timeframes: List[str] = None
    bias_timeframe: str = "1h"

    # Risk and exposure
    risk_pct_per_trade: float = 0.10  # 0.10 = 0.10% of equity
    max_positions: int = 3
    max_notional_pct_per_position: float = 5.0  # 5% of equity per position cap

    # Exit behavior
    exit_profile: ExitProfile = "STANDARD_5"

    # How automated this strategy currently is
    automation_mode: AutomationMode = "OFF"

    # Regime filters for signal engine
    regime: RegimeFilter = RegimeFilter()

    # Extra tags for logging / AI
    tags: List[str] = None


# ---------- Strategy Profiles ----------

# Sub7 — Canary / AI sandbox: small size, lots of experimentation
STRAT_CANARY_TREND_M5 = StrategyProfile(
    id="CANARY_TREND_M5",
    name="Canary Trend M5",
    description="Small-size trend-following on liquid perp alts, 5m entries with 1h bias. Used as AI sandbox.",
    preferred_sub_uid="flashback07",  # map to your real sub UID later
    symbols=["BTCUSDT", "ETHUSDT", "PUMPFUNUSDT", "FARTCOINUSDT"],
    timeframes=["5m", "15m"],
    bias_timeframe="1h",
    risk_pct_per_trade=0.05,
    max_positions=2,
    max_notional_pct_per_position=2.0,
    exit_profile="STANDARD_5",
    automation_mode="PAPER",  # start safe
    regime=RegimeFilter(
        min_adx=18.0,
        max_atr_pct=5.0,
        min_vol_z=-0.5,
        max_vol_z=3.0,
    ),
    tags=["canary", "trend", "ai_sandbox"],
)

# Sub2 — Breakout / momentum
STRAT_BREAKOUT_M15 = StrategyProfile(
    id="BREAKOUT_M15",
    name="Breakout M15",
    description="Breakout & momentum plays on strong movers, 15m entries, 1h bias.",
    preferred_sub_uid="flashback02",
    symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "PUMPFUNUSDT"],
    timeframes=["15m"],
    bias_timeframe="1h",
    risk_pct_per_trade=0.15,
    max_positions=3,
    max_notional_pct_per_position=4.0,
    exit_profile="STANDARD_5",
    automation_mode="OFF",  # executor can require manual approval first
    regime=RegimeFilter(
        min_adx=20.0,
        max_atr_pct=6.0,
        min_vol_z=0.0,
        max_vol_z=4.0,
    ),
    tags=["breakout", "momentum"],
)

# Sub1 — Higher-timeframe Trend / Swing
STRAT_SWING_H1 = StrategyProfile(
    id="SWING_H1",
    name="Swing Trend H1",
    description="Higher timeframe trend-following using 1h entries and 4h bias.",
    preferred_sub_uid="flashback01",
    symbols=["BTCUSDT", "ETHUSDT", "SOLUSDT", "LINKUSDT"],
    timeframes=["1h"],
    bias_timeframe="4h",
    risk_pct_per_trade=0.25,
    max_positions=4,
    max_notional_pct_per_position=6.0,
    exit_profile="STANDARD_5",
    automation_mode="OFF",
    regime=RegimeFilter(
        min_adx=17.0,
        max_atr_pct=8.0,
        min_vol_z=-0.5,
        max_vol_z=3.5,
    ),
    tags=["swing", "trend"],
)

# Placeholder profiles for other subs we’ll refine later
STRAT_SCALP_M1 = StrategyProfile(
    id="SCALP_M1",
    name="Scalp M1",
    description="Ultra-short-term scalps on 1m/3m. Will stay PAPER/OFF until proven.",
    preferred_sub_uid="flashback03",
    symbols=["BTCUSDT", "ETHUSDT"],
    timeframes=["1m", "3m"],
    bias_timeframe="15m",
    risk_pct_per_trade=0.05,
    max_positions=1,
    max_notional_pct_per_position=1.5,
    exit_profile="SCALP_3",
    automation_mode="OFF",
    regime=RegimeFilter(
        min_adx=15.0,
        max_atr_pct=4.0,
        min_vol_z=-0.5,
        max_vol_z=2.5,
    ),
    tags=["scalp"],
)

STRAT_RANGE_M15 = StrategyProfile(
    id="RANGE_M15",
    name="Range-Fade M15",
    description="Range-bound mean-reversion, short leash, tight risk.",
    preferred_sub_uid="flashback04",
    symbols=["BTCUSDT", "ETHUSDT"],
    timeframes=["15m"],
    bias_timeframe="1h",
    risk_pct_per_trade=0.10,
    max_positions=2,
    max_notional_pct_per_position=3.0,
    exit_profile="CUSTOM_5_TBD",
    automation_mode="OFF",
    regime=RegimeFilter(
        min_adx=0.0,
        max_atr_pct=4.0,
        min_vol_z=-1.0,
        max_vol_z=2.0,
    ),
    tags=["range", "mean_reversion"],
)


# All strategy profiles by id
STRATEGIES: Dict[str, StrategyProfile] = {
    s.id: s
    for s in [
        STRAT_CANARY_TREND_M5,
        STRAT_BREAKOUT_M15,
        STRAT_SWING_H1,
        STRAT_SCALP_M1,
        STRAT_RANGE_M15,
    ]
}

# Map "logical subaccounts" to strategy ids.
# These names should match what you use for sub labels in app.core.subs.
SUBACCOUNT_STRATEGY: Dict[str, str] = {
    "flashback01": "SWING_H1",
    "flashback02": "BREAKOUT_M15",
    "flashback03": "SCALP_M1",
    "flashback04": "RANGE_M15",
    "flashback07": "CANARY_TREND_M5",  # AI sandbox / canary
    # others can be added later (flashback05, 06, 08, 09, 10)
}


def get_strategy(strategy_id: str) -> StrategyProfile:
    return STRATEGIES[strategy_id]


def get_strategy_for_sub(label: str) -> Optional[StrategyProfile]:
    """
    label is e.g. 'flashback01', 'flashback07', etc.
    Returns the StrategyProfile or None if not mapped.
    """
    sid = SUBACCOUNT_STRATEGY.get(label)
    if not sid:
        return None
    return STRATEGIES.get(sid)


def strategy_dict(strategy_id: str) -> Dict:
    """
    Convenience to log a strategy in ai_store (dict form).
    """
    s = get_strategy(strategy_id)
    d = asdict(s)
    # Flatten regime to avoid nested dataclasses in logs if you want
    return d
