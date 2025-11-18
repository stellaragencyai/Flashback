#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Strategies Debug Tool

What this does:
- Loads config/strategies.yaml through app.core.strategies
- Prints:
    * All strategies summary
    * Live (canary/full) strategies per example symbol/timeframe
    * AI-eval strategies per symbol/timeframe

Use this BEFORE wiring executor_v2 to make sure the registry is correct.
"""

from __future__ import annotations

from typing import List

from app.core.strategies import (
    all_sub_strategies,
    enabled_strategies,
    live_strategies_for_signal,
    ai_strategies_for_signal,
)
from app.core.logger import get_logger

log = get_logger("strategies_debug")


def _fmt_strat(s) -> str:
    return (
        f"{s.name} | sub_uid={s.sub_uid} | role={s.role} | "
        f"mode={s.automation_mode} | risk={s.risk_per_trade_pct:.3f}% | "
        f"symbols={len(s.symbols)} tf={','.join(s.timeframes)}"
    )


def _print_header(title: str) -> None:
    print("\n" + "=" * 80)
    print(f"*** {title}")
    print("=" * 80)


def _print_list(title: str, items: List) -> None:
    _print_header(title)
    if not items:
        print("  (none)")
        return
    for s in items:
        print("  - " + _fmt_strat(s))


def main() -> None:
    # 1) Print all strategies
    all_strats = all_sub_strategies()
    enabled = enabled_strategies()

    _print_list("ALL STRATEGIES", all_strats)
    _print_list("ENABLED STRATEGIES", enabled)

    # 2) Check some example signals (BTC 5m, SOL 15m, FART/1m, PUMPFUN/5m)
    examples = [
        ("BTCUSDT", "5"),
        ("BTCUSDT", "15"),
        ("SOLUSDT", "5"),
        ("SOLUSDT", "15"),
        ("FARTCOINUSDT", "1"),
        ("FARTCOINUSDT", "5"),
        ("PUMPFUNUSDT", "1"),
        ("PUMPFUNUSDT", "5"),
    ]

    for symbol, tf in examples:
        ai_strats = ai_strategies_for_signal(symbol, tf)
        live_strats = live_strategies_for_signal(symbol, tf)

        _print_header(f"SYMBOL={symbol} TF={tf}")
        print("AI-EVAL STRATEGIES:")
        if not ai_strats:
            print("  (none)")
        else:
            for s in ai_strats:
                print("  - " + _fmt_strat(s))

        print("\nLIVE STRATEGIES (canary/full):")
        if not live_strats:
            print("  (none)")
        else:
            for s in live_strats:
                print("  - " + _fmt_strat(s))


if __name__ == "__main__":
    log.info("Running strategies_debug tool...")
    main()
