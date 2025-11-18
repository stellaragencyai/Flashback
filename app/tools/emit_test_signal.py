#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — One-shot Test Signal Emitter

Purpose:
- Append a single well-formed signal into signals/observed.jsonl
- So executor_v2 + strategies + AI + sizing can be tested end-to-end.

This does NOT talk to Bybit directly. It just writes to the signal bus.
"""

from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime, timezone

from app.core.config import settings

ROOT = settings.ROOT
SIGNAL_FILE = ROOT / "signals" / "observed.jsonl"
SIGNAL_FILE.parent.mkdir(parents=True, exist_ok=True)


def main() -> None:
    # Choose a canary-friendly symbol / TF
    # All 3 canaries see BTCUSDT 5m, so that's perfect.
    signal = {
        "symbol": "BTCUSDT",
        "timeframe": "5",
        "side": "Buy",          # long
        "price": 65000,         # dummy last price; executor uses this for sizing
        "ts": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "source": "emit_test_signal",
        "note": "single test signal for executor_v2 + canaries",
    }

    line = json.dumps(signal, separators=(",", ":"))
    with SIGNAL_FILE.open("a", encoding="utf-8") as f:
        f.write(line + "\n")

    print(f"Appended test signal to {SIGNAL_FILE}:\n{line}")


if __name__ == "__main__":
    main()
