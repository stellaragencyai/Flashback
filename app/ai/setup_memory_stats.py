#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Setup Memory Stats (v1)

Purpose
-------
Read:
  - state/setup_memory.jsonl   (labeled examples from setup_memory builder)

Produce:
  - Aggregated stats per strategy, per symbol, and per (strategy, symbol):
      • # trades
      • win / loss / breakeven counts
      • winrate
      • avg RR
      • median RR
      • avg PnL (usd)
      • share of "good" trades (rating >= 7)
  - Write JSON summary to:
      • state/setup_memory_summary.json
  - Print a concise ranking to stdout for quick eyeballing.

This is offline analytics ONLY — no Bybit calls, no orders.
"""

from __future__ import annotations

import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional

import orjson

# tolerant settings import
try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore


ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

MEMORY_PATH = STATE_DIR / "setup_memory.jsonl"
SUMMARY_PATH = STATE_DIR / "setup_memory_summary.json"


# ---------- helpers ----------

def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"[setup_memory_stats] WARNING: {path} not found.")
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                row = orjson.loads(line)
                if isinstance(row, dict):
                    rows.append(row)
            except Exception:
                continue
    return rows


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


# ---------- aggregation ----------

def _bucket_global(
    agg: Dict[str, Dict[str, Any]],
    strategy_id: str,
    strategy_name: str,
    sub_uid: str,
) -> Dict[str, Any]:
    if strategy_id not in agg:
        agg[strategy_id] = {
            "strategy_id": strategy_id,
            "strategy": strategy_name or strategy_id,
            "sub_uid": sub_uid or None,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "breakevens": 0,
            "good_trades": 0,   # rating >= 7
            "rr_list": [],
            "pnl_list": [],
        }
    b = agg[strategy_id]
    if not b.get("strategy") and strategy_name:
        b["strategy"] = strategy_name
    if not b.get("sub_uid") and sub_uid:
        b["sub_uid"] = sub_uid
    return b


def _bucket_sym(
    agg: Dict[str, Dict[str, Dict[str, Any]]],
    strategy_id: str,
    strategy_name: str,
    sub_uid: str,
    symbol: str,
) -> Dict[str, Any]:
    if strategy_id not in agg:
        agg[strategy_id] = {}
    if symbol not in agg[strategy_id]:
        agg[strategy_id][symbol] = {
            "strategy_id": strategy_id,
            "strategy": strategy_name or strategy_id,
            "sub_uid": sub_uid or None,
            "symbol": symbol,
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "breakevens": 0,
            "good_trades": 0,
            "rr_list": [],
            "pnl_list": [],
        }
    b = agg[strategy_id][symbol]
    if not b.get("strategy") and strategy_name:
        b["strategy"] = strategy_name
    if not b.get("sub_uid") and sub_uid:
        b["sub_uid"] = sub_uid
    return b


def build_stats(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_strategy: Dict[str, Dict[str, Any]] = {}
    by_strategy_symbol: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for r in rows:
        strat_id = str(r.get("strategy_id") or r.get("strategy") or "UNKNOWN")
        strat_name = str(r.get("strategy") or strat_id)
        sub_uid = str(r.get("sub_uid") or "")
        symbol = str(r.get("symbol") or "UNKNOWN").upper()

        result = str(r.get("result", "UNKNOWN")).upper()
        rr_f = _safe_float(r.get("realized_rr"))
        pnl_f = _safe_float(r.get("realized_pnl"))
        rating = r.get("rating_score")
        label_good = bool(r.get("label_good", False))

        # global bucket
        bg = _bucket_global(by_strategy, strat_id, strat_name, sub_uid)
        bg["trades"] += 1
        if result == "WIN":
            bg["wins"] += 1
        elif result == "LOSS":
            bg["losses"] += 1
        elif result == "BREAKEVEN":
            bg["breakevens"] += 1
        if label_good:
            bg["good_trades"] += 1
        if rr_f is not None:
            bg["rr_list"].append(rr_f)
        if pnl_f is not None:
            bg["pnl_list"].append(pnl_f)

        # per strategy+symbol bucket
        bs = _bucket_sym(by_strategy_symbol, strat_id, strat_name, sub_uid, symbol)
        bs["trades"] += 1
        if result == "WIN":
            bs["wins"] += 1
        elif result == "LOSS":
            bs["losses"] += 1
        elif result == "BREAKEVEN":
            bs["breakevens"] += 1
        if label_good:
            bs["good_trades"] += 1
        if rr_f is not None:
            bs["rr_list"].append(rr_f)
        if pnl_f is not None:
            bs["pnl_list"].append(pnl_f)

    # finalize
    def _finalize_bucket(b: Dict[str, Any]) -> None:
        rr_list = b.pop("rr_list", [])
        pnl_list = b.pop("pnl_list", [])

        trades = b.get("trades", 0)
        wins = b.get("wins", 0)
        good_trades = b.get("good_trades", 0)

        b["winrate"] = (wins / trades) if trades > 0 else None
        b["good_rate"] = (good_trades / trades) if trades > 0 else None
        b["avg_rr"] = (sum(rr_list) / len(rr_list)) if rr_list else None
        b["median_rr"] = statistics.median(rr_list) if rr_list else None
        b["avg_pnl_usd"] = (sum(pnl_list) / len(pnl_list)) if pnl_list else None

    for sid, bucket in by_strategy.items():
        _finalize_bucket(bucket)

    for sid, sym_map in by_strategy_symbol.items():
        for sym, bucket in sym_map.items():
            _finalize_bucket(bucket)

    return {
        "by_strategy": by_strategy,
        "by_strategy_symbol": by_strategy_symbol,
    }


# ---------- main ----------

def run() -> None:
    print("[setup_memory_stats] Loading setup_memory.jsonl ...")
    rows = _load_jsonl(MEMORY_PATH)
    print(f"[setup_memory_stats] Loaded {len(rows)} labeled examples.")

    if not rows:
        print("[setup_memory_stats] No data yet. Once trades are logged + merged, "
              "re-run this script.")
        return

    summary = build_stats(rows)

    # write out summary JSON
    SUMMARY_PATH.write_bytes(orjson.dumps(summary, option=orjson.OPT_INDENT_2))
    print(f"[setup_memory_stats] Summary written to: {SUMMARY_PATH}")

    # concise ranking
    print("\n=== Strategy ranking (by winrate, then avg RR) ===")
    bs = summary.get("by_strategy", {})
    ranks = []
    for sid, v in bs.items():
        trades = v.get("trades", 0)
        winrate = _safe_float(v.get("winrate"))
        avg_rr = _safe_float(v.get("avg_rr"))
        avg_pnl = _safe_float(v.get("avg_pnl_usd"))
        good_rate = _safe_float(v.get("good_rate"))
        name = v.get("strategy") or sid
        ranks.append((sid, name, trades, winrate, avg_rr, avg_pnl, good_rate))

    ranks.sort(
        key=lambda row: (
            row[3] if row[3] is not None else 0.0,  # winrate
            row[4] if row[4] is not None else 0.0,  # avg_rr
        ),
        reverse=True,
    )

    for sid, name, trades, winrate, avg_rr, avg_pnl, good_rate in ranks:
        line = f"- {sid} ({name}): trades={trades}"
        if winrate is not None:
            line += f", winrate={winrate:.2%}"
        else:
            line += ", winrate=n/a"
        if avg_rr is not None:
            line += f", avgRR={avg_rr:.2f}"
        if avg_pnl is not None:
            line += f", avgPnL={avg_pnl:.2f} usd"
        if good_rate is not None:
            line += f", good_rate={good_rate:.2%}"
        print(line)

    print("\n[setup_memory_stats] Done.")


if __name__ == "__main__":
    run()
