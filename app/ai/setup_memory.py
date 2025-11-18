#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Setup Memory Builder (v1.1)

Purpose
-------
Merge:
  - Trade-open feature snapshots (state/features_trades.jsonl)
  - Closed trade outcomes        (state/journal.jsonl)

Into:
  - state/setup_memory.jsonl

Each merged row is a single labeled training example:
  {
    "ts_open_ms": ...,
    "symbol": "...",
    "sub_uid": "...",
    "strategy": "...",
    "mode": "LIVE_CANARY" | "LIVE_FULL" | "PAPER",
    "features": {...},           # from feature_store
    "signal": {...},             # raw signal
    "ai_score": float,
    "ai_reason": "...",
    "risk_usd": float,
    "risk_pct": float,
    "equity_usd": float,
    "result": "WIN"/"LOSS"/"BREAKEVEN"/"UNKNOWN",
    "realized_pnl": float,
    "realized_rr": float or null,
    "rating_score": int,
    "rating_reason": str,
    "duration_ms": int,
    "duration_human": str,
    "label_win": bool,
    "label_good": bool,          # e.g. rating >= 7
    "label_rr_ge_1": bool,       # RR >= 1.0
    "order_link_id": str | null
  }

Matching logic
--------------
Primary:
- Join by order_link_id:
    • feature.order_link_id == journal.order_link_id

Fallback (for old rows with no link id):
- Match by symbol AND time:
    • For each closed trade in journal.jsonl:
        - take its ts_open (if present, else ts_close)
        - find the nearest feature snapshot with the same symbol
          where |ts_open - ts_feature| <= MATCH_WINDOW_MS (default 10 min)
        - use each feature row at most once.

Files
-----
Input:
  - ROOT/state/journal.jsonl
  - ROOT/state/features_trades.jsonl

Output:
  - ROOT/state/setup_memory.jsonl
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Set

import orjson

# -------- Paths / constants -------- #

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])

STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

JOURNAL_PATH = STATE_DIR / "journal.jsonl"
FEATURES_PATH = STATE_DIR / "features_trades.jsonl"
OUTPUT_PATH = STATE_DIR / "setup_memory.jsonl"

MATCH_WINDOW_MS = 10 * 60 * 1000  # 10 minutes


# -------- Small utils -------- #

def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if isinstance(x, Decimal):
            return float(x)
        return float(x)
    except Exception:
        return None


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        print(f"[setup_memory] WARNING: {path} does not exist, returning empty list.")
        return []
    rows: List[Dict[str, Any]] = []
    with path.open("rb") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = orjson.loads(line)
                if isinstance(row, dict):
                    rows.append(row)
            except Exception:
                continue
    return rows


def _get_ts(row: Dict[str, Any], keys: List[str]) -> Optional[int]:
    for k in keys:
        v = row.get(k)
        if v is None:
            continue
        try:
            iv = int(v)
            if iv > 0:
                return iv
        except Exception:
            continue
    return None


# -------- Matching logic (fallback: symbol + time) -------- #

def _index_features_by_symbol(features: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    by_sym: Dict[str, List[Dict[str, Any]]] = {}
    for r in features:
        sym = str(r.get("symbol", "")).upper().strip()
        if not sym:
            continue
        ts = _get_ts(r, ["ts_ms"])
        if ts is None:
            continue
        r["_ts"] = ts
        by_sym.setdefault(sym, []).append(r)

    # sort per symbol by time ascending
    for sym, rows in by_sym.items():
        rows.sort(key=lambda x: x["_ts"])
    return by_sym


def _find_best_feature_match(
    trade: Dict[str, Any],
    feature_rows: List[Dict[str, Any]],
    used_ids: Set[Tuple[int, int]],
) -> Optional[Dict[str, Any]]:
    """
    trade: a journal row
    feature_rows: all feature rows for that symbol (sorted by ts)
    used_ids: set of ids (index-based or object ids) already matched
    """
    ts_trade = _get_ts(trade, ["ts_open", "ts_close", "ts_ms"])
    if ts_trade is None:
        return None

    best_row = None
    best_delta = None

    for idx, r in enumerate(feature_rows):
        row_id = (id(r), idx)
        if row_id in used_ids:
            continue

        ts_feat = r.get("_ts")
        if ts_feat is None:
            continue

        delta = abs(ts_feat - ts_trade)
        if delta > MATCH_WINDOW_MS:
            continue

        if best_delta is None or delta < best_delta:
            best_delta = delta
            best_row = (idx, r, row_id)

    if best_row is None:
        return None

    idx, r, row_id = best_row
    used_ids.add(row_id)
    return r


# -------- Label building -------- #

def _build_labels_from_trade(trade: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract clean numeric labels from a journal row.
    """
    pnl = trade.get("realized_pnl")
    rr = trade.get("realized_rr")
    rating = trade.get("rating_score")
    result = (trade.get("result") or "UNKNOWN").upper()

    pnl_f = None
    rr_f = None
    try:
        if pnl is not None:
            pnl_f = float(pnl)
    except Exception:
        pnl_f = None
    try:
        if rr is not None:
            rr_f = float(rr)
    except Exception:
        rr_f = None

    try:
        rating_i = int(rating) if rating is not None else None
    except Exception:
        rating_i = None

    label_win = result == "WIN"
    label_good = rating_i is not None and rating_i >= 7
    label_rr_ge_1 = rr_f is not None and rr_f >= 1.0

    return {
        "result": result,
        "realized_pnl": pnl_f,
        "realized_rr": rr_f,
        "rating_score": rating_i,
        "rating_reason": trade.get("rating_reason"),
        "duration_ms": trade.get("duration_ms"),
        "duration_human": trade.get("duration_human"),

        "label_win": label_win,
        "label_good": label_good,
        "label_rr_ge_1": label_rr_ge_1,
    }


def _merge_feature_and_trade(
    feat: Dict[str, Any],
    trade: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Build a single training row from:
      - feature snapshot (feat)
      - journal row (trade)
    """
    sym = str(feat.get("symbol", "")).upper().strip()
    sub_uid = str(feat.get("sub_uid") or "")
    strat = str(feat.get("strategy") or "")
    strat_id = feat.get("strategy_id")

    mode = str(feat.get("mode") or "").upper()
    equity = feat.get("equity_usd")
    risk_usd = feat.get("risk_usd")
    risk_pct = feat.get("risk_pct")

    ai_score = feat.get("ai_score")
    ai_reason = feat.get("ai_reason")

    ts_open = feat.get("ts_ms")
    ts_iso = feat.get("ts_iso")

    order_link_id = feat.get("order_link_id") or trade.get("order_link_id")

    labels = _build_labels_from_trade(trade)

    row = {
        # identifiers
        "symbol": sym,
        "sub_uid": sub_uid,
        "strategy": strat,
        "strategy_id": strat_id,
        "mode": mode,
        "order_link_id": order_link_id,

        # timing
        "ts_open_ms": ts_open,
        "ts_open_iso": ts_iso,
        "ts_close_ms": trade.get("ts_close"),
        "ts_close_iso": trade.get("ts_close_iso"),

        # risk / equity
        "equity_usd": _to_float(equity),
        "risk_usd": _to_float(risk_usd),
        "risk_pct": _to_float(risk_pct),

        # AI gate
        "ai_score": _to_float(ai_score),
        "ai_reason": ai_reason,

        # signal & features
        "signal_reason": feat.get("signal_reason"),
        "signal_timeframe": feat.get("signal_timeframe"),
        "signal_raw": feat.get("signal_raw"),
        "features": feat.get("features") or {},

        # outcome labels
        **labels,
    }
    return row


# -------- Main build function -------- #

def build_memory() -> None:
    print("[setup_memory] Loading journal & feature snapshots...")

    journal_rows = _load_jsonl(JOURNAL_PATH)
    feature_rows = _load_jsonl(FEATURES_PATH)

    print(f"[setup_memory] Loaded {len(journal_rows)} journal rows.")
    print(f"[setup_memory] Loaded {len(feature_rows)} feature rows.")

    # --- Primary join: order_link_id ---
    feat_by_lid: Dict[str, Dict[str, Any]] = {}
    for fr in feature_rows:
        lid = fr.get("order_link_id")
        if not lid:
            continue
        feat_by_lid.setdefault(str(lid), fr)

    used_feature_ids: Set[int] = set()
    merged: List[Dict[str, Any]] = []

    remaining_trades: List[Dict[str, Any]] = []
    matched_by_link = 0

    for trade in journal_rows:
        lid = trade.get("order_link_id")
        if lid:
            fr = feat_by_lid.get(str(lid))
            if fr is not None:
                merged.append(_merge_feature_and_trade(fr, trade))
                used_feature_ids.add(id(fr))
                matched_by_link += 1
                continue
        # no match via link_id -> handle later via fallback
        remaining_trades.append(trade)

    # --- Fallback join: symbol + time window, for trades without valid link id ---
    fallback_unmatched = 0
    if remaining_trades:
        # use only features not already consumed by link matches
        remaining_features = [fr for fr in feature_rows if id(fr) not in used_feature_ids]
        by_sym = _index_features_by_symbol(remaining_features)
        used_ids: Set[Tuple[int, int]] = set()

        for trade in remaining_trades:
            sym = str(trade.get("symbol", "")).upper().strip()
            if not sym:
                fallback_unmatched += 1
                continue

            feats_for_sym = by_sym.get(sym)
            if not feats_for_sym:
                fallback_unmatched += 1
                continue

            best_feat = _find_best_feature_match(trade, feats_for_sym, used_ids)
            if best_feat is None:
                fallback_unmatched += 1
                continue

            merged.append(_merge_feature_and_trade(best_feat, trade))

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_PATH.open("wb") as f:
        for row in merged:
            f.write(orjson.dumps(row) + b"\n")

    print(f"[setup_memory] Merged {len(merged)} trades into {OUTPUT_PATH}.")
    print(f"[setup_memory] Matched via order_link_id: {matched_by_link}")
    print(f"[setup_memory] Fallback unmatched trades (symbol+time failed): {fallback_unmatched}")
    print(f"[setup_memory] Done.")


if __name__ == "__main__":
    build_memory()
