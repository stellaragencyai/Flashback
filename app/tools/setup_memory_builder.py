#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — AI Setup Memory Builder (v1)

Purpose
-------
Merge:
  - Trade-open feature snapshots (state/features_trades.jsonl)
  - Closed trade outcomes        (app/state/journal.jsonl or state/journal.jsonl)

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
    "realized_pnl": float | null,
    "realized_R": float | null
  }

This is *read-only*: it never touches Bybit or Telegram.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]

FEATURES_PATH = ROOT / "state" / "features_trades.jsonl"
JOURNAL_PATHS = [
    ROOT / "app" / "state" / "journal.jsonl",
    ROOT / "state" / "journal.jsonl",
]

OUTPUT_PATH = ROOT / "state" / "setup_memory.jsonl"

# Max allowed gap between feature open timestamp and journal open timestamp
# when matching, in milliseconds (e.g. 10 minutes).
MAX_TS_GAP_MS = 10 * 60 * 1000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_decimal(x: Any, default: Decimal = Decimal("0")) -> Decimal:
    try:
        if x is None:
            return default
        return Decimal(str(x))
    except Exception:
        return default


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                out.append(json.loads(line))
            except Exception:
                continue
    return out


def _first_existing(paths: List[Path]) -> Optional[Path]:
    for p in paths:
        if p.exists():
            return p
    return None


def _extract_ts_open_ms(row: Dict[str, Any]) -> Optional[int]:
    """
    Try to pull an 'open timestamp in ms' from a row, being forgiving
    about key names. Returns None if nothing sane is found.
    """
    candidates = [
        "ts_open_ms",
        "ts_ms",
        "open_ts_ms",
        "ts_open",
        "ts",
    ]
    for k in candidates:
        if k in row:
            try:
                v = int(row[k])
                if v > 0:
                    return v
            except Exception:
                continue
    return None


def _extract_result_label(j: Dict[str, Any]) -> Tuple[str, Optional[Decimal], Optional[Decimal]]:
    """
    Extract a coarse result label and (pnl, R) if present.

    We try several common field names because your logging is,
    let's say, "evolutionary."
    """
    pnl_keys = ["realized_pnl", "pnl", "pnl_usd", "realizedPnl"]
    r_keys = ["realized_R", "R", "risk_R"]

    pnl = None
    R = None

    for k in pnl_keys:
        if k in j:
            pnl = _safe_decimal(j.get(k))
            break

    for k in r_keys:
        if k in j:
            R = _safe_decimal(j.get(k))
            break

    # If R missing but pnl + risk_usd present, you can reconstruct later if needed
    # For now we just label result by pnl when available
    if pnl is None:
        label = j.get("result") or j.get("label") or "UNKNOWN"
        label = str(label).upper()
        if label not in ("WIN", "LOSS", "BREAKEVEN", "UNKNOWN"):
            label = "UNKNOWN"
        return label, None, None

    if pnl > 0:
        label = "WIN"
    elif pnl < 0:
        label = "LOSS"
    else:
        label = "BREAKEVEN"

    return label, pnl, R


@dataclass
class FeatureRow:
    raw: Dict[str, Any]
    ts_open_ms: Optional[int]
    sub_uid: Optional[str]
    symbol: Optional[str]
    side: Optional[str]
    strategy: Optional[str]


@dataclass
class JournalRow:
    raw: Dict[str, Any]
    ts_open_ms: Optional[int]
    sub_uid: Optional[str]
    symbol: Optional[str]
    side: Optional[str]
    strategy: Optional[str]
    label: str
    pnl: Optional[Decimal]
    R: Optional[Decimal]


# ---------------------------------------------------------------------------
# Load + index
# ---------------------------------------------------------------------------

def load_features() -> List[FeatureRow]:
    rows = _load_jsonl(FEATURES_PATH)
    out: List[FeatureRow] = []
    for r in rows:
        ts_open_ms = _extract_ts_open_ms(r)
        sub_uid = str(r.get("sub_uid") or r.get("subUid") or "") or None
        symbol = r.get("symbol") or r.get("sym")
        side = r.get("side") or r.get("direction")
        strategy = r.get("strategy") or r.get("strategy_name")
        out.append(
            FeatureRow(
                raw=r,
                ts_open_ms=ts_open_ms,
                sub_uid=sub_uid,
                symbol=symbol,
                side=side,
                strategy=strategy,
            )
        )
    return out


def load_journal() -> List[JournalRow]:
    path = _first_existing(JOURNAL_PATHS)
    if path is None:
        return []

    rows = _load_jsonl(path)
    out: List[JournalRow] = []
    for r in rows:
        ts_open_ms = _extract_ts_open_ms(r)
        sub_uid = str(r.get("sub_uid") or r.get("subUid") or "") or None
        symbol = r.get("symbol") or r.get("sym")
        side = r.get("side") or r.get("direction")
        strategy = r.get("strategy") or r.get("strategy_name")

        label, pnl, R = _extract_result_label(r)

        out.append(
            JournalRow(
                raw=r,
                ts_open_ms=ts_open_ms,
                sub_uid=sub_uid,
                symbol=symbol,
                side=side,
                strategy=strategy,
                label=label,
                pnl=pnl,
                R=R,
            )
        )
    return out


def build_journal_index(journal_rows: List[JournalRow]) -> Dict[Tuple[str, str, str], List[JournalRow]]:
    """
    Index journal rows by (sub_uid, symbol, side). If sub_uid is missing,
    we just store "" there.
    """
    idx: Dict[Tuple[str, str, str], List[JournalRow]] = {}
    for j in journal_rows:
        key = (
            j.sub_uid or "",
            j.symbol or "",
            (j.side or "").upper(),
        )
        idx.setdefault(key, []).append(j)

    # sort each bucket by ts_open_ms for faster nearest matching
    for key, bucket in idx.items():
        bucket.sort(key=lambda r: (r.ts_open_ms or 0))
    return idx


# ---------------------------------------------------------------------------
# Matching logic
# ---------------------------------------------------------------------------

def match_feature_to_journal(
    f: FeatureRow,
    idx: Dict[Tuple[str, str, str], List[JournalRow]],
) -> Optional[JournalRow]:
    if f.symbol is None or f.side is None:
        return None
    key_candidates: List[Tuple[str, str, str]] = []

    side_u = f.side.upper()
    sub_uid = f.sub_uid or ""

    # exact sub_uid match first
    key_candidates.append((sub_uid, f.symbol, side_u))
    # fallback: ignore sub_uid if nothing is found
    if sub_uid:
        key_candidates.append(("", f.symbol, side_u))

    for key in key_candidates:
        bucket = idx.get(key)
        if not bucket:
            continue

        # no timestamp? impossible to match reliably
        if f.ts_open_ms is None:
            # brute-force, just return latest in bucket (not ideal, but better than nothing)
            return bucket[-1]

        best: Optional[JournalRow] = None
        best_gap = MAX_TS_GAP_MS + 1

        for j in bucket:
            if j.ts_open_ms is None:
                continue
            gap = abs(j.ts_open_ms - f.ts_open_ms)
            if gap < best_gap:
                best_gap = gap
                best = j

        if best is not None and best_gap <= MAX_TS_GAP_MS:
            return best

    return None


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

def build_setup_memory() -> None:
    features = load_features()
    journal = load_journal()

    if not features:
        print(f"[setup_memory] No features found at {FEATURES_PATH}")
        return

    if not journal:
        print("[setup_memory] WARNING: No journal file found; all results will be UNKNOWN.")

    j_index = build_journal_index(journal)

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_f = OUTPUT_PATH.open("wb")

    total = 0
    matched = 0
    unknown = 0

    for f in features:
        total += 1
        j = match_feature_to_journal(f, j_index)

        if j is None:
            label = "UNKNOWN"
            pnl = None
            R = None
            unknown += 1
        else:
            label = j.label
            pnl = j.pnl
            R = j.R
            matched += 1

        raw_f = dict(f.raw)

        # Normalize fields
        ts_open_ms = _extract_ts_open_ms(raw_f) or (j.ts_open_ms if j else None)

        out_row: Dict[str, Any] = {
            "ts_open_ms": ts_open_ms,
            "symbol": f.symbol,
            "sub_uid": f.sub_uid,
            "strategy": f.strategy,
            "mode": raw_f.get("mode") or raw_f.get("trade_mode") or "UNKNOWN",
            "features": raw_f.get("features") or {},
            "signal": raw_f.get("signal") or {},
            "ai_score": raw_f.get("ai_score"),
            "ai_reason": raw_f.get("ai_reason"),
            "risk_usd": raw_f.get("risk_usd"),
            "risk_pct": raw_f.get("risk_pct"),
            "equity_usd": raw_f.get("equity_usd"),
            "result": label,
            "realized_pnl": float(pnl) if isinstance(pnl, Decimal) else pnl,
            "realized_R": float(R) if isinstance(R, Decimal) else R,
        }

        line = json.dumps(out_row, default=str).encode("utf-8")
        out_f.write(line + b"\n")

    out_f.close()

    print(
        f"[setup_memory] Done. total={total}, matched={matched}, unknown={unknown}, "
        f"output={OUTPUT_PATH}"
    )


if __name__ == "__main__":
    build_setup_memory()
