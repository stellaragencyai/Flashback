#!/usr/bin/env python3
# app/strategies/canary.py
from __future__ import annotations

from typing import List, Dict, Any
from decimal import Decimal
import orjson
from pathlib import Path

SETUP_MEMORY_PATH = Path("state/setup_memory.jsonl")

def load_top_canary_patterns(limit: int = 50) -> List[Dict[str, Any]]:
    """
    Load top patterns where sub_uid or strategy indicates it's from Sub7
    and rating / RR is high.
    """
    rows: List[Dict[str, Any]] = []
    if not SETUP_MEMORY_PATH.exists():
        return rows
    with SETUP_MEMORY_PATH.open("rb") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                row = orjson.loads(line)
            except Exception:
                continue
            if str(row.get("sub_uid")) != "7":  # adjust based on your actual UID
                continue
            rating = int(row.get("rating_score", 0) or 0)
            rr = row.get("realized_rr")
            try:
                rr_val = float(rr) if rr is not None else 0.0
            except Exception:
                rr_val = 0.0
            if rating >= 7 and rr_val >= 1.0:
                rows.append(row)
    # naive truncation
    return rows[:limit]

def canary_signal_decision(current_features: Dict[str, Any]) -> bool:
    """
    Placeholder:
      - compare current_features to stored top patterns (later via embeddings)
      - for now, dumb always-True stub
    """
    # TODO: similarity search using vectors
    return True
