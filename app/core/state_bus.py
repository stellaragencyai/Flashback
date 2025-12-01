#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — State Bus (Centralized State Engine, v2)

Purpose
-------
Provide a simple, centralized way for all bots to read/write shared state
without each one reinventing file paths or ad-hoc JSON dumps.

Concepts:
- "topics" are named buckets of state, e.g.:
    • positions_main
    • balances_main
    • heartbeats
    • tier_state
- Each topic is stored as a dict[key -> value] in:
      state/<topic>.json
  Internally, each key is stored as:
      { "_value": <actual_value>, "_expires_ms": <int|None> }

- For append-only logs (executions, events, etc) we support:
      state/<topic>.jsonl

This is intentionally simple and file-backed. It is NOT a database, but it
gives Flashback a single source of truth for key runtime state.

v2 Enhancements (backward compatible)
-------------------------------------
- Topic metadata:
    • Each topic stores _meta.updated_ms (last write timestamp, ms).
- TTL support:
    • set(..., ttl=seconds) and set_group(..., ttl=...) auto-expire keys.
- Group helpers:
    • set_group / get_group / all_group around "group_<name>" topics.
- Metrics helpers:
    • set_metric / get_metric / increment_metric.
- Multi-key + pipe helpers:
    • set_multi(topic, {k: v, ...}, ttl=...) writes multiple keys atomically.
    • pipe([(topic, key, value), ...]) for cross-topic batches.
- Basic schema validations (soft warnings) for positions/balances.
- Optional subscriptions:
    • subscribe(topic, callback(topic, key, value)) for local hooks.
- Debug snapshot:
    • debug_snapshot(path=None) dumps all topic dicts into one file/dict.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any, Dict, Optional, Iterable, Callable, List, Tuple

import orjson

try:
    from app.core.config import settings  # type: ignore
except ImportError:
    # Fallback if imported as core.state_bus
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])
STATE_DIR: Path = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)


def _now_ms() -> int:
    return int(time.time() * 1000)


class StateBus:
    """
    Centralized, file-backed state engine.

    - Topic stores (dict) are in: state/<topic>.json
      Internally:
        {
          "_meta": {"updated_ms": <int>},
          "data": {
            "key": {"_value": <actual>, "_expires_ms": <int|None>},
            ...
          }
        }

      Backward compatible with the old format where the file was just:
        { "k": v, "k2": v2, ... }

    - Append logs (stream) are in: state/<topic>.jsonl
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # topic -> list[callback(topic, key, value)]
        self._subscribers: Dict[str, List[Callable[[str, str, Any], None]]] = {}

    # ---------- internal helpers ----------

    def _topic_path(self, topic: str) -> Path:
        return STATE_DIR / f"{topic}.json"

    def _log_path(self, topic: str) -> Path:
        return STATE_DIR / f"{topic}.jsonl"

    def _load_topic_struct(self, topic: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Load the *structured* representation:

            meta: {"updated_ms": int} (may be empty)
            data: {key: {"_value": ..., "_expires_ms": int|None}, ...}

        Handles both:
          - new format with {"_meta": ..., "data": {...}}
          - old format where file is simply {key: value, ...}

        Also prunes TTL-expired entries (if _expires_ms < now).
        """
        path = self._topic_path(topic)
        if not path.exists():
            return {}, {}

        try:
            raw = orjson.loads(path.read_bytes())
        except Exception:
            return {}, {}

        meta: Dict[str, Any] = {}
        raw_data: Dict[str, Any]

        if isinstance(raw, dict) and "data" in raw and isinstance(raw["data"], dict):
            # New structured format
            meta = raw.get("_meta", {}) or {}
            raw_data = raw["data"]
        elif isinstance(raw, dict):
            # Old format: entire dict is the data
            meta = {}
            raw_data = raw
        else:
            return {}, {}

        now = _now_ms()
        changed = False
        data: Dict[str, Any] = {}

        for key, val in raw_data.items():
            actual = val
            expires_ms: Optional[int] = None

            # Normalize from new or old representation
            if isinstance(val, dict) and "_value" in val:
                actual = val.get("_value")
                exp = val.get("_expires_ms")
                if isinstance(exp, (int, float)) and exp > 0:
                    expires_ms = int(exp)
            else:
                # Old style value; wrap it
                changed = True
                expires_ms = None

            # TTL pruning
            if expires_ms is not None and expires_ms > 0 and now > expires_ms:
                # Expired; drop it
                changed = True
                continue

            entry = {"_value": actual}
            if expires_ms is not None:
                entry["_expires_ms"] = expires_ms
            data[key] = entry

        if changed:
            # Save normalized/cleaned representation
            self._save_topic_struct(topic, meta, data)

        return meta, data

    def _save_topic_struct(self, topic: str, meta: Dict[str, Any], data: Dict[str, Any]) -> None:
        """
        Save structured representation:
            {"_meta": {...}, "data": {...}}
        """
        path = self._topic_path(topic)
        path.parent.mkdir(parents=True, exist_ok=True)
        body = {"_meta": meta or {}, "data": data or {}}
        path.write_bytes(orjson.dumps(body))

    def _save_topic_values(
        self,
        topic: str,
        values: Dict[str, Any],
        ttl_map: Optional[Dict[str, Optional[int]]] = None,
    ) -> None:
        """
        Save a dict of *plain* values for keys, optionally with per-key TTL (seconds).

        ttl_map: key -> ttl_seconds | None
        """
        meta, data = self._load_topic_struct(topic)
        now_ms = _now_ms()

        if meta is None:
            meta = {}
        meta["updated_ms"] = now_ms

        if ttl_map is None:
            ttl_map = {}

        for key, value in values.items():
            ttl_sec = ttl_map.get(key)
            expires_ms: Optional[int] = None
            if ttl_sec is not None and ttl_sec > 0:
                expires_ms = now_ms + int(ttl_sec * 1000)

            entry = {"_value": value}
            if expires_ms is not None:
                entry["_expires_ms"] = expires_ms
            data[key] = entry

        self._save_topic_struct(topic, meta, data)

    def _topic_plain_dict(self, topic: str) -> Dict[str, Any]:
        """
        Return the plain {key -> value} dict for a topic,
        after TTL pruning and normalization.
        """
        _meta, data = self._load_topic_struct(topic)
        plain: Dict[str, Any] = {}
        for key, entry in data.items():
            if isinstance(entry, dict) and "_value" in entry:
                plain[key] = entry["_value"]
            else:
                plain[key] = entry
        return plain

    def _notify_subscribers(self, topic: str, key: str, value: Any) -> None:
        """
        Call any subscribed callbacks for (topic, key, value).
        """
        subs = self._subscribers.get(topic)
        if not subs:
            return
        for cb in list(subs):
            try:
                cb(topic, key, value)
            except Exception as e:
                # No logging infra here; local-print is enough.
                print(f"[StateBus] subscriber error for topic={topic}, key={key}: {e}")

    # ---------- dict-style API ----------

    def get(self, topic: str, key: str, default: Optional[Any] = None) -> Any:
        """
        Get a single key from a topic dict (plain value).
        TTL-expired entries are automatically pruned.
        """
        with self._lock:
            data = self._topic_plain_dict(topic)
            return data.get(key, default)

    def set(self, topic: str, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Set a single key in a topic dict.
        Optional ttl in seconds; if provided, key auto-expires.
        """
        with self._lock:
            self._save_topic_values(topic, {key: value}, ttl_map={key: ttl})
            self._notify_subscribers(topic, key, value)

    def set_multi(self, topic: str, values: Dict[str, Any], ttl: Optional[int] = None) -> None:
        """
        Atomically set multiple keys in a topic.
        Optional ttl applies to all keys.
        """
        ttl_map: Dict[str, Optional[int]] = {k: ttl for k in values.keys()}
        with self._lock:
            self._save_topic_values(topic, values, ttl_map=ttl_map)
            for k, v in values.items():
                self._notify_subscribers(topic, k, v)

    def delete(self, topic: str, key: str) -> None:
        """
        Delete a key from a topic dict.
        """
        with self._lock:
            meta, data = self._load_topic_struct(topic)
            if key in data:
                data.pop(key, None)
                meta["updated_ms"] = _now_ms()
                self._save_topic_struct(topic, meta, data)
                self._notify_subscribers(topic, key, None)

    def all(self, topic: str) -> Dict[str, Any]:
        """
        Return the entire dict for a topic (plain values).
        """
        with self._lock:
            return self._topic_plain_dict(topic)

    def keys(self, topic: str) -> Iterable[str]:
        """
        Return all keys for a topic.
        """
        with self._lock:
            data = self._topic_plain_dict(topic)
            return list(data.keys())

    # ---------- append-log API (jsonl) ----------

    def append_log(self, topic: str, row: Dict[str, Any]) -> None:
        """
        Append a single JSON row to state/<topic>.jsonl.
        """
        path = self._log_path(topic)
        path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            with path.open("ab") as f:
                f.write(orjson.dumps(row) + b"\n")

    # ---------- convenience helpers for Flashback (positions / balances) ----------

    @staticmethod
    def _validate_position(symbol: str, position: Dict[str, Any]) -> None:
        """
        Soft validation for position shape. Logs warnings only.
        """
        try:
            if "side" not in position or "size" not in position:
                print(f"[StateBus] WARNING: position for {symbol} missing side/size fields.")
        except Exception:
            pass

    @staticmethod
    def _validate_balance(asset: str, balance: Dict[str, Any]) -> None:
        """
        Soft validation for balance shape. Logs warnings only.
        """
        try:
            if "equity" not in balance and "walletBalance" not in balance:
                print(f"[StateBus] WARNING: balance for {asset} missing equity/walletBalance fields.")
        except Exception:
            pass

    def set_position(self, account_label: str, symbol: str, position: Dict[str, Any]) -> None:
        """
        Store a normalized position for (account_label, symbol).
        Topic name: positions_<account_label.lower()>
        Key: symbol
        """
        topic = f"positions_{account_label.lower()}"
        self._validate_position(symbol, position)
        self.set(topic, symbol, position)

    def all_positions(self, account_label: str) -> Dict[str, Any]:
        topic = f"positions_{account_label.lower()}"
        return self.all(topic)

    def set_balance(self, account_label: str, asset: str, balance: Dict[str, Any]) -> None:
        """
        Store a normalized balance for (account_label, asset).
        Topic name: balances_<account_label.lower()>
        Key: asset
        """
        topic = f"balances_{account_label.lower()}"
        self._validate_balance(asset, balance)
        self.set(topic, asset, balance)

    def all_balances(self, account_label: str) -> Dict[str, Any]:
        topic = f"balances_{account_label.lower()}"
        return self.all(topic)

    def log_ws_execution(self, label: str, ts_ms: int, row: Dict[str, Any]) -> None:
        """
        Backward compatible: writes to state/ws_executions.jsonl with the
        same shape trade_journal expects:
            { "label": "<label>", "ts": <epoch_ms>, "row": { ...exec row... } }
        """
        payload = {
            "label": label,
            "ts": ts_ms,
            "row": row,
        }
        # Keep the historical file name so journal continues to work.
        self.append_log("ws_executions", payload)

    # ---------- group helpers (namespaced topics) ----------

    def set_group(self, group: str, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """
        Grouped state:
          group "safety", key "risk_guardian" -> topic "group_safety"
        """
        topic = f"group_{group}"
        self.set(topic, key, value, ttl=ttl)

    def get_group(self, group: str, key: str, default: Optional[Any] = None) -> Any:
        topic = f"group_{group}"
        return self.get(topic, key, default=default)

    def all_group(self, group: str) -> Dict[str, Any]:
        topic = f"group_{group}"
        return self.all(topic)

    # ---------- metrics helpers ----------

    def set_metric(self, component: str, name: str, value: Any) -> None:
        """
        Store a metric as:
          topic="metrics", key="component:name"
        """
        topic = "metrics"
        key = f"{component}:{name}"
        self.set(topic, key, value)

    def get_metric(self, component: str, name: str, default: Optional[Any] = None) -> Any:
        topic = "metrics"
        key = f"{component}:{name}"
        return self.get(topic, key, default=default)

    def increment_metric(self, component: str, name: str, delta: float = 1.0) -> None:
        """
        Increment numeric metric by delta. If missing or non-numeric, treat as 0.
        """
        topic = "metrics"
        key = f"{component}:{name}"
        cur = self.get(topic, key, 0)
        try:
            cur_num = float(cur)
        except Exception:
            cur_num = 0.0
        new_val = cur_num + float(delta)
        self.set(topic, key, new_val)

    # ---------- pipe / batch helpers ----------

    def pipe(self, operations: Iterable[Tuple[str, str, Any]], ttl: Optional[int] = None) -> None:
        """
        Basic cross-topic batch setter.

        operations: iterable of (topic, key, value)
        Optional ttl (seconds) applied to all keys.
        """
        for topic, key, value in operations:
            self.set(topic, key, value, ttl=ttl)

    # ---------- subscriptions ----------

    def subscribe(self, topic: str, callback: Callable[[str, str, Any], None]) -> None:
        """
        Subscribe to updates on a topic.

        callback(topic, key, value) is called AFTER the topic is written.
        """
        with self._lock:
            self._subscribers.setdefault(topic, []).append(callback)

    # ---------- debug snapshot ----------

    def debug_snapshot(self, file_path: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
        """
        Create a debug snapshot of *all* JSON topic files in state/.

        Returns:
          {topic_name: {key: value, ...}, ...}

        If file_path is provided, writes the snapshot there as JSON.
        """
        snapshot: Dict[str, Dict[str, Any]] = {}
        with self._lock:
            for p in STATE_DIR.glob("*.json"):
                topic = p.stem
                try:
                    snapshot[topic] = self._topic_plain_dict(topic)
                except Exception as e:
                    print(f"[StateBus] snapshot load failed for {topic}: {e}")

        if file_path:
            try:
                out_path = Path(file_path)
                out_path.parent.mkdir(parents=True, exist_ok=True)
                out_path.write_bytes(orjson.dumps(snapshot))
            except Exception as e:
                print(f"[StateBus] snapshot write failed: {e}")

        return snapshot


# Provide a simple shared instance
bus = StateBus()
