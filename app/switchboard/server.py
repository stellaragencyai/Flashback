#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard (v1)

Purpose
-------
Central WebSocket hub for all accounts (MAIN + subaccounts).

- Opens ONE Bybit private WebSocket per configured account.
- Authenticates and subscribes to:
    • "position"  (private positions stream)
    • "execution" (fills on that account)
- Tags each message with `account_label` and writes to an append-only JSONL file:

      state/ws_<label>.jsonl   e.g. ws_MAIN.jsonl, ws_SUB_1.jsonl

This lets all bots read live events from a single, shared firehose instead of
each bot opening its own WS connection.

Environment
-----------
- BYBIT_WS_PRIVATE_URL   (default wss://stream.bybit.com/v5/private)
- For each account, we look for these env vars:

    MAIN:
        BYBIT_MAIN_TRADE_KEY
        BYBIT_MAIN_TRADE_SECRET

    SUB_1 .. SUB_10:
        BYBIT_SUB_1_TRADE_KEY, BYBIT_SUB_1_TRADE_SECRET
        ...
        BYBIT_SUB_10_TRADE_KEY, BYBIT_SUB_10_TRADE_SECRET

Accounts without both key+secret are silently skipped.

Usage
-----
Run under supervisor as:

    module: app.switchboard.server
    entry:  loop()

Bots can then tail / process state/ws_<label>.jsonl as needed.
"""

from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Dict, Any, List

import orjson

# Reuse websocket-client (same lib tp_sl_manager uses)
try:
    import websocket  # type: ignore
except ImportError as e:  # pragma: no cover
    raise RuntimeError(
        "websocket-client is required for WS switchboard. "
        "Install with: pip install websocket-client"
    ) from e

# Optional notifier (main channel) for diagnostics; tolerant import
try:
    from app.core.notifier_bot import get_notifier
except ImportError:  # pragma: no cover
    from core.notifier_bot import get_notifier  # type: ignore

tg = get_notifier("main")

# ---------------------------------------------------------------------------
# Paths / config
# ---------------------------------------------------------------------------

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)

WS_PRIVATE_URL = os.getenv("BYBIT_WS_PRIVATE_URL", "wss://stream.bybit.com/v5/private")

CATEGORY = "linear"  # in case we later filter by category


class AccountConfig:
    def __init__(self, label: str, key_env: str, sec_env: str) -> None:
        self.label = label
        self.key_env = key_env
        self.sec_env = sec_env

    @property
    def api_key(self) -> str:
        return os.getenv(self.key_env, "")

    @property
    def api_secret(self) -> str:
        return os.getenv(self.sec_env, "")

    def is_enabled(self) -> bool:
        return bool(self.api_key and self.api_secret)

    @property
    def state_path(self) -> Path:
        return STATE_DIR / f"ws_{self.label}.jsonl"


# MAIN + up to 10 subs, matching your TG naming style
ACCOUNT_CONFIGS: List[AccountConfig] = [
    AccountConfig("MAIN", "BYBIT_MAIN_TRADE_KEY", "BYBIT_MAIN_TRADE_SECRET"),
]

for i in range(1, 11):
    ACCOUNT_CONFIGS.append(
        AccountConfig(f"SUB_{i}", f"BYBIT_SUB_{i}_TRADE_KEY", f"BYBIT_SUB_{i}_TRADE_SECRET")
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ws_auth_payload(api_key: str, api_secret: str) -> Dict[str, Any]:
    """
    Build auth message for private WS:
      op: "auth"
      args: [api_key, expires, signature]

    Signature = HMAC_SHA256(secret, f"{api_key}{expires}") in hex.
    """
    import hmac
    import hashlib

    expires = int(time.time() * 1000) + 5000  # ms in future
    msg = f"{api_key}{expires}"
    sig = hmac.new(
        api_secret.encode("utf-8"),
        msg.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return {
        "op": "auth",
        "args": [api_key, str(expires), sig],
    }


def _write_jsonl(path: Path, row: Dict[str, Any]) -> None:
    """
    Append a single JSONL row. Never throws in normal flow.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab") as f:
            f.write(orjson.dumps(row) + b"\n")
    except Exception as _e:  # pragma: no cover
        # Last resort: print; switchboard must not die on logging errors.
        print(f"[WS-SB] failed to write {path}: {_e}")


# ---------------------------------------------------------------------------
# Per-account worker
# ---------------------------------------------------------------------------

def _account_worker(acct: AccountConfig) -> None:
    """
    One thread per account:
      - Connects to private WS
      - Authenticates
      - Subscribes to position + execution
      - Streams messages into state/ws_<label>.jsonl with `account_label` added
    """
    label = acct.label
    key = acct.api_key
    secret = acct.api_secret
    state_path = acct.state_path

    if not key or not secret:
        return

    # Mild spam protection on startup
    try:
        tg.info(f"🔌 WS switchboard starting for account {label}")
    except Exception:
        print(f"[WS-SB] starting for {label}")

    while True:
        ws = None
        try:
            ws = websocket.create_connection(WS_PRIVATE_URL, timeout=5)

            # 1) auth
            auth_msg = _ws_auth_payload(key, secret)
            ws.send(json.dumps(auth_msg))
            raw = ws.recv()
            resp = json.loads(raw)
            if resp.get("retCode") != 0:
                raise RuntimeError(f"{label}: WS auth failed: {resp}")

            # 2) subscribe to private topics
            sub = {"op": "subscribe", "args": ["position", "execution"]}
            ws.send(json.dumps(sub))

            last_ping = time.time()

            while True:
                # keepalive ping
                now = time.time()
                if now - last_ping > 15:
                    ws.send(json.dumps({"op": "ping"}))
                    last_ping = now

                raw = ws.recv()
                if not raw:
                    raise RuntimeError(f"{label}: WS closed")

                msg = json.loads(raw)

                # ignore heartbeats
                if msg.get("op") in ("pong", "ping"):
                    continue

                # tag and persist
                enriched = {
                    "account_label": label,
                    "ts_ms": int(time.time() * 1000),
                    "raw": msg,
                }
                _write_jsonl(state_path, enriched)

        except Exception as e:
            try:
                tg.warn(f"[WS-SB] {label} reconnect after error: {e}")
            except Exception:
                print(f"[WS-SB] {label} reconnect after error: {e}")
            time.sleep(3)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def loop() -> None:
    """
    Entry point called by supervisor.

    - Scans env for configured accounts.
    - Spawns a thread per enabled account (MAIN + SUB_1..SUB_10).
    - Threads reconnect on error; this function just sleeps forever.
    """
    enabled = [a for a in ACCOUNT_CONFIGS if a.is_enabled()]

    if not enabled:
        try:
            tg.warn("⚠️ WS switchboard: no accounts enabled (no WS keys in env).")
        except Exception:
            print("[WS-SB] no accounts enabled.")
        return

    labels = ", ".join([a.label for a in enabled])
    try:
        tg.info(f"🌐 WS switchboard online for accounts: {labels}")
    except Exception:
        print(f"[WS-SB] online for accounts: {labels}")

    threads: List[threading.Thread] = []
    for acct in enabled:
        t = threading.Thread(
            target=_account_worker,
            args=(acct,),
            name=f"ws-sb-{acct.label.lower()}",
            daemon=True,
        )
        t.start()
        threads.append(t)

    # Keep main thread alive; child threads are daemons and will exit with process
    while True:
        time.sleep(60)


if __name__ == "__main__":
    loop()
