#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Sanity Tester (public + private)

Purpose
-------
Quick sanity check that:
- Public Bybit WS works.
- Private Bybit WS auth + subscribe works using a *raw* v5 auth payload,
  independent of any helpers in flashback_common.

Usage
-----
    python -m app.tools.ws_sanity_tester
"""

from __future__ import annotations

import asyncio
import json
import time
import os
import hmac
import hashlib

import websockets

from app.core.logger import get_logger
from app.core.flashback_common import BYBIT_WS_PRIVATE_URL

log = get_logger("ws_sanity_tester")

PUBLIC_URL = "wss://stream.bybit.com/v5/public/linear"
PRIVATE_URL = BYBIT_WS_PRIVATE_URL  # usually wss://stream.bybit.com/v5/private

# Explicitly pull main API key/secret from .env here
MAIN_KEY = os.getenv("BYBIT_MAIN_API_KEY", "")
MAIN_SECRET = os.getenv("BYBIT_MAIN_API_SECRET", "")


def _build_ws_auth_payload_raw(api_key: str, api_secret: str) -> dict:
    """
    Minimal, hardcoded-auth builder for Bybit v5 WS private auth.

    This bypasses all helpers so we can see if the issue is our helper
    or the API key / permissions on Bybit's side.

    Bybit v5 pattern:
      op: "auth"
      args: [api_key, expiresMs, signature]

    where:
      signature = HMAC_SHA256(secret, "GET/realtime" + str(expiresMs))
    """
    if not api_key or not api_secret:
        raise RuntimeError("Missing BYBIT_MAIN_API_KEY / BYBIT_MAIN_API_SECRET in .env")

    # Expires ~60 seconds in the future, in *milliseconds*
    expires_ms = int(time.time() * 1000) + 60_000

    # Message must be exactly "GET/realtime" + expires_ms
    msg = f"GET/realtime{expires_ms}".encode("utf-8")
    sig = hmac.new(
        api_secret.encode("utf-8"),
        msg,
        hashlib.sha256,
    ).hexdigest()

    return {
        "op": "auth",
        "args": [api_key, expires_ms, sig],
    }


async def test_public():
    print("=== Step 1: Public WS sanity ===")
    print(f"[PUBLIC] Connecting to '{PUBLIC_URL}'...")
    async with websockets.connect(PUBLIC_URL, ping_interval=None) as ws:
        sub = {"op": "subscribe", "args": ["publicTrade.BTCUSDT"]}
        await ws.send(json.dumps(sub))
        print("[PUBLIC] Sent subscribe: publicTrade.BTCUSDT")

        count = 0
        start = time.time()

        while count < 5 and (time.time() - start) < 10:
            raw = await ws.recv()
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            if msg.get("op") in ("pong", "ping"):
                continue

            topic = msg.get("topic")
            if topic and topic.startswith("publicTrade."):
                count += 1
                print(f"\n[PUBLIC] #{count} trade message:")
                print(json.dumps(msg, indent=2)[:400], "...")
        if count >= 5:
            print("\n[PUBLIC] ✅ Received 5 publicTrade messages. Public WS looks OK.\n")
            return True
        else:
            print("\n[PUBLIC] ❌ Did not receive enough publicTrade messages.\n")
            return False


async def test_private():
    print("=== Step 2: Private WS sanity ===")
    print(f"[PRIVATE] Connecting to '{PRIVATE_URL}' using RAW v5 auth payload...")

    async with websockets.connect(PRIVATE_URL, ping_interval=None) as ws:
        # 1) AUTH using raw v5 logic (no helpers)
        try:
            auth_msg = _build_ws_auth_payload_raw(MAIN_KEY, MAIN_SECRET)
        except Exception as e:
            print(f"[PRIVATE] ❌ Failed to build auth payload: {e}")
            return False

        await ws.send(json.dumps(auth_msg))
        print("[PRIVATE] Sent auth message.")

        raw = await ws.recv()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            resp = json.loads(raw)
        except Exception:
            print(f"[PRIVATE] ❌ Invalid auth response JSON: {raw!r}")
            return False

        print(f"[PRIVATE] Auth response: {resp}")

        success = resp.get("success")
        ret_code = resp.get("retCode")
        if success is False or (ret_code not in (0, None)):
            err = resp.get("ret_msg") or resp.get("retMsg") or str(resp)
            print(f"[PRIVATE] ❌ Auth FAILED: {err}")
            return False

        print("[PRIVATE] ✅ Auth success.")

        # 2) SUBSCRIBE to execution + position
        sub = {"op": "subscribe", "args": ["execution", "position"]}
        await ws.send(json.dumps(sub))
        print("[PRIVATE] Sent subscribe: execution, position")

        got_data = False
        start = time.time()

        while (time.time() - start) < 30:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=10)
            except asyncio.TimeoutError:
                print("[PRIVATE] Still waiting for private data...")
                continue

            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="replace")
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            if msg.get("op") in ("pong", "ping"):
                continue

            if "retCode" in msg or "success" in msg:
                print(f"[PRIVATE] control/other: {msg}")
                continue

            topic = msg.get("topic")
            if topic in ("execution", "position"):
                print(f"\n[PRIVATE] ✅ First private data message on topic={topic}:")
                print(json.dumps(msg, indent=2)[:400], "...")
                got_data = True
                break

        if not got_data:
            print("\n[PRIVATE] ❌ No private data messages received within 30s.\n")
            return False

        return True


async def main_async():
    ok_pub = await test_public()
    ok_priv = await test_private()
    print("\n=== SUMMARY ===")
    print(f"Public WS:  {'OK' if ok_pub else 'FAILED'}")
    print(f"Private WS: {'OK' if ok_priv else 'FAILED'}")
    if ok_pub and ok_priv:
        print("\n[WS TEST] ✅ Both public and private WS look healthy.")
    else:
        print("\n[WS TEST] ❌ One or both tests failed. Fix this before wiring full WS city.")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
