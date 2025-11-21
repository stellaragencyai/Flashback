#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — WS Switchboard (multi-account, Bybit v5 private)

Purpose
-------
- Maintain ONE private WebSocket connection PER ACCOUNT (main + flashback01..10).
- Subscribe each connection to core private topics:
    • execution   (fills, partials, etc.)
    • position    (position snapshots/updates)
- Fan-out events via simple async callbacks:
    • execution handlers: async (label: str, row: dict) -> None
    • position handlers:  async (label: str, row: dict) -> None

Design
------
- WsSwitchboard:
    - Holds config for accounts (label + api_key + api_secret).
    - Creates a SubWsClient per account.
    - Provides .add_execution_handler() / .add_position_handler().
    - Runs all clients concurrently via .run_forever().

- SubWsClient:
    - One private WS connection for a single account.
    - Handles auth, subscribe, ping/pong, reconnect loop.
    - On data messages, calls provided callbacks.

Note
----
This module is intentionally generic. Bots like trade_journal, tp_sl_manager, etc.,
will later register handlers instead of talking to Bybit WS directly.
"""

from __future__ import annotations

import asyncio
import json
import hmac  # kept, though auth now uses shared helper
import hashlib  # kept, though auth now uses shared helper
import os
import time
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Dict, List, Optional

import websockets

from app.core.logger import get_logger
from app.core.flashback_common import (
    BYBIT_WS_PRIVATE_URL,      # shared WS private URL
    build_ws_auth_payload,     # shared v5 WS auth builder
)

log = get_logger("ws_switchboard")

# Default Bybit v5 private WS URL:
# - Prefer BYBIT_WS_PRIVATE if set (backward compat)
# - Otherwise use shared BYBIT_WS_PRIVATE_URL from flashback_common
BYBIT_WS_PRIVATE = os.getenv("BYBIT_WS_PRIVATE", BYBIT_WS_PRIVATE_URL)

ExecutionHandler = Callable[[str, Dict[str, Any]], Awaitable[None]]
PositionHandler = Callable[[str, Dict[str, Any]], Awaitable[None]]


@dataclass
class SubWsClient:
    """
    One Bybit private WS client for a single account (label).

    label: "main", "flashback01", ...
    api_key / api_secret: Bybit API creds for that account.
    url: WS endpoint (v5 private).
    on_execution: async callback(label, row)
    on_position:  async callback(label, row)
    """

    label: str
    api_key: str
    api_secret: str
    url: str = BYBIT_WS_PRIVATE
    on_execution: Optional[ExecutionHandler] = None
    on_position: Optional[PositionHandler] = None

    _reconnect_delay: float = field(default=3.0, init=False)

    async def run_forever(self) -> None:
        """
        Outer loop: keep reconnecting forever with backoff.
        """
        log.info("WS[%s] run_forever started", self.label)
        while True:
            try:
                await self._connect_and_run_once()
            except asyncio.CancelledError:
                log.info("WS[%s] cancelled", self.label)
                raise
            except Exception as e:
                log.exception("WS[%s] error in run_forever: %r", self.label, e)

            # Backoff on failure
            log.warning("WS[%s] disconnected; reconnecting in %.1fs", self.label, self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)
            # Simple capped exponential backoff
            self._reconnect_delay = min(self._reconnect_delay * 1.5, 60.0)

    async def _connect_and_run_once(self) -> None:
        """
        Single connection lifecycle: connect, auth, subscribe, read messages.

        Raises on fatal errors; run_forever() will reconnect.
        """
        self._reconnect_delay = 3.0  # reset on successful connect
        log.info("WS[%s] connecting to %s", self.label, self.url)

        async with websockets.connect(self.url, ping_interval=None) as ws:
            # AUTH (now using shared Bybit v5 helper)
            await self._auth(ws)

            # SUBSCRIBE to core private topics
            await self._subscribe(ws, topics=["execution", "position"])

            # Main read loop
            while True:
                raw = await ws.recv()
                # Server may send bytes
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")

                try:
                    msg = json.loads(raw)
                except Exception:
                    log.warning("WS[%s] invalid JSON: %r", self.label, raw[:200])
                    continue

                await self._handle_message(ws, msg)

    async def _auth(self, ws: websockets.WebSocketClientProtocol) -> None:
        """
        Send v5 auth message using shared flashback_common helper.

        The helper builds:
          {
            "op": "auth",
            "args": [api_key, expires_ms, signature]
          }

        where:
          signature = HMAC_SHA256(secret, f"GET/realtime{expires_ms}")
        """
        # Build auth payload with the shared, tested helper
        auth_msg = build_ws_auth_payload(self.api_key, self.api_secret)
        await ws.send(json.dumps(auth_msg))
        log.info("WS[%s] auth sent", self.label)

        # Wait for auth response
        raw = await ws.recv()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            resp = json.loads(raw)
        except Exception:
            raise RuntimeError(f"WS[{self.label}] invalid auth response JSON: {raw!r}")

        # Bybit usually responds with either:
        #   {"op":"auth","success":true,...}
        # or:
        #   {"op":"auth","retCode":0,...}
        if resp.get("success") is not True and resp.get("retCode") not in (0, None):
            raise RuntimeError(f"WS[{self.label}] auth failed: {resp!r}")

        log.info("WS[%s] auth success", self.label)

    async def _subscribe(self, ws: websockets.WebSocketClientProtocol, topics: List[str]) -> None:
        args = list(dict.fromkeys(topics))  # dedupe
        sub_msg = {
            "op": "subscribe",
            "args": args,
        }
        await ws.send(json.dumps(sub_msg))
        log.info("WS[%s] subscribe sent: %s", self.label, args)

    async def _handle_message(self, ws: websockets.WebSocketClientProtocol, msg: Dict[str, Any]) -> None:
        """
        Handle messages:
          - ping / pong
          - subscription / auth acks
          - data: topic="execution"/"position"
        """
        # Bybit can send ping-like things in a few shapes; be defensive.
        if msg.get("op") == "ping" or msg.get("event") == "ping":
            await ws.send(json.dumps({"op": "pong"}))
            return

        topic = msg.get("topic")
        if not topic:
            # Probably an ack or unknown; log debug once in a while
            if "retCode" in msg or "success" in msg:
                log.info("WS[%s] control msg: %s", self.label, msg)
            return

        data = msg.get("data") or []
        if not isinstance(data, list):
            data = [data]

        if topic == "execution":
            if self.on_execution:
                for row in data:
                    try:
                        await self.on_execution(self.label, row)
                    except Exception as e:
                        log.exception("WS[%s] execution handler error: %r", self.label, e)
        elif topic == "position":
            if self.on_position:
                for row in data:
                    try:
                        await self.on_position(self.label, row)
                    except Exception as e:
                        log.exception("WS[%s] position handler error: %r", self.label, e)
        else:
            # Future extension: other private topics
            log.debug("WS[%s] unhandled topic %s: %s", self.label, topic, msg)


class WsSwitchboard:
    """
    Multi-account WS orchestrator.

    - Builds one SubWsClient per configured account.
    - Lets you register async handlers for executions + positions.
    - Runs all clients concurrently.
    """

    def __init__(self) -> None:
        self._clients: Dict[str, SubWsClient] = {}
        self._exec_handlers: List[ExecutionHandler] = []
        self._pos_handlers: List[PositionHandler] = []

    # ---------- Public registration API ----------

    def add_execution_handler(self, handler: ExecutionHandler) -> None:
        """
        Register an async handler for execution rows.
        Signature: async handler(label: str, row: dict) -> None
        """
        self._exec_handlers.append(handler)

    def add_position_handler(self, handler: PositionHandler) -> None:
        """
        Register an async handler for position rows.
        Signature: async handler(label: str, row: dict) -> None
        """
        self._pos_handlers.append(handler)

    # ---------- Client management ----------

    def add_account(self, label: str, api_key: str, api_secret: str) -> None:
        """
        Add a new account to the switchboard.

        label: "main" or "flashback01", etc.
        """
        label = label.strip()
        if not api_key or not api_secret:
            log.warning("WS[%s] skipping account: missing api_key/api_secret", label)
            return

        if label in self._clients:
            log.warning("WS[%s] already configured; ignoring duplicate add_account", label)
            return

        client = SubWsClient(
            label=label,
            api_key=api_key,
            api_secret=api_secret,
            on_execution=self._dispatch_execution,
            on_position=self._dispatch_position,
        )
        self._clients[label] = client
        log.info("WS[%s] account added to switchboard", label)

    async def _dispatch_execution(self, label: str, row: Dict[str, Any]) -> None:
        """
        Fan-out a single execution row to all registered handlers.
        """
        for h in self._exec_handlers:
            try:
                await h(label, row)
            except Exception as e:
                log.exception("WS dispatch execution handler error (%s): %r", label, e)

    async def _dispatch_position(self, label: str, row: Dict[str, Any]) -> None:
        """
        Fan-out a single position row to all registered handlers.
        """
        for h in self._pos_handlers:
            try:
                await h(label, row)
            except Exception as e:
                log.exception("WS dispatch position handler error (%s): %r", label, e)

    # ---------- Run loop ----------

    async def run_forever(self) -> None:
        """
        Start all configured clients and keep them running forever.

        This should be called from a top-level `asyncio.run(...)`.
        """
        if not self._clients:
            log.warning("WsSwitchboard started with NO accounts configured.")
            # Just idle forever so the bot doesn't crash
            while True:
                await asyncio.sleep(10.0)

        # Create a task for each client's run_forever
        tasks = []
        for label, client in self._clients.items():
            log.info("WS[%s] launching run_forever task", label)
            t = asyncio.create_task(client.run_forever(), name=f"ws-{label}")
            tasks.append(t)

        # Wait for any of the tasks to fail (shouldn't, they reconnect internally)
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("WsSwitchboard cancelled; shutting down.")
            raise
        except Exception as e:
            log.exception("WsSwitchboard fatal error: %r", e)
            # Let caller decide whether to restart the whole bot
