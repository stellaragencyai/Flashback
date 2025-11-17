#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Switchboard — Shared WebSocket Client Base

Purpose
-------
Reusable client for all your bots:
- Connects to ws://HOST:PORT/ws/bot
- Sends HELLO with bot_id, role, and optional topics
- Sends periodic heartbeats
- Reconnects on failures with a backoff
- Dispatches messages to overridable hooks:

    - on_ack(message)
    - on_event(topic, data, raw)
    - on_command_result(message)
    - on_error(message)
    - on_unknown(message)

Each real bot will:
- import SwitchboardClient
- subclass it OR pass callbacks
- run .start() in its own main().

Env (optional):
- SWITCHBOARD_URL   (default "ws://127.0.0.1:5000/ws/bot")
- SWITCHBOARD_LOG   ("debug" / "info" / "warn")
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional, Callable

import websockets
from websockets import WebSocketClientProtocol

# ------------- Logging ------------- #

LOG_LEVEL = os.getenv("SWITCHBOARD_LOG", "info").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] [%(levelname)s] [client] %(message)s",
)
log = logging.getLogger("switchboard.client")


class SwitchboardClient:
    """
    Generic WebSocket client for the Switchboard.

    Usage:
        client = SwitchboardClient(
            bot_id="tp-manager-main",
            role="tp_manager",
            topics=["events:positions", "events:orders"],
        )
        asyncio.run(client.start())
    """

    def __init__(
        self,
        bot_id: str,
        role: str = "bot",
        topics: Optional[List[str]] = None,
        url: Optional[str] = None,
        heartbeat_interval: float = 5.0,
        reconnect_delay: float = 3.0,
    ) -> None:
        self.bot_id = bot_id
        self.role = role
        self.topics = topics or []
        self.url = url or os.getenv("SWITCHBOARD_URL", "ws://127.0.0.1:5000/ws/bot")
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_delay = reconnect_delay

        self._ws: Optional[WebSocketClientProtocol] = None
        self._stop = asyncio.Event()

        # Optional external callbacks if you don't want to subclass
        self.on_event_cb: Optional[
            Callable[[str, Any, Dict[str, Any]], None]
        ] = None

    # ---------- Public API ---------- #

    async def start(self) -> None:
        """
        Main loop:
        - connect
        - run heartbeat + recv loop
        - reconnect on errors until stop() is called
        """
        log.info(
            f"Starting SwitchboardClient bot_id={self.bot_id}, "
            f"role={self.role}, url={self.url}"
        )

        while not self._stop.is_set():
            try:
                async with websockets.connect(self.url) as ws:
                    self._ws = ws
                    log.info("Connected to switchboard")

                    # HELLO
                    await self._send_hello()

                    # SUBSCRIBE
                    if self.topics:
                        await self._send_subscribe(self.topics)

                    # Run recv + heartbeat in parallel
                    await asyncio.gather(
                        self._heartbeat_loop(),
                        self._recv_loop(),
                    )

            except asyncio.CancelledError:
                # Stop requested
                break
            except Exception as e:
                log.warning(f"Connection error: {e!r}, reconnecting in {self.reconnect_delay}s")
                await asyncio.sleep(self.reconnect_delay)

        log.info("Client stopped cleanly")

    def stop(self) -> None:
        self._stop.set()

    async def send_command(
        self,
        command: str,
        sub_id: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        correlation_id: Optional[str] = None,
    ) -> None:
        """
        Send a 'command' message through the switchboard.

        Example:
            await client.send_command(
                "place_order",
                sub_id="sub_260417078",
                payload={"symbol": "BTCUSDT", "side": "Buy", ...},
                correlation_id="uuid-123",
            )
        """
        if not self._ws or self._ws.closed:
            log.warning("send_command called but WebSocket is not connected")
            return

        msg = {
            "type": "command",
            "command": command,
            "sub_id": sub_id,
            "payload": payload or {},
        }
        if correlation_id:
            msg["correlation_id"] = correlation_id

        await self._ws.send(json.dumps(msg, separators=(",", ":")))
        log.debug(f"sent command: {msg}")

    # ---------- Internal helpers ---------- #

    async def _send_hello(self) -> None:
        if not self._ws or self._ws.closed:
            return
        payload = {
            "type": "hello",
            "bot_id": self.bot_id,
            "role": self.role,
            "ts": int(time.time() * 1000),
        }
        await self._ws.send(json.dumps(payload, separators=(",", ":")))
        log.info(f"HELLO sent: bot_id={self.bot_id}, role={self.role}")

    async def _send_subscribe(self, topics: List[str]) -> None:
        if not self._ws or self._ws.closed:
            return
        payload = {
            "type": "subscribe",
            "topics": topics,
        }
        await self._ws.send(json.dumps(payload, separators=(",", ":")))
        log.info(f"SUBSCRIBE sent: topics={topics}")

    async def _heartbeat_loop(self) -> None:
        while not self._stop.is_set() and self._ws and not self._ws.closed:
            try:
                msg = {
                    "type": "heartbeat",
                    "bot_id": self.bot_id,
                    "ts": int(time.time() * 1000),
                }
                await self._ws.send(json.dumps(msg, separators=(",", ":")))
                log.debug("heartbeat sent")
            except Exception as e:
                log.warning(f"heartbeat failed: {e!r}")
                return  # let outer loop reconnect
            await asyncio.sleep(self.heartbeat_interval)

    async def _recv_loop(self) -> None:
        if not self._ws:
            return
        try:
            async for raw in self._ws:
                try:
                    msg = json.loads(raw)
                except Exception:
                    log.warning(f"received non-JSON message: {raw!r}")
                    continue
                await self._dispatch_message(msg)
        except websockets.ConnectionClosed:
            log.info("WebSocket closed, leaving recv loop")
        except Exception as e:
            log.warning(f"recv_loop error: {e!r}")

    async def _dispatch_message(self, msg: Dict[str, Any]) -> None:
        mtype = msg.get("type")
        if mtype == "ack":
            await self.on_ack(msg)
        elif mtype == "event":
            topic = msg.get("topic", "")
            data = msg.get("data")
            await self.on_event(topic, data, msg)
        elif mtype == "command_result":
            await self.on_command_result(msg)
        elif mtype == "error":
            await self.on_error(msg)
        else:
            await self.on_unknown(msg)

    # ---------- Default handlers (for subclassing/override) ---------- #

    async def on_ack(self, message: Dict[str, Any]) -> None:
        log.info(f"ACK: {message}")

    async def on_event(self, topic: str, data: Any, raw: Dict[str, Any]) -> None:
        if self.on_event_cb:
            try:
                self.on_event_cb(topic, data, raw)
            except Exception as e:
                log.warning(f"on_event callback error: {e!r}")
        else:
            log.info(f"EVENT topic={topic}, data={data}")

    async def on_command_result(self, message: Dict[str, Any]) -> None:
        log.info(f"COMMAND RESULT: {message}")

    async def on_error(self, message: Dict[str, Any]) -> None:
        log.warning(f"ERROR from switchboard: {message}")

    async def on_unknown(self, message: Dict[str, Any]) -> None:
        log.info(f"UNKNOWN message: {message}")


# ------------- Demo entrypoint (optional) ------------- #

async def _demo() -> None:
    """
    Simple demo client:
    - connects
    - sends HELLO
    - sends SUBSCRIBE to ["test"]
    - logs heartbeats + any events
    """

    client = SwitchboardClient(
        bot_id="demo-client",
        role="demo",
        topics=["test"],
    )

    # Example of attaching a callback instead of subclassing
    def handle_event(topic: str, data: Any, raw: Dict[str, Any]) -> None:
        log.info(f"[demo] got EVENT topic={topic}, data={data}")

    client.on_event_cb = handle_event

    await client.start()


if __name__ == "__main__":
    try:
        asyncio.run(_demo())
    except KeyboardInterrupt:
        log.info("Demo interrupted by user")
