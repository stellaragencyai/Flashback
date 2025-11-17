#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Switchboard — Central WebSocket Hub

What this does *right now*:
- Runs a WebSocket server on HOST:PORT (env-controlled, default 127.0.0.1:5000).
- Accepts bot connections at: ws://HOST:PORT/ws/bot
- Lets bots:
    • send "hello" to identify
    • "subscribe" to topics (e.g., market:PUMPFUNUSDT:1m)
    • send "command" messages (place_order, cancel_order, etc.) — currently stubbed
- Routes:
    • events to subscribed bots via a publish_event(topic, data) helper.

What this does *NOT* yet:
- Connect to Bybit WS.
- Actually place orders on Bybit.
- Enforce risk.

Those will be built on top of this skeleton so your whole bot swarm shares
the same fast WS hub.

Env:
- SWITCHBOARD_HOST   (default "127.0.0.1")
- SWITCHBOARD_PORT   (default "5000")
- SWITCHBOARD_LOG    ("debug" / "info" / "warn")

Typical usage:
    python -m app.switchboard.server
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from dataclasses import dataclass, field
from typing import Dict, Set, Any, Optional, List

import websockets
from websockets.server import WebSocketServerProtocol


# ---------------- Logging ---------------- #

LOG_LEVEL = os.getenv("SWITCHBOARD_LOG", "info").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)
log = logging.getLogger("switchboard")


# ---------------- Models ---------------- #

@dataclass
class BotClient:
    ws: WebSocketServerProtocol
    bot_id: str = "unknown"
    role: str = "bot"
    subscriptions: Set[str] = field(default_factory=set)


class Switchboard:
    """
    In-memory hub that:
    - tracks connected bots
    - tracks topic subscriptions
    - provides publish_event() to fan out messages
    """

    def __init__(self) -> None:
        # Active clients: ws -> BotClient
        self.clients: Dict[WebSocketServerProtocol, BotClient] = {}
        # Topic subscriptions: topic -> set(ws)
        self.topic_subs: Dict[str, Set[WebSocketServerProtocol]] = {}
        # For clean shutdown
        self._shutdown = asyncio.Event()

    # ---- Client lifecycle ---- #

    async def register(self, ws: WebSocketServerProtocol) -> None:
        client = BotClient(ws=ws)
        self.clients[ws] = client
        log.info(f"[Switchboard] Bot connected from {ws.remote_address}")

    async def unregister(self, ws: WebSocketServerProtocol) -> None:
        client = self.clients.pop(ws, None)
        if not client:
            return

        # Remove from any topic sets
        for topic in list(client.subscriptions):
            subs = self.topic_subs.get(topic)
            if subs and ws in subs:
                subs.remove(ws)
                if not subs:
                    self.topic_subs.pop(topic, None)

        log.info(
            f"[Switchboard] Bot disconnected: bot_id={client.bot_id}, "
            f"role={client.role}, addr={ws.remote_address}"
        )

    # ---- Subscriptions ---- #

    def _subscribe_client(self, ws: WebSocketServerProtocol, topics: List[str]) -> None:
        client = self.clients.get(ws)
        if not client:
            return

        for topic in topics:
            topic = topic.strip()
            if not topic:
                continue
            client.subscriptions.add(topic)
            subs = self.topic_subs.setdefault(topic, set())
            subs.add(ws)

        log.debug(
            f"[Switchboard] bot_id={client.bot_id} subscribed to: {topics}"
        )

    # ---- Event publishing ---- #

    async def publish_event(self, topic: str, data: Any) -> None:
        """
        Fan-out helper: sends an 'event' message to all bots subscribed to `topic`.

        event shape:
        {
          "type": "event",
          "topic": "<topic>",
          "data": <payload>
        }
        """
        subs = self.topic_subs.get(topic)
        if not subs:
            return

        msg = json.dumps(
            {
                "type": "event",
                "topic": topic,
                "data": data,
            },
            separators=(",", ":"),
        )

        dead: List[WebSocketServerProtocol] = []

        for ws in subs:
            if ws.closed:
                dead.append(ws)
                continue
            try:
                await ws.send(msg)
            except Exception as e:
                log.warning(f"[Switchboard] send event failed to {ws.remote_address}: {e}")
                dead.append(ws)

        # Cleanup dead sockets
        for ws in dead:
            await self.unregister(ws)

    # ---- Incoming message handling ---- #

    async def _send_error(
        self, ws: WebSocketServerProtocol, message: str, correlation_id: Optional[str] = None
    ) -> None:
        payload = {
            "type": "error",
            "message": message,
        }
        if correlation_id:
            payload["correlation_id"] = correlation_id

        try:
            await ws.send(json.dumps(payload, separators=(",", ":")))
        except Exception as e:
            log.warning(f"[Switchboard] failed to send error to client: {e}")

    async def _handle_hello(self, ws: WebSocketServerProtocol, payload: Dict[str, Any]) -> None:
        client = self.clients.get(ws)
        if not client:
            return
        client.bot_id = str(payload.get("bot_id", client.bot_id))
        client.role = str(payload.get("role", client.role))
        # Optional: token auth in future
        log.info(
            f"[Switchboard] HELLO from bot_id={client.bot_id}, "
            f"role={client.role}, addr={ws.remote_address}"
        )

    async def _handle_subscribe(self, ws: WebSocketServerProtocol, payload: Dict[str, Any]) -> None:
        topics = payload.get("topics")
        if not isinstance(topics, list):
            await self._send_error(ws, "subscribe.topics must be a list")
            return
        topics = [str(t) for t in topics]
        self._subscribe_client(ws, topics)

    async def _handle_command(self, ws: WebSocketServerProtocol, payload: Dict[str, Any]) -> None:
        """
        Handle commands from bots.

        shape:
        {
          "type": "command",
          "command": "place_order",
          "correlation_id": "uuid-123",
          "sub_id": "sub_260417078",
          "payload": { ... }
        }

        For now, this is a stub that just logs and replies with 'ok'
        so you can already wire strategy bots to it.
        We'll later connect this to actual Bybit REST + risk checks.
        """
        cmd = str(payload.get("command", "")).strip()
        correlation_id = payload.get("correlation_id")
        sub_id = payload.get("sub_id")
        body = payload.get("payload", {})

        client = self.clients.get(ws)
        who = client.bot_id if client else "unknown"

        if not cmd:
            await self._send_error(ws, "command missing", correlation_id)
            return

        log.info(
            f"[Switchboard] command from bot={who}: "
            f"cmd={cmd}, sub_id={sub_id}, payload={body}"
        )

        # TODO: route to real handlers: place_order, cancel_order, set_tp_sl, etc.
        # For now we just echo a stubbed "ok".
        resp = {
            "type": "command_result",
            "command": cmd,
            "correlation_id": correlation_id,
            "status": "ok",
            "detail": {
                "info": "stubbed in switchboard; no real order yet"
            },
        }
        try:
            await ws.send(json.dumps(resp, separators=(",", ":")))
        except Exception as e:
            log.warning(f"[Switchboard] failed to send command_result: {e}")

    async def handle_client(self, ws: WebSocketServerProtocol) -> None:
        """
        Main per-connection loop.
        """
        await self.register(ws)
        try:
            async for msg in ws:
                try:
                    data = json.loads(msg)
                except Exception:
                    await self._send_error(ws, "invalid JSON")
                    continue

                if not isinstance(data, dict):
                    await self._send_error(ws, "message must be a JSON object")
                    continue

                mtype = data.get("type")
                if mtype == "hello":
                    await self._handle_hello(ws, data)
                elif mtype == "subscribe":
                    await self._handle_subscribe(ws, data)
                elif mtype == "command":
                    await self._handle_command(ws, data)
                else:
                    await self._send_error(ws, f"unknown type '{mtype}'")
        except websockets.ConnectionClosed:
            # normal disconnect
            pass
        except Exception as e:
            log.warning(f"[Switchboard] client loop error: {e}")
        finally:
            await self.unregister(ws)

    # ---- Server lifecycle ---- #

    async def run_server(self, host: str, port: int) -> None:
        """
        Start the WebSocket server at /ws/bot.
        """

        async def handler(ws: WebSocketServerProtocol) -> None:
            """
            New-style websockets handler: only `ws` is passed.
            Path is read from `ws.path` (for websockets >= 10.x).
            """
            path = getattr(ws, "path", "/")
            if path != "/ws/bot":
                await ws.close(code=1008, reason="invalid path")
                return
            await self.handle_client(ws)

        log.info(f"[Switchboard] starting server on ws://{host}:{port}/ws/bot")

        async with websockets.serve(handler, host, port, max_size=2**20):
            # Wait until shutdown event triggered (via signal handler)
            await self._shutdown.wait()
            log.info("[Switchboard] shutdown requested, closing server...")

    def request_shutdown(self) -> None:
        self._shutdown.set()


# ---------------- Entrypoint ---------------- #

async def _main_async() -> None:
    host = os.getenv("SWITCHBOARD_HOST", "127.0.0.1")
    port_str = os.getenv("SWITCHBOARD_PORT", "5000")
    try:
        port = int(port_str)
    except ValueError:
        raise SystemExit(f"Invalid SWITCHBOARD_PORT={port_str!r}")

    switchboard = Switchboard()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, switchboard.request_shutdown)
        except NotImplementedError:
            # Windows doesn't support signal handlers in Proactor loop, no big deal.
            pass

    await switchboard.run_server(host, port)


def main() -> None:
    try:
        asyncio.run(_main_async())
    except KeyboardInterrupt:
        log.info("[Switchboard] keyboard interrupt, exiting.")


if __name__ == "__main__":
    main()
