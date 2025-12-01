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
register handlers instead of talking to Bybit WS directly.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import hmac
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

import websockets

from app.core.logger import get_logger
from app.core.flashback_common import BYBIT_WS_PRIVATE_URL  # shared WS private URL

# ---- Telegram health notifier (best-effort) ----
try:
    from app.core.notifier_bot import get_notifier  # type: ignore
except ImportError:  # extremely defensive; shouldn't happen in Flashback
    def get_notifier(name: str):  # type: ignore
        class _Dummy:
            def info(self, *a, **k):  # pragma: no cover - fallback
                pass
            def warn(self, *a, **k):
                pass
            def error(self, *a, **k):
                pass
        return _Dummy()

tg_health = get_notifier("health")

log = get_logger("ws_switchboard")

# Default Bybit v5 private WS URL:
# - Prefer BYBIT_WS_PRIVATE if set (backward compat)
# - Otherwise use shared BYBIT_WS_PRIVATE_URL from flashback_common
BYBIT_WS_PRIVATE = os.getenv("BYBIT_WS_PRIVATE", BYBIT_WS_PRIVATE_URL)

# ---- WS health / stale config ----
WS_STALE_SEC = int(os.getenv("WS_STALE_SEC", "20"))  # no messages for this long => "stale"
WS_STALE_ALERT_COOLDOWN_SEC = int(os.getenv("WS_STALE_ALERT_COOLDOWN_SEC", "300"))  # health ping cooldown

# ---- WS status file (for dashboard / other bots) ----
ROOT_DIR = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT_DIR / "state"
STATE_DIR.mkdir(parents=True, exist_ok=True)
WS_STATUS_PATH = STATE_DIR / "ws_status.json"

_WS_STATUS_CACHE: Dict[str, Dict[str, Any]] = {}


def _init_ws_status() -> None:
    global _WS_STATUS_CACHE
    try:
        if WS_STATUS_PATH.exists():
            _WS_STATUS_CACHE = json.loads(WS_STATUS_PATH.read_text(encoding="utf-8"))
        else:
            _WS_STATUS_CACHE = {}
    except Exception:
        _WS_STATUS_CACHE = {}


def _save_ws_status() -> None:
    try:
        WS_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
        WS_STATUS_PATH.write_text(
            json.dumps(_WS_STATUS_CACHE, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        # Status is "nice to have" only; never crash the switchboard on it.
        pass


def _update_ws_status_record(label: str, **fields: Any) -> None:
    """
    Update status snapshot for a WS label and persist to state/ws_status.json.

    Known fields:
      - status: "INIT" | "AUTH_OK" | "OK" | "STALE" | "AUTH_BLOCKED" | "ERROR"
      - last_msg_ms: int
      - last_auth_ok_ms: int
      - last_error: str
    """
    global _WS_STATUS_CACHE
    now_ms = int(time.time() * 1000)

    rec = _WS_STATUS_CACHE.get(label, {})
    rec.setdefault("label", label)
    for k, v in fields.items():
        if v is None and k in ("last_error",):
            rec.pop(k, None)
        else:
            rec[k] = v
    rec["updated_ms"] = now_ms
    _WS_STATUS_CACHE[label] = rec
    _save_ws_status()


# Initialize status cache on import
_init_ws_status()

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
    on_position: async callback(label, row)
    """

    label: str
    api_key: str
    api_secret: str
    url: str = BYBIT_WS_PRIVATE
    on_execution: Optional[ExecutionHandler] = None
    on_position: Optional[PositionHandler] = None

    _reconnect_delay: float = field(default=3.0, init=False)
    _last_msg_ts: float = field(default_factory=lambda: time.time(), init=False)
    _last_auth_ok_ms: int = field(default=0, init=False)
    _last_error: Optional[str] = field(default=None, init=False)
    _status: str = field(default="INIT", init=False)
    _last_stale_alert_ts: float = field(default=0.0, init=False)

    def __post_init__(self) -> None:
        _update_ws_status_record(
            self.label,
            status="INIT",
            last_error=None,
        )

    # ---- internal status helpers ------------------------------------------

    def _mark_message_seen(self) -> None:
        self._last_msg_ts = time.time()
        _update_ws_status_record(
            self.label,
            last_msg_ms=int(self._last_msg_ts * 1000),
            status=self._status,
        )

    def _set_status(self, status: str, error: Optional[str] = None) -> None:
        self._status = status
        self._last_error = error
        _update_ws_status_record(
            self.label,
            status=status,
            last_error=error,
        )

    # ---- AUTH (exactly per Bybit v5 docs) ---------------------------------

    def _build_ws_auth_payload(self) -> Dict[str, Any]:
        """
        Build v5 WS auth payload.

        Official spec:
          expires = int((time.time() + 1) * 1000)
          sign    = HMAC_SHA256(secret, "GET/realtime" + str(expires))

          payload = {
            "op": "auth",
            "args": [api_key, expires, sign]
          }
        """
        if not self.api_key or not self.api_secret:
            raise RuntimeError(f"WS[{self.label}] missing api_key/api_secret for auth")

        expires_ms = int((time.time() + 1) * 1000)

        msg = f"GET/realtime{expires_ms}".encode("utf-8")
        sig = hmac.new(
            self.api_secret.encode("utf-8"),
            msg,
            hashlib.sha256,
        ).hexdigest()

        return {
            "op": "auth",
            "args": [self.api_key, expires_ms, sig],
        }

    async def run_forever(self) -> None:
        """
        Outer loop: keep reconnecting forever with backoff.

        Rate-limit aware:
        - If we see a "too many requests" / 403 style error, we back off HARD
          (at least 300 seconds) to avoid CloudFront / WS bans.
        """
        log.info("WS[%s] run_forever started", self.label)
        while True:
            try:
                await self._connect_and_run_once()
            except asyncio.CancelledError:
                log.info("WS[%s] cancelled", self.label)
                raise
            except Exception as e:
                msg = str(e)
                log.exception("WS[%s] error in run_forever: %r", self.label, e)

                # Detect Bybit / CloudFront rate-limit style errors
                lower = msg.lower()
                if "too many requests" in lower or "status 403" in lower:
                    # Hard backoff: at least 5 minutes
                    self._reconnect_delay = max(self._reconnect_delay, 300.0)
                    warn_text = (
                        f"⛔ WS[{self.label}] rate-limited (too many requests / 403). "
                        f"Backing off for {int(self._reconnect_delay)}s."
                    )
                    log.warning(warn_text)
                    try:
                        tg_health.warn(warn_text)
                    except Exception:
                        pass
                else:
                    # Normal error: keep exponential backoff
                    self._reconnect_delay = min(self._reconnect_delay * 1.5, 60.0)

                try:
                    tg_health.error(f"❌ WS[{self.label}] crashed in run_forever: {type(e).__name__}: {e}")
                except Exception:
                    pass
                self._set_status("ERROR", msg)

            log.warning("WS[%s] disconnected; reconnecting in %.1fs", self.label, self._reconnect_delay)
            await asyncio.sleep(self._reconnect_delay)


    async def _connect_and_run_once(self) -> None:
        """
        Single connection lifecycle: connect, auth, subscribe, read messages.
        """
        self._reconnect_delay = 3.0
        log.info("WS[%s] connecting to %s", self.label, self.url)

        async with websockets.connect(self.url, ping_interval=None) as ws:
            await self._auth(ws)
            await self._subscribe(ws, topics=["execution", "position"])

            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=WS_STALE_SEC)
                except asyncio.TimeoutError:
                    now = time.time()
                    if now - self._last_stale_alert_ts >= WS_STALE_ALERT_COOLDOWN_SEC:
                        self._last_stale_alert_ts = now
                        msg = (
                            f"⚠️ WS[{self.label}] no messages for {WS_STALE_SEC}s "
                            f"(possible stale feed or quiet market)."
                        )
                        log.warning(msg)
                        try:
                            tg_health.warn(msg)
                        except Exception:
                            pass
                    self._set_status("STALE", f"no messages for {WS_STALE_SEC}s")
                    continue

                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")

                try:
                    msg = json.loads(raw)
                except Exception:
                    log.warning("WS[%s] invalid JSON: %r", self.label, raw[:200])
                    continue

                self._mark_message_seen()
                if self._status not in ("AUTH_BLOCKED",):
                    self._set_status("OK", None)

                await self._handle_message(ws, msg)

    async def _auth(self, ws: websockets.WebSocketClientProtocol) -> None:
        """
        Send v5 auth message and validate response.

        Expected response on success:
          {"op":"auth","success":true,"ret_msg":"","conn_id":"..."}
        """
        auth_msg = self._build_ws_auth_payload()
        await ws.send(json.dumps(auth_msg))
        log.info("WS[%s] auth sent", self.label)

        raw = await ws.recv()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        try:
            resp = json.loads(raw)
        except Exception:
            raise RuntimeError(f"WS[{self.label}] invalid auth response JSON: {raw!r}")

        op = resp.get("op")
        success = resp.get("success")
        ret_code = resp.get("retCode")

        if op != "auth":
            raise RuntimeError(f"WS[{self.label}] unexpected auth response: {resp!r}")

        # If success is explicitly False OR retCode is non-zero, treat as auth failure
        if success is False or (ret_code not in (None, 0)):
            err = resp.get("ret_msg") or resp.get("retMsg") or str(resp)
            self._set_status("ERROR", f"auth failed: {err}")
            try:
                tg_health.error(f"❌ WS[{self.label}] auth failed: {err}")
            except Exception:
                pass
            raise RuntimeError(f"WS[{self.label}] auth failed: {resp!r}")

        now_ms = int(time.time() * 1000)
        self._last_auth_ok_ms = now_ms
        self._set_status("AUTH_OK", None)
        _update_ws_status_record(self.label, last_auth_ok_ms=now_ms)
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
        if msg.get("op") == "ping" or msg.get("event") == "ping":
            self._mark_message_seen()
            await ws.send(json.dumps({"op": "pong"}))
            return

        topic = msg.get("topic")
        if not topic:
            if "retCode" in msg or "success" in msg:
                log.info("WS[%s] control msg: %s", self.label, msg)

                op = msg.get("op")
                success = msg.get("success")
                if op == "subscribe" and success is False:
                    err = msg.get("ret_msg") or msg.get("retMsg") or "unknown error"
                    self._set_status("AUTH_BLOCKED", err)
                    self._reconnect_delay = max(self._reconnect_delay, 60.0)
                    text = (
                        f"⛔ WS[{self.label}] subscribe failed: {err} "
                        f"(likely IP whitelist / permissions / WS scope on key). "
                        f"Connection left alive, status=AUTH_BLOCKED."
                    )
                    log.error(text)
                    try:
                        tg_health.error(text)
                    except Exception:
                        pass
                    return
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
        self._exec_handlers.append(handler)

    def add_position_handler(self, handler: PositionHandler) -> None:
        self._pos_handlers.append(handler)

    # ---------- Client management ----------

    def add_account(self, label: str, api_key: str, api_secret: str) -> None:
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
        for h in self._exec_handlers:
            try:
                await h(label, row)
            except Exception as e:
                log.exception("WS dispatch execution handler error (%s): %r", label, e)

    async def _dispatch_position(self, label: str, row: Dict[str, Any]) -> None:
        for h in self._pos_handlers:
            try:
                await h(label, row)
            except Exception as e:
                log.exception("WS dispatch position handler error (%s): %r", label, e)

    # ---------- Run loop ----------

    async def run_forever(self) -> None:
        if not self._clients:
            log.warning("WsSwitchboard started with NO accounts configured.")
            while True:
                await asyncio.sleep(10.0)

        tasks = []
        for label, client in self._clients.items():
            log.info("WS[%s] launching run_forever task", label)
            t = asyncio.create_task(client.run_forever(), name=f"ws-{label}")
            tasks.append(t)

        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            log.info("WsSwitchboard cancelled; shutting down.")
            raise
        except Exception as e:
            log.exception("WsSwitchboard fatal error: %r", e)
            # caller (ws switchboard bot) decides restart behavior
