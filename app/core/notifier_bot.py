#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Central Telegram Notifier

Purpose:
- Provide a single, sane place to send Telegram messages from all bots.
- Support multiple Telegram bots (main, flashback01..flashback10).
- Enforce:
    • Per-bot rate limiting (no message floods).
    • Message de-duplication (no identical spam).
    • 429-aware mute using Telegram's retry_after.
    • Severity levels: info / warn / error.
- Simple API for bots:
    from app.core.notifier_bot import get_notifier

    tg = get_notifier("flashback01")   # or "main"
    tg.info("flashback01 bot started")
    tg.trade("Opened LONG BTCUSDT ...")
    tg.error("Exception in executor: ...")

Env layout (example):

  # Main notifier
  TG_MAIN_TOKEN=123456:ABC...
  TG_MAIN_CHAT=7776809236
  TG_LEVEL_MAIN=info      # optional: info | warn | error

  # Flashback subaccount bots (01..10; use as many as you want)
  TG_FB01_TOKEN=...
  TG_FB01_CHAT=...
  TG_LEVEL_FB01=info

  TG_FB02_TOKEN=...
  TG_FB02_CHAT=...

  ...
  TG_FB10_TOKEN=...
  TG_FB10_CHAT=...

Notes:
- You may use the SAME chat id for all of them if you want a single stream.
- Or separate chats per bot if you like isolation; the code doesn't care.
"""

from __future__ import annotations

import os
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Deque, Optional, Tuple

import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load .env from project root (two levels up: app/core -> app -> ROOT)
# ---------------------------------------------------------------------------

THIS_FILE = Path(__file__).resolve()
ROOT_DIR = THIS_FILE.parents[2]  # .../Flashback
ENV_PATH = ROOT_DIR / ".env"

load_dotenv(ENV_PATH)


# ---------------------------------------------------------------------------
# Constants & config
# ---------------------------------------------------------------------------

# Rate limits (per notifier, i.e., per token+chat pair)
# These are intentionally conservative. Adjust via env if needed.
TG_MAX_PER_30S = int(os.getenv("TG_MAX_PER_30S", "10"))   # msgs per 30 seconds
TG_MAX_PER_300S = int(os.getenv("TG_MAX_PER_300S", "80")) # msgs per 5 minutes

# Dedup window: identical text will not be sent more than once in this interval
TG_DEDUP_WINDOW_SEC = int(os.getenv("TG_DEDUP_WINDOW_SEC", "30"))

# Global timeout for Telegram requests
TG_TIMEOUT_SEC = float(os.getenv("TG_TIMEOUT_SEC", "6.0"))

# Severity mapping
_LEVEL_MAP = {
    "info": 10,
    "warn": 20,
    "warning": 20,
    "error": 30,
}


def _parse_level(s: Optional[str], default: str = "info") -> str:
    if not s:
        return default
    s = s.strip().lower()
    return s if s in _LEVEL_MAP else default


# ---------------------------------------------------------------------------
# Channel mapping: logical name -> env prefix
# ---------------------------------------------------------------------------

# You can extend this dict as needed. Keys are what bots call `get_notifier` with.
CHANNEL_ENV_PREFIX: Dict[str, str] = {
    "main": "TG_MAIN",
    "flashback01": "TG_FB01",
    "flashback02": "TG_FB02",
    "flashback03": "TG_FB03",
    "flashback04": "TG_FB04",
    "flashback05": "TG_FB05",
    "flashback06": "TG_FB06",
    "flashback07": "TG_FB07",
    "flashback08": "TG_FB08",
    "flashback09": "TG_FB09",
    "flashback10": "TG_FB10",
}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class _RateState:
    # Deques of timestamps for sliding windows
    last_30s: Deque[float] = field(default_factory=deque)
    last_300s: Deque[float] = field(default_factory=deque)
    # Mute state after 429
    muted_until: float = 0.0
    # Dedup: message -> last_timestamp
    last_msg_time: Dict[str, float] = field(default_factory=dict)


@dataclass
class TelegramNotifier:
    """
    Notifier bound to a single Telegram bot (token + chat_id + min severity).
    Handles:
      - Rate limiting
      - Deduplication
      - 429 backoff
    """
    name: str
    token: str
    chat_id: str
    min_level: str = "info"
    rate_state: _RateState = field(default_factory=_RateState)

    # Internal session for performance
    _session: requests.Session = field(default_factory=requests.Session, repr=False)

    # ---- Public API --------------------------------------------------------

    def info(self, text: str) -> None:
        self._send(text, level="info")

    def warn(self, text: str) -> None:
        self._send(text, level="warn")

    def error(self, text: str) -> None:
        self._send(text, level="error")

    def trade(self, text: str) -> None:
        """
        Convenience method for trade-related messages.
        Treated as 'info' by default.
        """
        self._send(f"💹 {text}", level="info")

    def raw(self, text: str, level: str = "info") -> None:
        """
        Send a raw message with explicit level.
        """
        self._send(text, level=level)

    # ---- Core send logic ---------------------------------------------------

    def _send(self, text: str, level: str = "info") -> None:
        """
        Core send method with:
          - severity filtering
          - mute handling
          - rate limiting
          - dedup
          - 429-aware backoff
        """
        if not self.token or not self.chat_id:
            print(f"[TG:{self.name}] No token/chat configured; skipping message.")
            return

        level = _parse_level(level)
        if _LEVEL_MAP[level] < _LEVEL_MAP[self.min_level]:
            # Below this notifier's minimum severity; ignore
            return

        now = time.time()
        rs = self.rate_state

        # 429 mute check
        if now < rs.muted_until:
            # Still in mute window; silently drop
            return

        # Dedup: same text within window -> drop
        last_t = rs.last_msg_time.get(text)
        if last_t is not None and (now - last_t) < TG_DEDUP_WINDOW_SEC:
            # Too soon to repeat same text
            return

        # Rate limiting
        self._trim_rates(now)

        if len(rs.last_30s) >= TG_MAX_PER_30S or len(rs.last_300s) >= TG_MAX_PER_300S:
            # Too many messages recently; drop to avoid spam / 429
            # We do NOT send a "suppressed" TG message to avoid recursion.
            print(
                f"[TG:{self.name}] Rate limit hit; "
                f"dropping message. last_30s={len(rs.last_30s)}, last_300s={len(rs.last_300s)}"
            )
            return

        # Ready to send; update rate state preemptively
        rs.last_30s.append(now)
        rs.last_300s.append(now)
        rs.last_msg_time[text] = now

        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
        }

        try:
            resp = self._session.post(url, json=payload, timeout=TG_TIMEOUT_SEC)
        except Exception as e:
            print(f"[TG:{self.name}] Exception: {type(e).__name__}: {e}")
            # Don't crash bots because Telegram is moody
            return

        if resp.status_code == 429:
            # Hard rate-limited by Telegram; respect retry_after
            retry_after = 60  # default fallback
            try:
                data = resp.json()
                retry_after = int(data.get("parameters", {}).get("retry_after", retry_after))
            except Exception:
                pass

            rs.muted_until = time.time() + retry_after
            print(
                f"[TG:{self.name}] 429 Too Many Requests. "
                f"Muting for {retry_after} seconds."
            )
            return

        if not resp.ok:
            # Log the error but don't crash.
            print(
                f"[TG:{self.name}] HTTP {resp.status_code} error: {resp.text!r}"
            )

    def _trim_rates(self, now: float) -> None:
        """Trim old timestamps from rate deques."""
        rs = self.rate_state
        cutoff_30 = now - 30.0
        cutoff_300 = now - 300.0

        while rs.last_30s and rs.last_30s[0] < cutoff_30:
            rs.last_30s.popleft()
        while rs.last_300s and rs.last_300s[0] < cutoff_300:
            rs.last_300s.popleft()


# ---------------------------------------------------------------------------
# Notifier registry
# ---------------------------------------------------------------------------

_NOTIFIERS: Dict[str, TelegramNotifier] = {}


def _load_channel_config(name: str) -> Tuple[str, str, str]:
    """
    Load token, chat_id, and level for a logical channel name.

    name: "main", "flashback01", "flashback02", etc.
    """
    prefix = CHANNEL_ENV_PREFIX.get(name)
    if not prefix:
        # Guess a prefix on the fly: uppercase name without spaces
        prefix = "TG_" + name.replace(" ", "").upper()

    token = os.getenv(f"{prefix}_TOKEN", "")
    chat_id = os.getenv(f"{prefix}_CHAT", "")
    level_raw = os.getenv(f"TG_LEVEL_{prefix.split('_', 1)[-1]}", None)
    # Fallback: TG_LEVEL_DEFAULT
    level_raw = level_raw or os.getenv("TG_LEVEL_DEFAULT", "info")

    level = _parse_level(level_raw, default="info")
    return token, chat_id, level


def get_notifier(name: str = "main") -> TelegramNotifier:
    """
    Get (or create) a TelegramNotifier for the given logical name.

    Usage:
        from app.core.notifier_bot import get_notifier

        tg = get_notifier("flashback01")
        tg.info("Bot started")
        tg.trade("Opened LONG BTCUSDT 25x at 0.1234")
    """
    name = name.strip()
    if name in _NOTIFIERS:
        return _NOTIFIERS[name]

    token, chat_id, level = _load_channel_config(name)

    notifier = TelegramNotifier(
        name=name,
        token=token,
        chat_id=chat_id,
        min_level=level,
    )
    _NOTIFIERS[name] = notifier

    # Print a small hint at creation time (no secrets leaked)
    token_hint = (token[:8] + "…") if token else "None"
    print(
        f"[TG:init] channel={name!r}, token_present={bool(token)}, "
        f"token_prefix={token_hint}, chat_id={chat_id!r}, level={level}"
    )

    return notifier
