#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Central Telegram Notifier (env-compatible with current .env)

Purpose:
- Provide a single, sane place to send Telegram messages from all bots.
- Support multiple Telegram bots (main + up to 10 sub-bots + journal bot + drip bot + profit_sweeper + health).
- Enforce per-bot:
    • Rate limiting (no message floods).
    • Message de-duplication (no identical spam every second).
    • 429-aware mute using Telegram's retry_after.
    • Severity levels: info / warn / error.
- Simple API for bots:

    from app.core.notifier_bot import get_notifier, tg_send

    tg = get_notifier("flashback01")   # or "main", "journal", "drip", "profit_sweeper", "health"
    tg.info("flashback01 bot started")
    tg.trade("Opened LONG BTCUSDT ...")
    tg.error("Exception in executor: ...")

ENV EXPECTATIONS (matches your current .env):

  # Main bot
  TG_TOKEN_MAIN=...
  TG_CHAT_MAIN=7776809236
  TG_LEVEL_MAIN=info      # optional: info | warn | error

  # Journal bot (optional, for trade_journal.py)
  TG_TOKEN_JOURNAL=...
  TG_CHAT_JOURNAL=...
  TG_LEVEL_JOURNAL=info   # optional

  # Drip bot (for equity_drip_bot)
  TG_TOKEN_DRIP=...
  TG_CHAT_DRIP=...
  TG_LEVEL_DRIP=info      # optional

  # Profit Sweeper bot (for profit_sweeper.py)
  TG_TOKEN_PROFIT_SWEEPER=...
  TG_CHAT_PROFIT_SWEEPER=...
  TG_LEVEL_PROFIT_SWEEPER=info  # optional

  # Health / infra bot (supervisor, switchboard, etc.)
  TG_TOKEN_HEALTH=...
  TG_CHAT_HEALTH=...
  TG_LEVEL_HEALTH=info    # optional

  # Subaccount bots
  TG_TOKEN_SUB_1=...
  TG_CHAT_SUB_1=...
  TG_LEVEL_SUB_1=info     # optional

  ...
  TG_TOKEN_SUB_10=...
  TG_CHAT_SUB_10=...
  TG_LEVEL_SUB_10=warn    # example

Global defaults:
  TG_LEVEL_DEFAULT=info
  TG_MAX_PER_30S=10                 # global default per-notifier rate
  TG_MAX_PER_300S=80

Per-channel overrides:
  TG_MAX_PER_30S_MAIN=5
  TG_MAX_PER_300S_MAIN=40
  TG_PREFIX_MAIN=[MAIN]

  TG_MAX_PER_30S_JOURNAL=3
  TG_PREFIX_JOURNAL=[JOURNAL]

Error mirroring:
  TG_MIRROR_ERRORS_TO_HEALTH=true|false   # mirror .error() from all channels into "health" (if configured)
"""

from __future__ import annotations

import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Deque, Optional, List

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
# Safe printing for Windows consoles (no emoji explosions)
# ---------------------------------------------------------------------------

def _safe_print(text: str) -> None:
    """
    Print text in a way that won't crash on cp1252 consoles when emojis are present.
    Non-encodable chars are replaced.
    """
    try:
        # Try normal print first
        print(text)
    except UnicodeEncodeError:
        # Fallback: encode with replacement and write via buffer
        enc = sys.stdout.encoding or "utf-8"
        try:
            sys.stdout.buffer.write((text + "\n").encode(enc, errors="replace"))
        except Exception:
            # As a last resort, strip to ASCII-ish
            ascii_only = text.encode("ascii", errors="replace").decode("ascii", errors="ignore")
            print(ascii_only)


# ---------------------------------------------------------------------------
# Constants & config
# ---------------------------------------------------------------------------

# Rate limits (global defaults, per notifier)
TG_MAX_PER_30S = int(os.getenv("TG_MAX_PER_30S", "10"))   # msgs per 30 seconds
TG_MAX_PER_300S = int(os.getenv("TG_MAX_PER_300S", "80")) # msgs per 5 minutes

# Dedup window: identical text will not be sent more than once in this interval
TG_DEDUP_WINDOW_SEC = int(os.getenv("TG_DEDUP_WINDOW_SEC", "30"))

# Global timeout for Telegram requests
TG_TIMEOUT_SEC = float(os.getenv("TG_TIMEOUT_SEC", "6.0"))

# Mirror all .error(...) into "health" channel (if configured)
def _parse_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    return str(val).strip().lower() in ("1", "true", "yes", "on")

TG_MIRROR_ERRORS_TO_HEALTH = _parse_bool(os.getenv("TG_MIRROR_ERRORS_TO_HEALTH"), False)

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


def _channel_env_suffix(name: str) -> str:
    """
    Convert logical channel name -> ENV suffix, e.g.:
      "main"          -> "MAIN"
      "journal"       -> "JOURNAL"
      "flashback01"   -> "FLASHBACK01"
      "profit_sweeper"-> "PROFIT_SWEEPER"
      "health"        -> "HEALTH"
    """
    return name.strip().upper().replace("-", "_")


# ---------------------------------------------------------------------------
# Channel → env mapping (matches your .env naming)
# ---------------------------------------------------------------------------
# Logical names you will use in code:
#   "main"
#   "journal"
#   "drip"
#   "profit_sweeper"
#   "health"
#   "flashback01" .. "flashback10"
#
# These map to:
#   main           -> TG_TOKEN_MAIN / TG_CHAT_MAIN / TG_LEVEL_MAIN
#   journal        -> TG_TOKEN_JOURNAL / TG_CHAT_JOURNAL / TG_LEVEL_JOURNAL
#   drip           -> TG_TOKEN_DRIP / TG_CHAT_DRIP / TG_LEVEL_DRIP
#   profit_sweeper -> TG_TOKEN_PROFIT_SWEEPER / TG_CHAT_PROFIT_SWEEPER / TG_LEVEL_PROFIT_SWEEPER
#   health         -> TG_TOKEN_HEALTH / TG_CHAT_HEALTH / TG_LEVEL_HEALTH
#   flashback01    -> TG_TOKEN_SUB_1 / TG_CHAT_SUB_1 / TG_LEVEL_SUB_1
#   ...
#   flashback10    -> TG_TOKEN_SUB_10 / TG_CHAT_SUB_10 / TG_LEVEL_SUB_10

CHANNEL_ENV_KEYS: Dict[str, Dict[str, str]] = {
    "main": {
        "token": "TG_TOKEN_MAIN",
        "chat": "TG_CHAT_MAIN",
        "level": "TG_LEVEL_MAIN",
    },
    "journal": {
        "token": "TG_TOKEN_JOURNAL",
        "chat": "TG_CHAT_JOURNAL",
        "level": "TG_LEVEL_JOURNAL",
    },
    # 💧 Drip bot notifier
    "drip": {
        "token": "TG_TOKEN_DRIP",
        "chat": "TG_CHAT_DRIP",
        "level": "TG_LEVEL_DRIP",
    },
    # Profit Sweeper
    "profit_sweeper": {
        "token": "TG_TOKEN_PROFIT_SWEEPER",
        "chat": "TG_CHAT_PROFIT_SWEEPER",
        "level": "TG_LEVEL_PROFIT_SWEEPER",
    },
    # Health / infra channel
    "health": {
        "token": "TG_TOKEN_HEALTH",
        "chat": "TG_CHAT_HEALTH",
        "level": "TG_LEVEL_HEALTH",
    },
}

# Add flashback01..flashback10 mappings
for i in range(1, 11):
    name = f"flashback{i:02d}"  # flashback01, flashback02, ...
    CHANNEL_ENV_KEYS[name] = {
        "token": f"TG_TOKEN_SUB_{i}",
        "chat": f"TG_CHAT_SUB_{i}",
        "level": f"TG_LEVEL_SUB_{i}",
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

    # Per-channel rate limits (override global defaults if set)
    max_per_30s: int = TG_MAX_PER_30S
    max_per_300s: int = TG_MAX_PER_300S

    # Optional channel prefix, e.g. "[MAIN]"
    prefix: str = ""

    _session: requests.Session = field(default_factory=requests.Session, repr=False)

    # ---- Convenience flags -------------------------------------------------

    @property
    def enabled(self) -> bool:
        """
        True if this notifier has both token and chat_id configured.
        Safe to use from other modules: `if tg.enabled: ...`
        """
        return bool(self.token and self.chat_id)

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

    # ---- Internal helpers --------------------------------------------------

    def _apply_prefix(self, text: str) -> str:
        if self.prefix:
            return f"{self.prefix} {text}"
        return text

    # ---- Core send logic ---------------------------------------------------

    def _send(self, text: str, level: str = "info") -> None:
        """
        Core send method with:
          - severity filtering
          - mute handling
          - rate limiting
          - dedup
          - 429-aware backoff
          - optional mirroring of errors to 'health'
        """
        if not self.token or not self.chat_id:
            _safe_print(f"[TG:{self.name}] No token/chat configured; skipping message.")
            return

        level = _parse_level(level)
        if _LEVEL_MAP[level] < _LEVEL_MAP[self.min_level]:
            # Below this notifier's minimum severity; ignore
            return

        # Apply channel prefix
        text = self._apply_prefix(text)

        now = time.time()
        rs = self.rate_state

        # 429 mute check
        if now < rs.muted_until:
            # Still in mute window; silently drop
            return

        # Dedup: same text within window -> drop
        last_t = rs.last_msg_time.get(text)
        if last_t is not None and (now - last_t) < TG_DEDUP_WINDOW_SEC:
            return

        # Rate limiting
        self._trim_rates(now)

        if len(rs.last_30s) >= self.max_per_30s or len(rs.last_300s) >= self.max_per_300s:
            _safe_print(
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
            _safe_print(f"[TG:{self.name}] Exception: {type(e).__name__}: {e}")
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
            _safe_print(
                f"[TG:{self.name}] 429 Too Many Requests. "
                f"Muting for {retry_after} seconds."
            )
            return

        if not resp.ok:
            _safe_print(f"[TG:{self.name}] HTTP {resp.status_code} error: {resp.text!r}")

        # Optional: mirror errors to "health" channel
        if TG_MIRROR_ERRORS_TO_HEALTH and level == "error" and self.name != "health":
            try:
                health = get_notifier("health")
                if health.enabled:
                    # no prefix from 'health' about itself, just tag original channel
                    health.raw(f"[{self.name}] {text}", level="error")
            except Exception:
                # Don't let mirroring kill the original notifier
                pass

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


def _load_channel_config(name: str) -> TelegramNotifier:
    """
    Load token, chat_id, and level for a logical channel name.

    name: "main", "journal", "drip", "profit_sweeper", "health",
          "flashback01", "flashback02", ..., "flashback10"
    """
    cfg = CHANNEL_ENV_KEYS.get(name, {})
    token_key = cfg.get("token")
    chat_key = cfg.get("chat")
    level_key = cfg.get("level")

    token = os.getenv(token_key, "") if token_key else ""
    chat_id = os.getenv(chat_key, "") if chat_key else ""

    # Level: TG_LEVEL_MAIN / TG_LEVEL_SUB_1 / TG_LEVEL_JOURNAL / TG_LEVEL_DRIP / etc.
    # Fallback to TG_LEVEL_DEFAULT.
    level_raw = os.getenv(level_key) if level_key else None
    if not level_raw:
        level_raw = os.getenv("TG_LEVEL_DEFAULT", "info")
    level = _parse_level(level_raw, default="info")

    # Per-channel rate overrides & prefix
    suffix = _channel_env_suffix(name)
    max_30 = int(os.getenv(f"TG_MAX_PER_30S_{suffix}", str(TG_MAX_PER_30S)))
    max_300 = int(os.getenv(f"TG_MAX_PER_300S_{suffix}", str(TG_MAX_PER_300S)))
    prefix = os.getenv(f"TG_PREFIX_{suffix}", "").strip()

    notifier = TelegramNotifier(
        name=name,
        token=token,
        chat_id=chat_id,
        min_level=level,
        max_per_30s=max_30,
        max_per_300s=max_300,
        prefix=prefix,
    )

    token_hint = (token[:8] + "...") if token else "None"
    _safe_print(
        f"[TG:init] channel={name!r}, token_present={bool(token)}, "
        f"token_prefix={token_hint}, chat_id={chat_id!r}, level={level}, "
        f"max_30s={max_30}, max_300s={max_300}, prefix={prefix!r}"
    )

    return notifier


def get_notifier(name: str = "main") -> TelegramNotifier:
    """
    Get (or create) a TelegramNotifier for the given logical name.

    Usage:
        from app.core.notifier_bot import get_notifier

        tg_main   = get_notifier("main")
        tg_fb01   = get_notifier("flashback01")
        tg_journal= get_notifier("journal")
        tg_drip   = get_notifier("drip")
        tg_ps     = get_notifier("profit_sweeper")
        tg_health = get_notifier("health")

        tg_main.info("Supervisor starting...")
        tg_fb01.trade("flashback01: LONG BTCUSDT 25x at 61234.5")
        tg_journal.info("Journal: new closed trade logged.")
        tg_drip.info("Drip bot online.")
        tg_ps.info("Profit sweeper run completed.")
        tg_health.warn("WS bus stale for 15s.")
    """
    name = name.strip()
    if name in _NOTIFIERS:
        return _NOTIFIERS[name]

    notifier = _load_channel_config(name)
    _NOTIFIERS[name] = notifier
    return notifier


# ---------------------------------------------------------------------------
# Optional: startup summary (now safe for lame consoles)
# ---------------------------------------------------------------------------

def _startup_summary() -> None:
    """
    Print a short summary of which channels have tokens configured.
    No emojis, so Windows doesn't have a meltdown.
    """
    lines: List[str] = []
    lines.append("Telegram notifier startup summary:")
    for name, keys in CHANNEL_ENV_KEYS.items():
        token_key = keys["token"]
        chat_key = keys["chat"]
        token = os.getenv(token_key, "")
        chat_id = os.getenv(chat_key, "")
        status = "OK" if token and chat_id else "MISSING"
        lines.append(f"  - {name}: {status} (token_key={token_key}, chat_key={chat_key})")
    summary = "\n".join(lines)
    _safe_print(summary)


_startup_summary()


def tg_send(
    msg: str,
    level: str = "info",
    channel: str = "main",
) -> None:
    """
    Backwards-compatible convenience wrapper used by older bots.

    - channel: which notifier to use ("main", "journal", "drip", "profit_sweeper",
               "health", "flashback01", etc.)
    - level:   "info" | "error" | "trade"
    """
    try:
        notifier = get_notifier(channel)
    except Exception:
        # Fallback: try main channel if anything goes weird
        try:
            notifier = get_notifier("main")
        except Exception:
            return

    level = (level or "info").lower().strip()
    if level == "error":
        notifier.error(msg)
    elif level == "trade":
        notifier.trade(msg)
    else:
        notifier.info(msg)
