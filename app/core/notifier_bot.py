# app/core/notifier_bot.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Telegram notifier hub

Responsibilities:
- One TelegramNotifier per logical "channel" (main, journal, drip, subaccounts, etc.).
- Centralized rate limiting (per process, per channel).
- Simple .info() / .warn() / .error() / .trade() methods.
- Backwards-compatible tg_send(text, channel="main", level="info") helper for old code.
"""

from __future__ import annotations

import os
import time
import threading
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict

import requests

from app.core.logger import get_logger

# ---------------------------------------------------------------------------
# Early .env loading (so TG_TOKEN_SUB_* is available at import time)
# ---------------------------------------------------------------------------

try:
    from pathlib import Path
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    Path = None  # type: ignore
    load_dotenv = None  # type: ignore

if Path is not None and load_dotenv is not None:
    try:
        ROOT = Path(__file__).resolve().parents[2]
        ENV_PATH = ROOT / ".env"
        if ENV_PATH.exists():
            load_dotenv(ENV_PATH)
    except Exception:
        # Never let env loading kill the process
        pass

log = get_logger("notifier_bot")

# ---------------------------------------------------------------------------
# Env wiring
# ---------------------------------------------------------------------------

# Main & system channels
TG_TOKEN_MAIN = os.getenv("TG_TOKEN_MAIN", "")
TG_CHAT_MAIN = os.getenv("TG_CHAT_MAIN", "")

TG_TOKEN_JOURNAL = os.getenv("TG_TOKEN_JOURNAL", "")
TG_CHAT_JOURNAL = os.getenv("TG_CHAT_JOURNAL", "")

TG_TOKEN_DRIP = os.getenv("TG_TOKEN_DRIP", "")
TG_CHAT_DRIP = os.getenv("TG_CHAT_DRIP", "")

TG_TOKEN_PROFIT_SWEEPER = os.getenv("TG_TOKEN_PROFIT_SWEEPER", "")
TG_CHAT_PROFIT_SWEEPER = os.getenv("TG_CHAT_PROFIT_SWEEPER", "")

TG_TOKEN_HEALTH = os.getenv("TG_TOKEN_HEALTH", "")
TG_CHAT_HEALTH = os.getenv("TG_CHAT_HEALTH", "")

# Subaccount channels (flashback01..flashback10)
TG_TOKEN_SUB_1 = os.getenv("TG_TOKEN_SUB_1", "")
TG_CHAT_SUB_1 = os.getenv("TG_CHAT_SUB_1", "")

TG_TOKEN_SUB_2 = os.getenv("TG_TOKEN_SUB_2", "")
TG_CHAT_SUB_2 = os.getenv("TG_CHAT_SUB_2", "")

TG_TOKEN_SUB_3 = os.getenv("TG_TOKEN_SUB_3", "")
TG_CHAT_SUB_3 = os.getenv("TG_CHAT_SUB_3", "")

TG_TOKEN_SUB_4 = os.getenv("TG_TOKEN_SUB_4", "")
TG_CHAT_SUB_4 = os.getenv("TG_CHAT_SUB_4", "")

TG_TOKEN_SUB_5 = os.getenv("TG_TOKEN_SUB_5", "")
TG_CHAT_SUB_5 = os.getenv("TG_CHAT_SUB_5", "")

TG_TOKEN_SUB_6 = os.getenv("TG_TOKEN_SUB_6", "")
TG_CHAT_SUB_6 = os.getenv("TG_CHAT_SUB_6", "")

TG_TOKEN_SUB_7 = os.getenv("TG_TOKEN_SUB_7", "")
TG_CHAT_SUB_7 = os.getenv("TG_CHAT_SUB_7", "")

TG_TOKEN_SUB_8 = os.getenv("TG_TOKEN_SUB_8", "")
TG_CHAT_SUB_8 = os.getenv("TG_CHAT_SUB_8", "")

TG_TOKEN_SUB_9 = os.getenv("TG_TOKEN_SUB_9", "")
TG_CHAT_SUB_9 = os.getenv("TG_CHAT_SUB_9", "")

TG_TOKEN_SUB_10 = os.getenv("TG_TOKEN_SUB_10", "")
TG_CHAT_SUB_10 = os.getenv("TG_CHAT_SUB_10", "")

HTTP_TIMEOUT = float(os.getenv("TG_HTTP_TIMEOUT", "6"))

# Global (per-process) send caps default
DEFAULT_MAX_30S = int(os.getenv("TG_MAX_MSGS_PER_30S", "10"))
DEFAULT_MAX_300S = int(os.getenv("TG_MAX_MSGS_PER_300S", "80"))

# ---------------------------------------------------------------------------
# TelegramNotifier implementation
# ---------------------------------------------------------------------------


@dataclass
class TelegramNotifier:
    channel: str
    token: str
    chat_id: str
    level: str = "info"
    max_30s: int = DEFAULT_MAX_30S
    max_300s: int = DEFAULT_MAX_300S
    prefix: str = ""

    def __post_init__(self) -> None:
        self._lock = threading.Lock()
        self._times_30s: Deque[float] = deque(maxlen=512)
        self._times_300s: Deque[float] = deque(maxlen=2048)

        token_present = bool(self.token)
        token_prefix = (self.token[:8] + "...") if token_present else ""
        log.info(
            "[TG:init] channel='%s', token_present=%s, token_prefix=%s, chat_id='%s', "
            "level=%s, max_30s=%s, max_300s=%s, prefix='%s'",
            self.channel,
            token_present,
            token_prefix,
            self.chat_id or "",
            self.level,
            self.max_30s,
            self.max_300s,
            self.prefix,
        )

    # ---------- public API ----------

    def info(self, msg: str) -> None:
        self._send("INFO", msg)

    def warn(self, msg: str) -> None:
        self._send("WARN", f"⚠️ {msg}")

    def error(self, msg: str) -> None:
        self._send("ERROR", f"❌ {msg}")

    def trade(self, msg: str) -> None:
        # Trade-specific prefix, but still rate-limited like others
        self._send("TRADE", f"💹 {msg}")

    def debug(self, msg: str) -> None:
        # Usually suppressed by max_30s/max_300s anyway
        self._send("DEBUG", msg)

    # ---------- internal helpers ----------

    def _should_send(self) -> bool:
        """
        Per-notifier, per-process rate limit:
          - at most max_30s in sliding 30s window
          - at most max_300s in sliding 300s window
        """
        if not self.token or not self.chat_id:
            return False

        now = time.time()
        with self._lock:
            # purge old timestamps
            while self._times_30s and now - self._times_30s[0] > 30.0:
                self._times_30s.popleft()
            while self._times_300s and now - self._times_300s[0] > 300.0:
                self._times_300s.popleft()

            if self.max_30s > 0 and len(self._times_30s) >= self.max_30s:
                log.debug(
                    "[TG:%s] Rate limit hit; dropping message. last_30s=%d, last_300s=%d",
                    self.channel,
                    len(self._times_30s),
                    len(self._times_300s),
                )
                return False

            if self.max_300s > 0 and len(self._times_300s) >= self.max_300s:
                log.debug(
                    "[TG:%s] 5-min rate limit hit; dropping message. last_30s=%d, last_300s=%d",
                    self.channel,
                    len(self._times_30s),
                    len(self._times_300s),
                )
                return False

            self._times_30s.append(now)
            self._times_300s.append(now)
            return True

    def _send(self, level: str, msg: str) -> None:
        if not self._should_send():
            return

        text = f"{self.prefix}{msg}" if self.prefix else msg
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"

        try:
            requests.post(
                url,
                json={"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True},
                timeout=HTTP_TIMEOUT,
            )
        except Exception as e:
            # Do NOT let TG flakiness break bots
            log.debug("[TG:%s] send failed at level %s: %r", self.channel, level, e)


# ---------------------------------------------------------------------------
# Notifier factory & registry
# ---------------------------------------------------------------------------

_NOTIFIERS: Dict[str, TelegramNotifier] = {}
_BUILD_LOCK = threading.Lock()


def _build_notifier(channel: str) -> TelegramNotifier:
    """
    Internal: construct a TelegramNotifier for a given logical channel.

    Known channels:
      - main
      - journal
      - drip
      - profit_sweeper
      - health
      - flashback01..flashback10

    Unknown channels fall back to main.
    """
    ch = channel.lower().strip()

    # Defaults
    token = TG_TOKEN_MAIN
    chat_id = TG_CHAT_MAIN
    prefix = ""
    max_30s = DEFAULT_MAX_30S
    max_300s = DEFAULT_MAX_300S

    if ch == "main":
        token, chat_id = TG_TOKEN_MAIN, TG_CHAT_MAIN
        prefix = ""
    elif ch == "journal":
        token, chat_id = TG_TOKEN_JOURNAL, TG_CHAT_JOURNAL
        prefix = "📒 "
    elif ch == "drip":
        token, chat_id = TG_TOKEN_DRIP, TG_CHAT_DRIP
        prefix = "💧 "
    elif ch == "profit_sweeper":
        token, chat_id = TG_TOKEN_PROFIT_SWEEPER, TG_CHAT_PROFIT_SWEEPER
        prefix = "🧹 "
    elif ch == "health":
        token, chat_id = TG_TOKEN_HEALTH, TG_CHAT_HEALTH
        prefix = "🩺 "

    # Subaccounts: flashback01..flashback10
    elif ch == "flashback01":
        token, chat_id = TG_TOKEN_SUB_1, TG_CHAT_SUB_1
        prefix = "🧪[fb01] "
    elif ch == "flashback02":
        token, chat_id = TG_TOKEN_SUB_2, TG_CHAT_SUB_2
        prefix = "🧪[fb02] "
    elif ch == "flashback03":
        token, chat_id = TG_TOKEN_SUB_3, TG_CHAT_SUB_3
        prefix = "🧪[fb03] "
    elif ch == "flashback04":
        token, chat_id = TG_TOKEN_SUB_4, TG_CHAT_SUB_4
        prefix = "🧪[fb04] "
    elif ch == "flashback05":
        token, chat_id = TG_TOKEN_SUB_5, TG_CHAT_SUB_5
        prefix = "🧪[fb05] "
    elif ch == "flashback06":
        token, chat_id = TG_TOKEN_SUB_6, TG_CHAT_SUB_6
        prefix = "🧪[fb06] "
    elif ch == "flashback07":
        token, chat_id = TG_TOKEN_SUB_7, TG_CHAT_SUB_7
        prefix = "🧪[fb07] "
    elif ch == "flashback08":
        token, chat_id = TG_TOKEN_SUB_8, TG_CHAT_SUB_8
        prefix = "🧪[fb08] "
    elif ch == "flashback09":
        token, chat_id = TG_TOKEN_SUB_9, TG_CHAT_SUB_9
        prefix = "🧪[fb09] "
    elif ch == "flashback10":
        token, chat_id = TG_TOKEN_SUB_10, TG_CHAT_SUB_10
        prefix = "🧪[fb10] "

    # If token/chat_id are missing, notifier is "disabled" but still safe to call.
    return TelegramNotifier(
        channel=ch,
        token=token,
        chat_id=chat_id,
        level="info",
        max_30s=max_30s,
        max_300s=max_300s,
        prefix=prefix,
    )


def get_notifier(channel: str = "main") -> TelegramNotifier:
    """
    Get (and cache) a TelegramNotifier for a given channel.
    """
    ch = channel.lower().strip()
    with _BUILD_LOCK:
        if ch not in _NOTIFIERS:
            _NOTIFIERS[ch] = _build_notifier(ch)
        return _NOTIFIERS[ch]


# ---------------------------------------------------------------------------
# Backwards-compatible helper: tg_send
# ---------------------------------------------------------------------------

def tg_send(text: str, channel: str = "main", level: str = "info") -> None:
    """
    Legacy helper, kept for older bots like executor_v2.

    Usage patterns seen historically:
      from app.core.notifier_bot import tg_send
      tg_send("message")                    -> main.info
      tg_send("msg", channel="journal")     -> journal.info
      tg_send("msg", channel="main", level="error") -> main.error

    This is now just a thin wrapper over get_notifier(...).
    """
    try:
        n = get_notifier(channel)
        lvl = (level or "info").lower()
        if lvl == "warn":
            n.warn(text)
        elif lvl == "error":
            n.error(text)
        elif lvl == "trade":
            n.trade(text)
        elif lvl == "debug":
            n.debug(text)
        else:
            n.info(text)
    except Exception as e:
        # This must NEVER crash bots
        log.debug("tg_send failed for channel=%s: %r", channel, e)
