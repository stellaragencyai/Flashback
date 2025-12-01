#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Error Helpers

Classify exceptions to decide:
  - Is it transient (network / WS)?
  - Is it config / permission?
  - Is it logic / bug?

Used by bots + supervisor to send better Telegram messages.
"""

from typing import Tuple, Optional, Type
import requests
import websockets
import socket


TRANSIENT_EXCEPTIONS: Tuple[Type[BaseException], ...] = (
    requests.exceptions.Timeout,
    requests.exceptions.ConnectionError,
    socket.timeout,
    websockets.exceptions.ConnectionClosedError,
)


def classify_error(exc: BaseException) -> str:
    """
    Return one of: "transient", "config", "permission", "logic"
    """
    msg = str(exc).lower()

    for t in TRANSIENT_EXCEPTIONS:
        if isinstance(exc, t):
            return "transient"

    # Bybit-style permission / auth errors
    if "api key" in msg or "api-key" in msg or "invalid api" in msg:
        return "permission"
    if "not authorized" in msg or "forbidden" in msg:
        return "permission"

    # Configuration
    if "no such file or directory" in msg:
        return "config"
    if "env" in msg and "missing" in msg:
        return "config"
    if "yaml" in msg and "parse" in msg:
        return "config"

    # Default
    return "logic"
