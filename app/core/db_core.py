#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — DB Core Compatibility Shim (root/core/db_core.py)

Purpose:
    Older code may do `from core.db_core import ...`.
    The real implementation now lives in `app.core.db`.

This shim simply re-exports everything from `app.core.db`.
"""

from __future__ import annotations

try:
    from app.core.db import *  # noqa: F401,F403
except Exception as e:
    raise ImportError("db_core shim cannot locate app.core.db") from e
