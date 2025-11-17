#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — DB Core Compatibility Shim

Purpose
-------
Older modules may import `app.core.db_core` while the real implementation
now lives in `app.core.db`.

This file simply re-exports everything from db.py so both old and new
imports keep working.
"""

from __future__ import annotations

# Re-export public API from db
from .db import *  # noqa: F401,F403
