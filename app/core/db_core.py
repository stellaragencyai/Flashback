#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — DB Core Compatibility Shim

Older code may import `core.db_core` or `app.core.db_core`.
We just re-export the real DB API from db.py.
"""

from __future__ import annotations

from .db import *  # noqa: F401,F403
