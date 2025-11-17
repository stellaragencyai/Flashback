#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Root-level db_core shim.

Allows legacy imports: `from core.db_core import ...`
to work by delegating to app.core.db.
"""

from __future__ import annotations

from app.core.db import *  # noqa: F401,F403
