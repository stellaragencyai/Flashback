#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations

try:
    from core.db import *  # noqa
except Exception as e:
    raise ImportError("db_core shim cannot locate core/db.py") from e
