# core/__init__.py
"""
Flashback — root-level core package

Purpose:
- Allow imports like `from core.config import settings`, `from core.logger import get_logger`, etc.
- Keep this file *lightweight* and do NOT import from app.core here to avoid circular / missing module issues.
"""

# Intentionally empty: submodules (config, logger, bybit_client, etc.)
# are imported directly, e.g. `from core.config import settings`.
