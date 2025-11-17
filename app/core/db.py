import os

def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)) or str(default)
    # strip inline comments and whitespace
    raw = raw.split("#", 1)[0].strip()
    try:
        return int(raw)
    except ValueError:
        return default

POLL_SECONDS   = _env_int("TPM_POLL_SECONDS", 2)
_ATR_CACHE_TTL = _env_int("TPM_ATR_CACHE_SEC", 60)
