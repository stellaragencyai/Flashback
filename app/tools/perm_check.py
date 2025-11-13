#!/usr/bin/env python3
# Flashback — Permission & Connectivity Check (main + subs)
# - Verifies required .env keys exist (main read/trade/transfer, Telegram)
# - Tests Bybit READ (wallet-balance) for main
# - Tests Bybit TRADE scope on main via cancel-all (no-op if no orders)
# - Tests Transfer key by listing recent inter-transfers (harmless)
# - Sends a Telegram ping on success
# - Iterates over up to 10 subaccounts and validates their READ/TRADE scopes
#
# Env expectations for subs:
#   One-key-per-sub is fine: set READ_* and TRADE_* to the same value.
#   BYBIT_SUB{N}_READ_KEY / BYBIT_SUB{N}_READ_SECRET
#   BYBIT_SUB{N}_TRADE_KEY / BYBIT_SUB{N}_TRADE_SECRET
#
# This script does not place any opening orders. The only trade call is cancel-all.

import os, sys
from typing import Optional, Tuple
from app.core.flashback_common import (
    bybit_get, bybit_post, send_tg,
    KEY_READ, SEC_READ, KEY_TRADE, SEC_TRADE,
    TG_TOKEN_MAIN, TG_CHAT_MAIN,
    KEY_XFER, SEC_XFER
)

MAJOR_SYMBOL = "BTCUSDT"  # harmless target for cancel-all

def _fail(msg: str) -> None:
    print(f"❌ {msg}")
    sys.exit(1)

def _warn(msg: str) -> None:
    print(f"⚠️  {msg}")

def _ok(msg: str) -> None:
    print(f"✅ {msg}")

def _has(val: Optional[str]) -> bool:
    return bool(val and str(val).strip())

def _check_main_read():
    try:
        r = bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"})
        lst = (r.get("result", {}) or {}).get("list", []) or []
        if not lst:
            _fail("Bybit READ succeeded but no UNIFIED account returned.")
        _ok("Main: READ scope OK (wallet-balance)")
    except Exception as e:
        _fail(f"Main: READ failed: {e}")

def _check_main_trade():
    try:
        bybit_post("/v5/order/cancel-all", {"category": "linear", "symbol": MAJOR_SYMBOL})
        _ok("Main: TRADE scope OK (cancel-all)")
    except Exception as e:
        _fail(f"Main: TRADE failed (enable Contract Trading on this key): {e}")

def _check_transfer_key():
    if not (_has(KEY_XFER) and _has(SEC_XFER)):
        _warn("Transfer: transfer keypair not set (skipping). Required for sweeps/drips.")
        return
    try:
        # harmless query for recent inter-transfers
        r = bybit_get("/v5/asset/transfer/query-inter-transfer-list",
                      {"limit": "5"}, key=KEY_XFER, secret=SEC_XFER)
        _ok("Transfer: scope OK (query inter-transfer list)")
    except Exception as e:
        _fail(f"Transfer: key present but scope failed (needs transfer permission): {e}")

def _check_telegram():
    if not (_has(TG_TOKEN_MAIN) and _has(TG_CHAT_MAIN)):
        _fail("Telegram: TG_TOKEN_MAIN and TG_CHAT_MAIN are required.")
    try:
        send_tg("🔧 Flashback perm_check: all good.")
        _ok("Telegram: OK (message sent)")
    except Exception as e:
        _fail(f"Telegram failed: {e}")

def _env_sub_key(n: int, kind: str) -> Tuple[Optional[str], Optional[str]]:
    """
    kind in {"READ","TRADE"}
    Returns (key, secret) or (None, None) if unset.
    """
    k = os.getenv(f"BYBIT_SUB{n}_{kind}_KEY", "")
    s = os.getenv(f"BYBIT_SUB{n}_{kind}_SECRET", "")
    if _has(k) and _has(s):
        return k, s
    return None, None

def _check_one_sub(n: int):
    rk, rs = _env_sub_key(n, "READ")
    tk, ts = _env_sub_key(n, "TRADE")

    # If user prefers one key for both, allow READ-only provided and mirror it.
    if not (_has(tk) and _has(ts)) and (_has(rk) and _has(rs)):
        tk, ts = rk, rs

    # If still nothing, just skip this index quietly.
    if not ((_has(rk) and _has(rs)) or (_has(tk) and _has(ts))):
        print(f"• Sub{n}: not configured (skipped)")
        return

    # READ check
    try:
        bybit_get("/v5/account/wallet-balance", {"accountType": "UNIFIED"}, key=rk or tk, secret=rs or ts)
        print(f"   - READ: OK")
    except Exception as e:
        print(f"   - READ: FAIL → {e}")

    # TRADE check (harmless cancel-all)
    if _has(tk) and _has(ts):
        try:
            bybit_post("/v5/order/cancel-all", {"category": "linear", "symbol": MAJOR_SYMBOL}, key=tk, secret=ts)
            print(f"   - TRADE: OK (cancel-all)")
        except Exception as e:
            print(f"   - TRADE: FAIL → {e}")
    else:
        print(f"   - TRADE: SKIP (no trade keypair)")

def main():
    # Presence check for main + telegram
    required = {
        "BYBIT_MAIN_READ_KEY": KEY_READ,
        "BYBIT_MAIN_READ_SECRET": SEC_READ,
        "BYBIT_MAIN_TRADE_KEY": KEY_TRADE,
        "BYBIT_MAIN_TRADE_SECRET": SEC_TRADE,
        "TG_TOKEN_MAIN": TG_TOKEN_MAIN,
        "TG_CHAT_MAIN": TG_CHAT_MAIN,
    }
    missing = [k for k, v in required.items() if not _has(v)]
    if missing:
        _fail(f"Missing env keys: {', '.join(missing)}. Fill Flashback/.env")

    print("=== Flashback Permission & Connectivity Check ===")
    _check_main_read()
    _check_main_trade()
    _check_transfer_key()
    _check_telegram()

    print("\n--- Subaccount checks ---")
    any_configured = False
    for i in range(1, 11):
        rk, rs = _env_sub_key(i, "READ")
        tk, ts = _env_sub_key(i, "TRADE")
        if any([_has(rk), _has(rs), _has(tk), _has(ts)]):
            any_configured = True
            print(f"Sub{i}:")
            _check_one_sub(i)
    if not any_configured:
        print("No subaccounts configured yet. This is fine for main-only operation.")

    print("\nAll main checks completed. If subs show FAIL on TRADE, enable Contract Trading on that sub key.")
    print("You’re cleared for takeoff. ✈️")

if __name__ == "__main__":
    main()
