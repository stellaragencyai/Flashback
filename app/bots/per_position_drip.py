# app/bots/per_position_drip.py
# Flashback — Per-Position Drip (Main)
# - Polls latest closed PnL entries.
# - For each NEW profitable close, transfers DRIP_PCT of realized PnL to the next sub (round-robin).
# - Minimum transfer DRIP_MIN_USD and respects MAIN_BAL_FLOOR_USD.

import time
from decimal import Decimal
from pathlib import Path
from typing import Optional, Dict, Any
import orjson

from app.core.flashback_common import (
    bybit_get, send_tg, inter_transfer_usdt_to_sub,
    DRIP_PCT, DRIP_MIN_USD, MAIN_BAL_FLOOR_USD, get_equity_usdt
)
from app.core.subs import rr_next, peek_current

STATE_PATH = Path("app/state/drip_state.json")

def _load_state() -> dict:
    try:
        if STATE_PATH.exists():
            return orjson.loads(STATE_PATH.read_bytes())
    except Exception:
        pass
    return {"last_row_id": None}

def _save_state(st: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_bytes(orjson.dumps(st))

def _latest_closed(limit: int = 3) -> list:
    r = bybit_get("/v5/position/closed-pnl", {"category": "linear", "limit": str(limit)})
    return r.get("result", {}).get("list", []) or []

def _row_id(row: Dict[str, Any]) -> str:
    # Construct a unique-ish id from execTime and symbol
    return f"{row.get('symbol','?')}:{row.get('execTime',row.get('updatedTime','0'))}"

def loop():
    st = _load_state()
    send_tg("💧 Per-Position Drip started.")
    while True:
        try:
            rows = _latest_closed(limit=5)
            for row in rows:
                rid = _row_id(row)
                if st.get("last_row_id") == rid:
                    break  # nothing newer
                # process in reverse order: oldest first
            rows.reverse()

            for row in rows:
                rid = _row_id(row)
                if st.get("last_row_id") == rid:
                    continue

                pnl = Decimal(str(row.get("closedPnl", "0")))
                sym = row.get("symbol", "?")
                if pnl > 0:
                    eq = get_equity_usdt()
                    if eq < MAIN_BAL_FLOOR_USD:
                        send_tg(f"🟨 Drip skipped (equity {eq} below floor {MAIN_BAL_FLOOR_USD}).")
                    else:
                        amt = (pnl * DRIP_PCT).quantize(Decimal("0.01"))
                        if amt >= DRIP_MIN_USD:
                            sub = rr_next()
                            if sub:
                                try:
                                    inter_transfer_usdt_to_sub(sub["uid"], amt)
                                    send_tg(f"✅ Drip: {sym} profit ${pnl:.2f} → sent ${amt} to {sub.get('label','sub')} ({sub['uid']}).")
                                except Exception as e:
                                    send_tg(f"⚠️ Drip transfer failed to {sub.get('label','?')} ({sub['uid']}): {e}")
                        else:
                            send_tg(f"ℹ️ Drip too small (${amt}); min is ${DRIP_MIN_USD}.")
                st["last_row_id"] = rid
                _save_state(st)

            time.sleep(6)
        except Exception as e:
            send_tg(f"[Drip] {e}")
            time.sleep(8)

if __name__ == "__main__":
    loop()
