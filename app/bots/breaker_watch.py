# Flashback — Breaker Watch
# If app/state/breaker_on.flag exists, repeatedly remind and set GLOBAL_BREAKER['on']=True.

import time
from pathlib import Path
from app.core.flashback_common import GLOBAL_BREAKER, send_tg

FLAG = Path("app/state/breaker_on.flag")

def loop():
    send_tg("🧯 Breaker Watch online.")
    last_state = None
    while True:
        on = FLAG.exists()
        GLOBAL_BREAKER["on"] = on
        if on != last_state:
            if on:
                send_tg("🟥 GLOBAL BREAKER: ON (no entries; managers should only heartbeat)")
            else:
                send_tg("🟩 GLOBAL BREAKER: OFF (normal operations allowed)")
            last_state = on
        time.sleep(2)

if __name__ == "__main__":
    loop()
