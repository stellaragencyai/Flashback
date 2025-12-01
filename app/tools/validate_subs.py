# app/tools/validate_subs.py
from __future__ import annotations
from decimal import Decimal
from app.core.subs import load_subs, SubConfigError
from app.core.flashback_common import bybit_get, send_tg

CATEGORY = "linear"

def main():
    try:
        subs = load_subs()
    except SubConfigError as e:
        print(f"❌ {e}")
        return

    ok = 0
    for s in subs:
        label = s["label"]
        print(f"▶ Testing {label} (uid={s['uid']})")
        try:
            # Wallet read (UNIFIED)
            bal = bybit_get("/v5/account/wallet-balance",
                            {"accountType":"UNIFIED"},
                            key=s["api_key"], secret=s["api_secret"])
            # Orders read on linear USDT
            _ = bybit_get("/v5/order/realtime",
                          {"category": CATEGORY, "settleCoin": "USDT"},
                          key=s["api_key"], secret=s["api_secret"])
            # Telegram ping (if provided)
            if s["tg_token"] and s["tg_chat"]:
                # Reuse main send_tg by temporarily overriding env via direct call
                try:
                    import requests
                    requests.post(
                        f"https://api.telegram.org/bot{s['tg_token']}/sendMessage",
                        json={"chat_id": s["tg_chat"], "text": f"✅ {label}: API & TG ok"},
                        timeout=8
                    )
                except Exception:
                    pass
            ok += 1
            print(f"   ✔ {label} OK")
        except Exception as e:
            print(f"   ✖ {label} failed: {e}")

    print(f"\nDone. {ok}/{len(subs)} subaccounts validated.")

if __name__ == "__main__":
    main()
