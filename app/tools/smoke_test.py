from app.core.flashback_common import get_equity_usdt, send_tg

if __name__ == "__main__":
    eq = get_equity_usdt()
    print("Equity (USDT):", eq)
    send_tg(f"Flashback smoke test OK. Equity: ${eq}", main=True)
