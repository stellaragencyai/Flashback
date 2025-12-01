import sys, subprocess

BOTS = {
    "tp":        "app.bots.tp_sl_manager",
    "risk":      "app.bots.risk_guardian",
    "journal":   "app.bots.trade_journal",
    "tier":      "app.bots.tier_enforcer",
    "sweeper":   "app.bots.profit_sweeper",
    "scout":     "app.bots.volatility_scout",
    "drip":      "app.bots.equity_drip_bot",
}

if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] not in BOTS:
        print("Usage: python run_bot.py [tp|risk|journal|tier|sweeper|scout|drip]")
        sys.exit(1)
    mod = BOTS[sys.argv[1]]
    sys.exit(subprocess.call([sys.executable, "-m", mod]))
