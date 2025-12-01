# Requires venv activated in this shell before launch
Start-Job { python -m app.bots.tp_sl_manager }
Start-Job { python -m app.bots.risk_guardian }
Start-Job { python -m app.bots.trade_journal }
Start-Job { python -m app.bots.tier_enforcer }
Start-Job { python -m app.bots.volatility_scout }
# Profit sweeper is scheduled daily; optional to run in background too:
# Start-Job { python -m app.bots.profit_sweeper }
# Drip bot only if you want continuous profit splitting:
# Start-Job { python -m app.bots.equity_drip_bot }
Get-Job
