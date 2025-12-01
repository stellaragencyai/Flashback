#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Config Sanity Checker

Run with:
    python -m tools.config_sanity

Checks:
  - strategies.yaml formatting
  - risk_profile names exist in risk_profiles.yaml
  - exit_profile names exist in exit_profiles.yaml
  - sub_uid uniqueness
"""

from pathlib import Path
from typing import Dict, Any, Set

import yaml

try:
    from app.core.config import settings
except ImportError:
    from core.config import settings  # type: ignore

ROOT: Path = getattr(settings, "ROOT", Path(__file__).resolve().parents[2])

STRAT_CFG = ROOT / "config" / "strategies.yaml"
RISK_CFG  = ROOT / "config" / "risk_profiles.yaml"
EXIT_CFG  = ROOT / "config" / "exit_profiles.yaml"


def _load_yaml(path: Path) -> Any:
    if not path.exists():
        raise SystemExit(f"Missing config: {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def main() -> None:
    strat = _load_yaml(STRAT_CFG)
    risk  = _load_yaml(RISK_CFG)
    exitp = _load_yaml(EXIT_CFG)

    profiles_risk = set((risk.get("profiles") or {}).keys())
    profiles_exit = set((exitp.get("profiles") or {}).keys())

    entries = strat if isinstance(strat, list) else strat.get("strategies") or strat

    seen_uids: Set[str] = set()
    errors = 0

    for idx, s in enumerate(entries):
        sub_uid = str(s.get("sub_uid", "")).strip()
        name = s.get("name", f"idx={idx}")
        risk_name = str(s.get("risk_profile", "")).upper()
        exit_name = str(s.get("exit_profile", "")).upper()

        if not sub_uid:
            print(f"[ERR] Strategy {name}: missing sub_uid")
            errors += 1
        else:
            if sub_uid in seen_uids:
                print(f"[ERR] Duplicate sub_uid {sub_uid} (strategy {name})")
                errors += 1
            seen_uids.add(sub_uid)

        if risk_name and risk_name not in profiles_risk:
            print(f"[ERR] Strategy {name}: unknown risk_profile={risk_name}")
            errors += 1

        if exit_name and exit_name not in profiles_exit:
            print(f"[ERR] Strategy {name}: unknown exit_profile={exit_name}")
            errors += 1

    if errors == 0:
        print("[OK] All configs passed sanity checks.")
    else:
        print(f"[FAIL] {errors} config issues found.")


if __name__ == "__main__":
    main()
