#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Flashback — Exit Profiles loader

Loads exit profile definitions from config/exit_profiles.yaml and computes
actual TP/SL prices for a given entry + R distance + side.
"""

import os
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Any, List, Tuple

import yaml

CFG_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "..", "config", "exit_profiles.yaml")
CFG_PATH = os.path.abspath(CFG_PATH)


class ExitProfiles:
    def __init__(self) -> None:
        self._profiles: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not os.path.exists(CFG_PATH):
            self._profiles = {}
            return
        with open(CFG_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        self._profiles = data.get("profiles", {})

    def get_profile(self, name: str) -> Dict[str, Any]:
        if not self._profiles:
            self._load()
        profile = self._profiles.get(name)
        if not profile:
            raise KeyError(f"Exit profile '{name}' not found")
        return profile

    def compute_ladder(
        self,
        profile_name: str,
        entry_price: Decimal,
        risk_per_unit: Decimal,
        side: str,  # "Buy" or "Sell"
        total_qty: Decimal,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        profile = self.get_profile(profile_name)
        tps_cfg = profile.get("tps", [])
        sl_cfg = profile.get("sl", {})

        tps: List[Dict[str, Any]] = []

        for tp in tps_cfg:
            rr = Decimal(str(tp["rr"]))
            size_pct = Decimal(str(tp["size_pct"]))
            qty = (total_qty * size_pct).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            if side == "Buy":
                price = (entry_price + rr * risk_per_unit).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            else:
                price = (entry_price - rr * risk_per_unit).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
            tps.append(
                {
                    "rr": float(rr),
                    "size_pct": float(size_pct),
                    "qty": str(qty),
                    "price": str(price),
                }
            )

        sl_rr = Decimal(str(sl_cfg.get("rr", -1.0)))
        if side == "Buy":
            sl_price = (entry_price + sl_rr * risk_per_unit).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)
        else:
            sl_price = (entry_price - sl_rr * risk_per_unit).quantize(Decimal("0.0001"), rounding=ROUND_DOWN)

        sl = {
            "rr": float(sl_rr),
            "price": str(sl_price),
        }
        return tps, sl


# singleton-ish helper
_profiles = ExitProfiles()


def get_profile(name: str) -> Dict[str, Any]:
    return _profiles.get_profile(name)


def compute_ladder(
    profile_name: str,
    entry_price: Decimal,
    risk_per_unit: Decimal,
    side: str,
    total_qty: Decimal,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    return _profiles.compute_ladder(profile_name, entry_price, risk_per_unit, side, total_qty)
