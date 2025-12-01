# app/ai/train_models.py
# Flashback — Setup Classifier Trainer v1.0
#
# Role:
# - Load trade-level feature rows from state/feature_store.jsonl
# - Build a supervised dataset:
#       X = features
#       y = 1 if pnl_r > 0 else 0
# - Train a simple classifier (Logistic Regression).
# - Save:
#       models/setup_classifier.pkl
#       models/setup_classifier_meta.json
#
# This is the first real AI artifact in Flashback.
# In v2 we can add:
#   - per-strategy models
#   - multi-class labels (e.g. strong_win / meh / loss)
#   - hyperparameter search

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

import orjson

try:
    from app.core.logger import get_logger
except Exception:
    import logging

    def get_logger(name: str) -> "logging.Logger":  # type: ignore
        logger_ = logging.getLogger(name)
        if not logger_.handlers:
            handler = logging.StreamHandler()
            fmt = logging.Formatter(
                "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
            )
            handler.setFormatter(fmt)
        logger_.addHandler(handler)
        logger_.setLevel(logging.INFO)
        return logger_


log = get_logger("train_models")

try:
    from app.core.config import settings
except Exception:
    class _DummySettings:  # type: ignore
        from pathlib import Path
        ROOT = Path(__file__).resolve().parents[2]
    settings = _DummySettings()  # type: ignore

ROOT: Path = settings.ROOT
STATE_DIR: Path = ROOT / "state"
MODELS_DIR: Path = ROOT / "models"

STATE_DIR.mkdir(parents=True, exist_ok=True)
MODELS_DIR.mkdir(parents=True, exist_ok=True)

FEATURE_STORE_PATH: Path = STATE_DIR / "feature_store.jsonl"
META_PATH: Path = MODELS_DIR / "setup_classifier_meta.json"
MODEL_PATH: Path = MODELS_DIR / "setup_classifier.pkl"


# ---------------------------------------------------------------------------
# Load dataset from feature_store.jsonl
# ---------------------------------------------------------------------------

def _load_feature_rows() -> List[Dict[str, Any]]:
    if not FEATURE_STORE_PATH.exists():
        log.error("feature_store.jsonl not found at %s", FEATURE_STORE_PATH)
        return []

    rows: List[Dict[str, Any]] = []
    with FEATURE_STORE_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception as e:
                log.warning("Invalid JSON in feature_store: %r", e)
                continue
            rows.append(row)

    log.info("Loaded %d feature rows from feature_store.", len(rows))
    return rows


def _session_to_int(session: str) -> int:
    s = (session or "").upper()
    if s == "ASIA":
        return 0
    if s == "LONDON":
        return 1
    if s == "NEW_YORK":
        return 2
    if s == "POST":
        return 3
    return -1


def build_dataset(rows: List[Dict[str, Any]]) -> Tuple[List[List[float]], List[int], Dict[str, Any]]:
    """
    Convert feature rows into X (list of feature vectors), y (labels),
    and some info about dataset composition.
    """
    X: List[List[float]] = []
    y: List[int] = []

    # Feature field names we rely on from feature_builder
    feature_names = [
        "side_sign",
        "atr_like",
        "atr_pct",
        "range_mean",
        "range_std",
        "volume_zscore",
        "trend_dir",
        "trend_strength",
        "entry_hour",
        "entry_dow",
    ]

    # Session encoded as int
    extra_feature_names = ["session_int"]

    total = 0
    discarded = 0

    for row in rows:
        total += 1
        try:
            pnl_r = float(row.get("pnl_r", 0.0))
        except Exception:
            pnl_r = 0.0

        # label: 1 = profitable trade (R > 0), 0 otherwise
        label = 1 if pnl_r > 0 else 0

        try:
            fv: List[float] = []
            for name in feature_names:
                val = row.get(name)
                fv.append(float(val) if val is not None else 0.0)

            session = str(row.get("session", ""))
            session_int = _session_to_int(session)
            fv.append(float(session_int))

        except Exception as e:
            discarded += 1
            continue

        X.append(fv)
        y.append(label)

    info = {
        "total_rows": total,
        "used_rows": len(X),
        "discarded_rows": discarded,
        "feature_names": feature_names + extra_feature_names,
    }
    return X, y, info


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------

def train_model(X: List[List[float]], y: List[int]) -> Dict[str, Any]:
    """
    Train a simple classifier (Logistic Regression) and return
    model + metrics + metadata dict.
    """
    if not X or not y or len(X) != len(y):
        raise ValueError("Empty or mismatched dataset.")

    # Import here so the script doesn't crash on import if sklearn missing
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import accuracy_score, roc_auc_score
    from sklearn.model_selection import train_test_split
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler

    X_train, X_test, y_train, y_test = train_test_split(
        X,
        y,
        test_size=0.2,
        random_state=42,
        stratify=y if len(set(y)) > 1 else None,
    )

    pipeline = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("clf", LogisticRegression(max_iter=200, n_jobs=None)),
        ]
    )

    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test) if X_test else []
    y_prob = (
        pipeline.predict_proba(X_test)[:, 1].tolist()
        if X_test and hasattr(pipeline, "predict_proba")
        else []
    )

    metrics: Dict[str, Any] = {}
    if y_test:
        metrics["accuracy"] = float(accuracy_score(y_test, y_pred))
        try:
            metrics["roc_auc"] = float(roc_auc_score(y_test, y_prob))
        except Exception:
            metrics["roc_auc"] = None
    else:
        metrics["accuracy"] = None
        metrics["roc_auc"] = None

    return {
        "model": pipeline,
        "metrics": metrics,
        "n_train": len(X_train),
        "n_test": len(X_test),
    }


def save_model_artifacts(
    model_obj: Any,
    meta: Dict[str, Any],
) -> None:
    """
    Persist model and metadata to disk.
    """
    # Save model via joblib
    import joblib

    joblib.dump(model_obj, MODEL_PATH)
    log.info("Saved model to %s", MODEL_PATH)

    META_PATH.write_text(json.dumps(meta, indent=2))
    log.info("Saved model metadata to %s", META_PATH)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    rows = _load_feature_rows()
    if not rows:
        log.error("No feature rows to train on. Run feature_builder first.")
        return

    X, y, info = build_dataset(rows)
    log.info(
        "Dataset built: total=%s used=%s discarded=%s",
        info["total_rows"],
        info["used_rows"],
        info["discarded_rows"],
    )

    if info["used_rows"] < 20:
        log.warning(
            "Very few samples (%s). Model quality will be garbage but we will still train.",
            info["used_rows"],
        )

    try:
        training_result = train_model(X, y)
    except Exception as e:
        log.exception("Training failed: %r", e)
        return

    model = training_result["model"]
    metrics = training_result["metrics"]

    meta = {
        "schema_version": 1,
        "feature_names": info["feature_names"],
        "dataset": {
            "total_rows": info["total_rows"],
            "used_rows": info["used_rows"],
            "discarded_rows": info["discarded_rows"],
        },
        "metrics": metrics,
    }

    log.info("Training metrics: %s", metrics)
    save_model_artifacts(model, meta)


if __name__ == "__main__":
    main()
