import logging
from pathlib import Path

LOG_DIR = Path("app/logs")
LOG_DIR.mkdir(exist_ok=True, parents=True)

def get_logger(name: str):
    log_path = LOG_DIR / f"{name}.log"
    handler = logging.FileHandler(log_path, encoding="utf-8")
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger
