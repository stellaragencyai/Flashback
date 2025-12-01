import logging
import sys
from pathlib import Path

def get_logger(name: str) -> logging.Logger:
    logger_ = logging.getLogger(name)

    if not logger_.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] [%(name)s] %(message)s"
        )
        handler.setFormatter(formatter)
        logger_.addHandler(handler)

    logger_.setLevel(logging.DEBUG)
    logger_.propagate = False

    # Optional: catch uncaught exceptions and log them
    def handle_exception(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
        logger_.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))

    sys.excepthook = handle_exception
    return logger_
