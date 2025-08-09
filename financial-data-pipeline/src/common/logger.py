import logging
import os
import sys
from contextlib import contextmanager
from time import perf_counter

def get_logger(name: str = "app", level: str = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    ch = logging.StreamHandler(stream=sys.stdout)

    # Fail-safe formatter: guarantees run_id even if the filter is not applied.
    class SafeRunIdFormatter(logging.Formatter):
        def format(self, record):
            if not hasattr(record, "run_id"):
                record.run_id = os.getenv("RUN_ID", "local")
            return super().format(record)

    fmt = SafeRunIdFormatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | run_id=%(run_id)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    logger.setLevel(level or os.getenv("LOG_LEVEL", "INFO"))

    # Filter that adds run_id (remains as backup)

    def add_run_id(record):
        if not hasattr(record, "run_id"):
            record.run_id = os.getenv("RUN_ID", "local")
        return True

    logger.addFilter(add_run_id)
    return logger

@contextmanager
def timed(logger: logging.Logger, msg: str):
    t0 = perf_counter()
    try:
        yield
    finally:
        dt = perf_counter() - t0
        logger.info(f"{msg} | elapsed={dt:.3f}s")
