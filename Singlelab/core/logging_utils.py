import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from threading import Lock

from config.settings import logging_config

_LOGGER_CACHE = {}
_LOCK = Lock()


def _ensure_log_dir(base_dir: Path) -> Path:
    try:
        base_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return base_dir


def get_machine_logger(machine_name: str) -> logging.Logger:
    """Return a logger that writes to a rotating file per machine."""
    key = machine_name or "machine"
    with _LOCK:
        if key in _LOGGER_CACHE:
            return _LOGGER_CACHE[key]

        level_str = (logging_config.get("level") or "INFO").upper()
        level = getattr(logging, level_str, logging.INFO)

        base_dir_setting = logging_config.get("log_dir") or "logs"
        base_dir = Path(base_dir_setting)
        if not base_dir.is_absolute():
            base_dir = Path(__file__).resolve().parents[1] / base_dir
        log_dir = _ensure_log_dir(base_dir)

        safe_name = key.replace(" ", "_")
        log_path = log_dir / f"{safe_name}.log"

        handler = RotatingFileHandler(
            log_path,
            maxBytes=int(logging_config.get("max_bytes", 1_048_576)),
            backupCount=int(logging_config.get("backup_count", 5)),
            encoding="utf-8",
        )
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(formatter)

        logger = logging.getLogger(f"machine.{safe_name}")
        logger.setLevel(level)
        logger.addHandler(handler)
        logger.propagate = False

        _LOGGER_CACHE[key] = logger
        return logger
