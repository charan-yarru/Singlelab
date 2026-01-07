import json
import os
from typing import Dict


def load_settings() -> Dict:
    path = os.path.join(os.path.dirname(__file__), "settings.json")
    try:
        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        return {}


_SETTINGS = load_settings()


def get_settings() -> Dict:
    """Return a copy of the application settings."""
    return dict(_SETTINGS)


# Backwards-compatible exports
db_config = _SETTINGS.get("db") or _SETTINGS
logging_config = _SETTINGS.get("logging", {})
alert_config = _SETTINGS.get("alerts", {})
