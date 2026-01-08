from __future__ import annotations

import uvicorn

from config.settings import get_settings


def main() -> None:
    settings = get_settings()
    server_cfg = settings.get("server", {})
    host = server_cfg.get("host", "127.0.0.1")
    port = int(server_cfg.get("port", 8000))
    reload = bool(server_cfg.get("reload", False))

    uvicorn.run("backend.app:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    main()
