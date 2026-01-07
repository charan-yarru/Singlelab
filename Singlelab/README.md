# LIMS Cluster — local run instructions

This repository contains a Python backend and a small React frontend for a single lab.

Overview
- Backend: FastAPI app in `backend/app.py`. It serves a static frontend when `backend/static` exists and exposes `/api/machines`, `/api/machine-status`, and `/api/machine-samples`.
- Frontend: A Vite + React scaffold in `frontend/`. Build the frontend to produce static files in `frontend/dist`, then copy those files into `backend/static/` (or configure your deployment/static hosting).

Quick start (Windows PowerShell)

1) Create a Python venv and install backend deps

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
pip install -r backend/requirements.txt
```

2) Start the backend now

```powershell
# from repository root
python -m uvicorn backend.app:app --host 127.0.0.1 --port 8000
```

3) Build the frontend and copy the files to backend static dir

```powershell
# requires Node.js/npm
cd frontend
npm install
npm run build
# copy output
Remove-Item -Recurse -Force ../backend/static/*; Copy-Item -Path .\dist\* -Destination ..\backend\static -Recurse
cd ..
```

4) Open http://127.0.0.1:8000/ in your browser. The page will fetch `/api/machines`, `/api/machine-status`, and `/api/machine-samples`.

Notes
- The backend connects to SQL Server using `config/settings.json`. Ensure the SQL Server ODBC driver is installed and credentials are correct.
- If your backend is behind a reverse proxy, the frontend uses `window.location.host` to connect to the correct host/port.

### Logging & Alerts
- Per-machine log files live under the directory configured via `logging.log_dir` inside `config/settings.json`. Each listener writes to `<log_dir>/<MACHINENAME>.log` with rotation controlled by `max_bytes`/`backup_count`.
- Email notifications are disabled by default. To enable them, set `alerts.enabled` to `true` and provide SMTP credentials plus recipient list (see the `alerts` section in `config/settings.json`). Critical listener errors will trigger these alerts.

If you want, I can:
- Run the backend here and simulate one NormalizedResult to show the UI updating (dry-run), or
- Flip to live DB mode and run one controlled update if you explicitly confirm.
