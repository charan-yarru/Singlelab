from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from db.db_handler import DBHandler
from core.machine_manager import MachineManager

# Initialize FastAPI app
app = FastAPI()

# Allow all CORS (adjust in production)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # change this to frontend URL in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create DB handler and MachineManager
db = DBHandler()
mgr = MachineManager(db)
mgr.load_machines()

STATIC_DIR = Path(__file__).resolve().parent / "static"
ASSETS_DIR = STATIC_DIR / "assets"
LOGS_DIR = Path(__file__).resolve().parent.parent / "logs"

if ASSETS_DIR.exists():
    app.mount("/assets", StaticFiles(directory=ASSETS_DIR), name="assets")
LOGS_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/logs", StaticFiles(directory=LOGS_DIR), name="logs")

@app.on_event("startup")
async def startup_event():
    print("App starting up...")
    mgr.load_machines()  # Reload machines on startup

@app.get("/api/machines")
def get_machine_configs():
    print("API call: /api/machines")
    data = mgr.ensure_loaded()
    print(f"[API] /api/machines -> {len(data)} machines")
    return data

@app.get("/api/machine-status")
def get_machine_status():
    mgr.ensure_loaded()
    data = mgr.get_machine_status()
    print(f"[API] /api/machine-status -> {len(data)} entries")
    return data


@app.get("/api/machine-samples")
def get_machine_samples(limit: int = 3):
    """Return the latest sample IDs per machine."""
    if limit <= 0:
        raise HTTPException(status_code=400, detail="limit must be positive")
    limit = min(limit, 50)

    mgr.ensure_loaded()
    samples = db.get_recent_samples(limit)
    name_map = mgr.get_machine_name_map()

    response = {}
    for machine_id, entries in samples.items():
        target_name = name_map.get(machine_id, machine_id)
        response.setdefault(target_name, [])
        for entry in entries:
            updated_at = entry.get("updated_at")
            if hasattr(updated_at, "isoformat"):
                updated_value = updated_at.isoformat()
            elif updated_at is not None:
                updated_value = str(updated_at)
            else:
                updated_value = None

            response[target_name].append(
                {
                    "sample_id": entry.get("sample_id"),
                    "updated_at": updated_value,
                }
            )

    print(f"[API] /api/machine-samples -> {sum(len(v) for v in response.values())} rows")
    return response




@app.post("/api/machines/{machine_name}/start")
def start_machine(machine_name: str):
    result = mgr.start_machine(machine_name)
    print(f"[API] start {machine_name} -> {result}")
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("message", "Unable to start"))
    return result


@app.post("/api/machines/{machine_name}/stop")
def stop_machine(machine_name: str):
    result = mgr.stop_machine(machine_name)
    print(f"[API] stop {machine_name} -> {result}")
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("message", "Unable to stop"))
    return result


@app.get("/", include_in_schema=False)
def serve_frontend():
    """Serve the compiled frontend index.html."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        print("[API] Serving frontend index.html")
        return FileResponse(index_path)
    return {"detail": "Frontend build not found", "path": str(index_path)}


@app.get("/logs/{machine_name}", include_in_schema=False)
def serve_machine_log(machine_name: str):
    """
    Serve a machine-specific log file (expects <machine_name>.log in logs/).
    """
    base_name = (machine_name or "").strip()
    if base_name.lower().endswith(".log"):
        base_name = base_name[:-4]
    safe_name = base_name.replace(" ", "_")
    filename = f"{safe_name}.log"
    log_path = LOGS_DIR / filename
    if not log_path.exists():
        raise HTTPException(status_code=404, detail="Log not found")
    return FileResponse(log_path, media_type="text/plain")
