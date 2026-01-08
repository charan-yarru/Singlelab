import threading
from typing import Any, Dict

from communication.unified_listener import UnifiedListener
from core.logging_utils import get_machine_logger
from core.notifier import EmailNotifier


class MachineManager:
    def __init__(self, db_handler):
        self.db = db_handler
        self.machines: Dict[str, Dict[str, str]] = {}          # Simplified configs for UI
        self.machine_records: Dict[str, Dict[str, Any]] = {}   # Raw DB rows for listeners
        self.active_listeners: Dict[str, Any] = {}             # Running HL7/Serial listeners
        self.listener_status: Dict[str, str] = {}              # Human-readable state per machine
        self._lock = threading.Lock()
        self.notifier = EmailNotifier()

    # ------------------------------------------------------------------
    def load_machines(self):
        """Fetch machine rows from the database and normalize the payload."""
        rows = self.db.get_machines()
        formatted: Dict[str, Dict[str, str]] = {}
        records: Dict[str, Dict[str, Any]] = {}
        seen_names = set()

        for row in rows:
            name = (row.get("MachineName") or "").strip()
            comm = row.get("CommPort") or ""
            settings = row.get("Settings") or ""

            if not name:
                continue

            comm_str = str(comm)
        protocol_display = self._display_protocol_type(comm_str, settings)
        protocol_value = self._build_protocol_value(protocol_display)
            port_display = self._build_port_display(protocol_display, comm_str, settings)
            formatted[name] = {
                "name": name,
                "protocol": protocol_value,
                "protocol_type": protocol_display,
                "port_display": port_display,
            }

            records[name] = dict(row)
            seen_names.add(name)

        listeners_to_stop = []
        with self._lock:
            removed = set(self.machines.keys()) - seen_names
            for name in removed:
                listener = self.active_listeners.pop(name, None)
                if listener:
                    listeners_to_stop.append(listener)
                self.listener_status.pop(name, None)

            for name in formatted:
                self.listener_status.setdefault(name, "Stopped")

            self.machines = formatted
            self.machine_records = records

        for listener in listeners_to_stop:
            try:
                listener.stop()
            except Exception:
                pass

        print(f"[MachineManager] Loaded {len(formatted)} machines")
        return formatted

    # ------------------------------------------------------------------
    def _display_protocol_type(self, comm: str, settings: str) -> str:
        comm_upper = (comm or "").upper()
        settings_str = (settings or "").strip()
        if settings_str and "," in settings_str:
            return "Serial"
        if comm_upper.startswith("COM"):
            return "Serial"
        return "TCP/IP"

    # ------------------------------------------------------------------
    def _build_protocol_value(self, protocol_display: str) -> str:
        if protocol_display == "Serial":
            return "AUTO-Serial"
        return "AUTO-TCP"

    # ------------------------------------------------------------------
    def _build_port_display(self, protocol_display: str, comm: str, settings: str) -> str:
        comm = (comm or "").strip()
        settings = (settings or "").strip()
        if protocol_display == "Serial":
            port = self._format_serial_port(comm)
            if comm and settings:
                return f"COM: {port or comm} — {settings}"
            if comm:
                return f"COM: {port or comm}"
            return f"Serial settings: {settings}"
        # TCP/IP details
        if comm and settings:
            return f"IP: {settings} Port: {comm}"
        if settings:
            return f"IP: {settings}"
        return f"Port: {comm}"

    # ------------------------------------------------------------------
    def get_machine_configs(self):
        """Return loaded machine configurations."""
        return list(self.machines.values())

    # ------------------------------------------------------------------
    def get_machine_status(self):
        """Return running/stopped status for each machine."""
        with self._lock:
            return {name: self.listener_status.get(name, "Stopped") for name in self.machines.keys()}

    # ------------------------------------------------------------------
    def ensure_loaded(self):
        """Reload machine list if the cache is empty."""
        with self._lock:
            has_data = bool(self.machines)
        if not has_data:
            self.load_machines()
        return list(self.machines.values())

    # ------------------------------------------------------------------
    def get_machine_name_map(self):
        """Map machine IDs (from DB rows) back to their configured names."""
        mapping: Dict[str, str] = {}
        with self._lock:
            for name, record in self.machine_records.items():
                machine_id = record.get("MachineId") or record.get("machine_id") or name
                if not machine_id:
                    continue
                mapping[str(machine_id)] = name
        return mapping

    # ------------------------------------------------------------------
    def start_machine(self, name):
        with self._lock:
            record = self.machine_records.get(name)
            if not record:
                return {"success": False, "message": f"{name} not configured", "state": "Unknown"}

            if name in self.active_listeners:
                return {"success": True, "message": f"{name} already running", "state": self.listener_status.get(name, "Running")}

            config = self._prepare_listener_config(name, record)
            transport = self._resolve_transport(record, str(config.get("CommPort") or ""))
            config["transport"] = transport
            if transport == "Serial":
                config["comm_port"] = self._format_serial_port(config.get("CommPort") or config.get("comm_port"))
            else:
                config["comm_port"] = str(config.get("CommPort") or config.get("comm_port") or "")
            listener = self._create_listener(config, name)

            self.listener_status[name] = "Starting"
            self.active_listeners[name] = listener

        try:
            listener.start()
        except Exception as exc:
            with self._lock:
                self.active_listeners.pop(name, None)
                self.listener_status[name] = "Stopped"
            return {"success": False, "message": f"Failed to start {name}: {exc}", "state": "Stopped"}

        return {"success": True, "message": f"{name} starting", "state": self.listener_status.get(name, "Starting")}

    # ------------------------------------------------------------------
    def stop_machine(self, name):
        with self._lock:
            listener = self.active_listeners.get(name)
            if not listener:
                return {"success": False, "message": f"{name} not running", "state": self.listener_status.get(name, "Stopped")}

        success = False
        try:
            listener.stop()
            success = True
            message = f"{name} stopped"
        except Exception as exc:
            success = False
            message = f"Failed to stop {name}: {exc}"
        finally:
            with self._lock:
                self.active_listeners.pop(name, None)
                self.listener_status[name] = "Stopped"

        return {"success": success, "message": message, "state": "Stopped"}

    # ------------------------------------------------------------------
    def is_machine_running(self, name):
        with self._lock:
            return name in self.active_listeners

    # ------------------------------------------------------------------
    def get_all_status(self):
        return self.get_machine_status()

    # ------------------------------------------------------------------
    def _resolve_protocol(self, record: Dict[str, Any], comm_str: str) -> str:
        protocol_hint = (record.get("Protocol") or record.get("protocol") or "").strip().upper()
        if protocol_hint.startswith("ASTM"):
            return "ASTM"
        if protocol_hint.startswith("HL7"):
            return "HL7"

        settings = str(record.get("Settings") or "")
        if settings and "," in settings:
            return "ASTM"

        settings_is_ip = self._looks_like_ip(settings)
        if settings_is_ip:
            return "HL7"

        comm_upper = (comm_str or "").upper()
        if comm_upper.startswith("COM"):
            return "ASTM"

        # Default to HL7 (TCP/IP)
        return "HL7"

    # ------------------------------------------------------------------
    def _resolve_transport(self, record: Dict[str, Any], comm_str: str) -> str:
        protocol_display = self._display_protocol_type(comm_str, str(record.get("Settings") or ""))
        return "Serial" if protocol_display == "Serial" else "TCP"

    # ------------------------------------------------------------------
    def _prepare_listener_config(self, name: str, record: Dict[str, Any]) -> Dict[str, Any]:
        config = dict(record)
        machine_id = record.get("MachineId") or record.get("machine_id") or name
        comm_port = record.get("CommPort") or record.get("comm_port")
        settings = record.get("Settings") or record.get("settings") or ""

        config.update(
            {
                "MachineName": name,
                "machine_id": machine_id,
                "CommPort": comm_port,
                "Settings": settings,
                "id": machine_id,
                "comm_port": comm_port,
                "settings": settings,
            }
        )
        return config

    # ------------------------------------------------------------------
    def _create_listener(self, config: Dict[str, Any], machine_name: str):
        def status_callback(_machine_id, status):
            self._listener_status_callback(machine_name, status)

        logger = get_machine_logger(machine_name)

        return UnifiedListener(
            config,
            status_callback=status_callback,
            logger=logger,
            notifier=self.notifier,
        )

    # ------------------------------------------------------------------
    def _listener_status_callback(self, name: str, status: str):
        with self._lock:
            self.listener_status[name] = status or "Stopped"
            if status == "Stopped":
                self.active_listeners.pop(name, None)

    # ------------------------------------------------------------------
    def _looks_like_ip(self, value: str) -> bool:
        parts = value.split(".")
        if len(parts) != 4:
            return False
        try:
            return all(0 <= int(part) <= 255 for part in parts)
        except ValueError:
            return False

    # ------------------------------------------------------------------
    def _format_serial_port(self, value: str) -> str:
        port = (value or "").strip()
        if not port:
            return port
        if port.upper().startswith("COM"):
            return port.upper()
        return f"COM{port}"
