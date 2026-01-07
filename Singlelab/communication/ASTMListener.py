import threading
import serial
from datetime import datetime

from core.normalized_result import NormalizedResult
from db.db_handler import DBHandler


class ASTMListener:
    def __init__(self, config, status_callback=None, logger=None, notifier=None):
        self.config = config
        self.status_callback = status_callback
        self.running = False
        self.thread = None
        self.ser = None
        self.logger = logger
        self.notifier = notifier
        self.db = DBHandler()
        self.machine_id = (
            config.get("machine_id")
            or config.get("MachineId")
            or config.get("id")
            or config.get("MachineName")
        )
        self.param_map = self._load_param_map()

    def start(self):
        if self.running:
            return
        self.running = True
        self.thread = threading.Thread(target=self.listen)
        self.thread.start()
        if self.status_callback:
            self.status_callback(self.machine_id, "Running")
        self._log_info(f"ASTM listener started on {self.config['comm_port']}")

    def stop(self):
        self.running = False
        if self.ser:
            self.ser.close()
        if self.status_callback:
            self.status_callback(self.machine_id, "Stopped")
        self._log_info("ASTM listener stopped")

    def listen(self):
        port = self.config["comm_port"]
        settings = self.config["settings"]  # e.g., "9600,N,8,1"

        try:
            baudrate, parity, databits, stopbits = settings.split(',')
            parity = serial.PARITY_NONE if parity == 'N' else (
                serial.PARITY_EVEN if parity == 'E' else serial.PARITY_ODD
            )
            stopbits = serial.STOPBITS_ONE if stopbits == '1' else serial.STOPBITS_TWO

            self.ser = serial.Serial(
                port=port,
                baudrate=int(baudrate),
                parity=parity,
                stopbits=stopbits,
                bytesize=int(databits),
                timeout=1
            )
            self._log_info(f"Listening on {port} with {settings}")

            buffer = ""
            while self.running:
                if self.ser.in_waiting > 0:
                    byte_data = self.ser.read(self.ser.in_waiting).decode(errors='ignore')
                    buffer += byte_data
                    while True:
                        split_result = self._split_message(buffer)
                        if not split_result:
                            break
                        raw_message, buffer = split_result
                        if raw_message.strip():
                            self._log_debug(f"Raw message: {raw_message}")
                            self.handle_astm(raw_message)

        except Exception as e:
            message = f"ASTM error on {port}: {e}"
            self._log_error(message)
            self._notify_error(message)
        finally:
            if self.ser:
                self.ser.close()

    def handle_astm(self, raw_data):
        self._log_info(f"Received data at {datetime.now()}")
        self._log_debug(raw_data)

        cleaned = (
            raw_data.replace("\x02", "")
            .replace("\x03", "")
            .replace("\x04", "")
            .replace("\x05", "")
        )
        segments = cleaned.splitlines()
        is_astm = any(seg.strip().startswith(("O|", "R|")) for seg in segments)
        if not is_astm:
            self._handle_plain_text(segments)
            return

        sample_id = ""
        for segment in segments:
            segment = segment.strip()
            if not segment:
                continue

            parts = segment.split("|")
            record_type = parts[0][:1] if parts else ""

            if record_type == "O" and len(parts) > 2:
                sample_id = parts[2].strip()
            elif record_type == "R" and len(parts) > 3:
                parameter_code = parts[1].strip()
                value = parts[3].strip()
                if sample_id and parameter_code and value:
                    mapped_code = self._map_code(parameter_code)
                    if not mapped_code:
                        self._log_info(f"Skipped unmapped code '{parameter_code}' for sample {sample_id}")
                        continue
                    normalized = NormalizedResult(
                        sample_id=sample_id,
                        parameter_code=mapped_code,
                        result=value,
                        machine_id=str(self.machine_id or ""),
                        status="Y",
                        updated_at=datetime.now(),
                    )
                    self.db.update_lab_result(normalized)
                    self._log_info(f"Updated DB: {sample_id} {parameter_code} = {value}")

    def _handle_plain_text(self, segments):
        sample_id = ""
        results = []

        for segment in segments:
            line = segment.strip()
            if not line:
                continue
            upper = line.upper()
            if upper.startswith("DATE") or upper.startswith("NO."):
                continue
            if upper.startswith("SAMPLEID"):
                sample_id = line.split(":", 1)[-1].strip()
                sample_id = sample_id.split()[0].strip("-")
                continue
            if upper.startswith("ID:"):
                sample_id = line.split(":", 1)[-1].strip()
                sample_id = sample_id.split()[0].strip("-")
                continue

            parts = line.split()
            if len(parts) < 2:
                continue
            code = parts[0].strip(":").strip()
            value = " ".join(parts[1:]).strip()
            if not code or not value:
                continue
            results.append((code, value))

        if not sample_id:
            self._log_error("Plain text message missing SAMPLEID/ID line.")
            return

        for code, value in results:
            mapped_code = self._map_code(code)
            if not mapped_code:
                self._log_info(f"Skipped unmapped code '{code}' for sample {sample_id}")
                continue
            normalized = NormalizedResult(
                sample_id=sample_id,
                parameter_code=mapped_code,
                result=value,
                machine_id=str(self.machine_id or ""),
                status="Y",
                updated_at=datetime.now(),
            )
            self.db.update_lab_result(normalized)
            self._log_info(f"Updated DB: {sample_id} {code} = {value}")

    def _split_message(self, buffer: str):
        terminators = ["\x04", "\x03"]
        indices = [(buffer.find(t), t) for t in terminators if buffer.find(t) != -1]
        if not indices:
            return None
        idx, term = min(indices, key=lambda item: item[0])
        raw_message = buffer[:idx]
        remaining = buffer[idx + len(term):]
        return raw_message, remaining

    def _log_info(self, message: str):
        prefix = f"[ASTM {self.machine_id}] {message}"
        if self.logger:
            self.logger.info(message)
        print(prefix)

    def _log_debug(self, message: str):
        if self.logger:
            self.logger.debug(message)

    def _log_error(self, message: str):
        prefix = f"[ASTM {self.machine_id}] ERROR: {message}"
        if self.logger:
            self.logger.error(message)
        print(prefix)

    def _notify_error(self, message: str):
        if self.notifier:
            self.notifier.notify_machine_error(self.machine_id, message)

    def _load_param_map(self):
        try:
            mapping = self.db.get_param_map(str(self.machine_id or ""))
            if mapping:
                self._log_info(f"Loaded {len(mapping)} parameter mappings for {self.machine_id}")
                return mapping
            self._log_info(f"No parameter mappings found for {self.machine_id}")

            machine_name = self.config.get("MachineName") or self.config.get("machine_name")
            if machine_name and str(machine_name) != str(self.machine_id):
                fallback = self.db.get_param_map(str(machine_name))
                if fallback:
                    self._log_info(
                        f"Loaded {len(fallback)} parameter mappings for {machine_name} (fallback)"
                    )
                    return fallback
                self._log_info(f"No parameter mappings found for {machine_name}")
            return {}
        except Exception as exc:
            self._log_error(f"Failed to load param map for {self.machine_id}: {exc}")
            return {}

    def _map_code(self, instrument_code: str) -> str:
        key = self._normalize_code(instrument_code)
        if not key:
            return ""
        return self.param_map.get(key) or ""

    def _normalize_code(self, instrument_code: str) -> str:
        if not instrument_code:
            return ""
        return self._extract_code(instrument_code).lower()

    def _extract_code(self, code_field: str) -> str:
        if not code_field:
            return ""
        parts = [part for part in code_field.split("^") if part]
        if not parts:
            return code_field.strip()
        for part in parts:
            if any(char.isalpha() for char in part):
                return part
        return parts[-1]


# Optional: testing
if __name__ == "__main__":
    dummy_config = {
        "id": "m1",
        "comm_port": "COM11",
        "settings": "9600,N,8,1",
        "protocol": "ASTM",
        "machine_id": 1,
    }

    def dummy_callback(machine_id, status):
        print(f"Machine {machine_id} status: {status}")

    listener = ASTMListener(dummy_config, dummy_callback)
    listener.start()
