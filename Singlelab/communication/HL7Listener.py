import datetime
import socket
import threading
from db.db_handler import DBHandler
from core.normalized_result import NormalizedResult


class HL7Listener:
    def __init__(self, config, status_callback=None, logger=None, notifier=None):
        self.config = config
        self.db = DBHandler()
        self.machine_id = config["machine_id"]
        self.ip = config["Settings"]
        self.port = int(config["CommPort"])
        self.buffer_size = 4096
        self.running = False
        self.thread = None
        self.status_callback = status_callback
        self.logger = logger
        self.notifier = notifier
        self.param_map = self._load_param_map()

    def start(self):
        if self.thread and self.thread.is_alive():
            return  # Already running

        self.running = True
        self.thread = threading.Thread(target=self._listen, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join(timeout=2)

    def _listen(self):
        self._update_status("Running")
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                s.bind((self.ip, self.port))
                s.listen(1)
                self._log_info(f"HL7 listener ready on {self.ip}:{self.port}")

                while self.running:
                    conn, addr = s.accept()
                    self._log_info(f"Connection from {addr}")
                    with conn:
                        self._handle_connection(conn)

        except Exception as e:
            message = f"HL7 listener error on {self.ip}:{self.port} -> {e}"
            self._log_error(message)
            self._notify_error(message)
        finally:
            self._update_status("Stopped")

    def _process_hl7(self, message: str):
        lines = message.strip().split('\r')
        sample_id = None
        result_map = {}

        for line in lines:
            parts = line.split('|')
            if parts[0] == 'PID':
                sample_id = parts[3].strip()
            elif parts[0] == 'OBR':
                if not sample_id and len(parts) > 2:
                    sample_id = parts[2].strip() or sample_id
            elif parts[0] == 'OBX':
                if len(parts) > 5:
                    code = parts[3].strip()
                    value = parts[5].strip()
                    self._log_info(f"Parsed HL7 OBX: {code} = {value}")
                    result_map[code] = value

        if sample_id and result_map:
            for code, value in result_map.items():
                mapped = self._map_code(code)
                if not mapped:
                    self._log_info(f"Skipped unmapped code '{code}' for sample {sample_id}")
                    continue
                norm = NormalizedResult(
                    sample_id=sample_id,
                    parameter_code=mapped,
                    result=value,
                    status='Y',
                    machine_id=self.machine_id,
                    updated_at=datetime.datetime.now()
                )
                self.db.update_lab_result(norm)
                self._log_info(f"Updated DB: {sample_id} {code} = {value}")

    def _process_astm(self, message: str):
        cleaned = self._strip_controls(message)
        segments = cleaned.splitlines()
        sample_id = ""

        for segment in segments:
            segment = self._strip_frame_prefix(segment.strip())
            if not segment:
                continue
            parts = segment.split("|")
            record_type = parts[0][:1] if parts else ""

            if record_type == "O" and len(parts) > 2:
                sample_id = parts[2].strip()
            elif record_type == "R" and len(parts) > 3:
                code = parts[2].strip()
                value = parts[3].strip()
                if not (sample_id and code and value):
                    continue
                self._log_info(f"Parsed ASTM R: {sample_id} {code} = {value}")
                mapped = self._map_code(self._extract_code(code))
                if not mapped:
                    self._log_info(f"Skipped unmapped code '{code}' for sample {sample_id}")
                    continue
                norm = NormalizedResult(
                    sample_id=sample_id,
                    parameter_code=mapped,
                    result=value,
                    status='Y',
                    machine_id=self.machine_id,
                    updated_at=datetime.datetime.now()
                )
                self.db.update_lab_result(norm)
                self._log_info(f"Updated DB: {sample_id} {code} = {value}")

    def _handle_connection(self, conn):
        conn.settimeout(3.0)
        raw_bytes = bytearray()
        message_bytes = bytearray()
        astm_frames = []
        astm_mode = False
        in_frame = False
        expecting_checksum = 0
        frame_buf = bytearray()
        saw_enq = False

        while self.running:
            try:
                chunk = conn.recv(self.buffer_size)
            except socket.timeout:
                if astm_mode and astm_frames:
                    break
                continue

            if not chunk:
                break

            raw_bytes.extend(chunk)

            if not astm_mode and any(b in chunk for b in (0x02, 0x03, 0x04, 0x05, 0x17)):
                astm_mode = True

            if not astm_mode:
                message_bytes.extend(chunk)
                if b"\x1c" in chunk:
                    break
                continue

            for byte in chunk:
                if byte == 0x05:  # ENQ
                    saw_enq = True
                    self._send_astm_ack(conn)
                    continue

                if byte == 0x04:  # EOT
                    break

                if expecting_checksum:
                    expecting_checksum -= 1
                    if expecting_checksum == 0:
                        if frame_buf:
                            try:
                                astm_frames.append(frame_buf.decode("utf-8", errors="ignore"))
                            except Exception:
                                astm_frames.append(frame_buf.decode("latin1", errors="ignore"))
                            frame_buf = bytearray()
                        self._send_astm_ack(conn)
                    continue

                if byte in (0x0d, 0x0a):  # CR/LF
                    continue

                if byte == 0x02:  # STX
                    in_frame = True
                    frame_buf = bytearray()
                    continue

                if in_frame and byte in (0x03, 0x17):  # ETX/ETB
                    in_frame = False
                    expecting_checksum = 2
                    continue

                if in_frame:
                    frame_buf.append(byte)

        if raw_bytes:
            self._log_raw(self._decode_raw(raw_bytes))

        if astm_frames:
            message = "\r".join(astm_frames)
            self._log_debug(f"Received ASTM-over-TCP:\n{message}")
            self._process_astm(message)
            return

        if message_bytes:
            message = message_bytes.decode("utf-8", errors="ignore")
            self._log_raw(message)
            if self._looks_like_astm(message):
                self._log_debug(f"Received ASTM-over-TCP:\n{message}")
                self._process_astm(message)
            else:
                self._log_debug(f"Received HL7:\n{message}")
                self._process_hl7(message)
                self._send_ack(conn, message)

        if saw_enq and not astm_frames:
            self._log_info("ASTM session ended without frames.")

    def _send_ack(self, conn, original_message):
        msh_id = "UNKNOWN"
        for segment in original_message.strip().split('\r'):
            if segment.startswith("MSH"):
                fields = segment.split('|')
                if len(fields) > 9:
                    msh_id = fields[9]
        ack_msg = f"MSH|^~\\&|LIS|HOSPITAL|DEVICE|HIS|{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}||ACK^R01|{msh_id}|P|2.3\rMSA|AA|{msh_id}\x1c\r"
        conn.sendall(ack_msg.encode("utf-8"))
        self._log_info(f"Sent ACK for MSH.10={msh_id}")

    def _send_astm_ack(self, conn):
        try:
            conn.sendall(b"\x06")
        except Exception:
            pass

    def _update_status(self, status):
        if self.status_callback:
            self.status_callback(self.machine_id, status)

    def _log_info(self, message: str):
        if self.logger:
            self.logger.info(message)
        print(f"[{self.machine_id}] {message}")

    def _log_debug(self, message: str):
        if self.logger:
            self.logger.debug(message)

    def _log_error(self, message: str):
        if self.logger:
            self.logger.error(message)
        print(f"[{self.machine_id}] ERROR: {message}")

    def _log_raw(self, message: str):
        if self.logger:
            self.logger.info(f"RAW MESSAGE START\n{message}\nRAW MESSAGE END")

    def _notify_error(self, message: str):
        if self.notifier:
            self.notifier.notify_machine_error(self.machine_id, message)

    def _load_param_map(self):
        try:
            mapping = self.db.get_param_map(self.machine_id)
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

    def _looks_like_astm(self, message: str) -> bool:
        cleaned = self._strip_controls(message)
        for line in cleaned.splitlines():
            line = self._strip_frame_prefix(line.strip())
            if line.startswith(("H|", "P|", "O|", "R|", "L|", "C|", "M|")):
                return True
        return False

    def _strip_controls(self, message: str) -> str:
        return (
            message.replace("\x02", "")
            .replace("\x03", "")
            .replace("\x04", "")
            .replace("\x05", "")
            .replace("\x1c", "")
        )

    def _strip_frame_prefix(self, segment: str) -> str:
        while segment and segment[0].isdigit():
            segment = segment[1:]
        return segment

    def _extract_code(self, code_field: str) -> str:
        if not code_field:
            return ""
        parts = [part for part in code_field.split("^") if part]
        if not parts:
            return code_field.strip()
        # Prefer a short alpha code like WBC/RBC when present.
        for part in parts:
            if any(char.isalpha() for char in part):
                return part
        return parts[-1]

    def _decode_raw(self, data: bytes) -> str:
        try:
            return data.decode("utf-8", errors="ignore")
        except Exception:
            return data.decode("latin1", errors="ignore")
