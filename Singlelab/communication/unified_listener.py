import datetime
import socket
import threading
from typing import Any, Dict, List, Optional

import serial

from communication import parser
from db.db_handler import DBHandler


class UnifiedListener:
    """
    Unified listener that reads from TCP or Serial, detects HL7 vs ASTM,
    and routes to the correct parser.
    """

    def __init__(self, config: Dict[str, Any], status_callback=None, logger=None, notifier=None):
        self.config = config
        self.db = DBHandler()
        self.machine_id = (
            config.get("machine_id")
            or config.get("MachineId")
            or config.get("id")
            or config.get("MachineName")
        )
        self.transport = (config.get("transport") or "").lower()
        self.ip = str(config.get("Settings") or "")
        self.port = int(config.get("CommPort") or 0) if config.get("CommPort") else 0
        self.comm_port = str(config.get("comm_port") or config.get("CommPort") or "")
        self.settings = str(config.get("settings") or config.get("Settings") or "")
        self.buffer_size = 4096
        self.running = False
        self.thread = None
        self.status_callback = status_callback
        self.logger = logger
        self.notifier = notifier
        self.param_map = self._load_param_map()
        self._serial = None

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.running = True
        self.thread = threading.Thread(target=self._listen, daemon=True)
        self.thread.start()

    def stop(self):
        self.running = False
        if self._serial:
            try:
                self._serial.close()
            except Exception:
                pass
        if self.thread:
            self.thread.join(timeout=2)

    def _listen(self):
        self._update_status("Running")
        try:
            if self.transport == "serial":
                self._listen_serial()
            else:
                self._listen_tcp()
        except Exception as exc:
            message = f"Unified listener error -> {exc}"
            self._log_error(message)
            self._notify_error(message)
        finally:
            self._update_status("Stopped")

    def _listen_tcp(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.ip, self.port))
            sock.listen(1)
            self._log_info(f"Unified TCP listener ready on {self.ip}:{self.port}")

            while self.running:
                conn, addr = sock.accept()
                self._log_info(f"Connection from {addr}")
                with conn:
                    self._handle_connection(conn)

    def _listen_serial(self):
        if not self.comm_port:
            raise ValueError("Serial port not configured")
        if "," not in self.settings:
            raise ValueError("Serial settings not configured (expected '9600,N,8,1')")

        baudrate, parity, databits, stopbits = self.settings.split(",")
        parity = serial.PARITY_NONE if parity == "N" else (
            serial.PARITY_EVEN if parity == "E" else serial.PARITY_ODD
        )
        stopbits = serial.STOPBITS_ONE if stopbits == "1" else serial.STOPBITS_TWO

        self._serial = serial.Serial(
            port=self.comm_port,
            baudrate=int(baudrate),
            parity=parity,
            stopbits=stopbits,
            bytesize=int(databits),
            timeout=1,
        )
        self._log_info(f"Unified serial listener on {self.comm_port} with {self.settings}")

        buffer = ""
        while self.running:
            if self._serial.in_waiting > 0:
                chunk = self._serial.read(self._serial.in_waiting)
                for byte in chunk:
                    if byte == 0x05:  # ENQ
                        self._log_info("ASTM ENQ received")
                        self._send_serial_ack()
                        continue
                    if byte == 0x04:  # EOT
                        self._log_info("ASTM EOT received")
                        continue
                    buffer += chr(byte)

                # HL7 over serial (MLLP-style terminator)
                if "\x1c" in buffer:
                    parts = buffer.split("\x1c")
                    buffer = parts[-1]
                    for part in parts[:-1]:
                        message = part.strip("\x0b\r\n")
                        if message:
                            self._log_raw(message)
                            self._process_message(message, source="serial")
                    continue

                # ASTM framing (ETX/EOT terminators)
                while True:
                    split_result = self._split_message(buffer)
                    if not split_result:
                        break
                    raw_message, buffer = split_result
                    if raw_message.strip():
                        self._log_raw(raw_message)
                        self._process_message(raw_message, source="serial")
                        self._send_serial_ack()

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
            self._process_message(message, source="tcp")
            return

        if message_bytes:
            message = message_bytes.decode("utf-8", errors="ignore")
            self._log_raw(message)
            self._log_debug(f"Received TCP:\n{message}")
            protocol = parser.detect_protocol(message.encode("utf-8", errors="ignore"))
            self._process_message(message, source="tcp")
            if protocol == "HL7":
                self._send_hl7_ack(conn, message)

        if saw_enq and not astm_frames:
            self._log_info("ASTM session ended without frames.")

    def _process_message(self, message: str, source: str):
        raw_bytes = message.encode("utf-8", errors="ignore")
        protocol = parser.detect_protocol(raw_bytes)

        if protocol == "ASTM":
            sample_id = self._extract_astm_query_sample_id(message)
            if sample_id and source == "serial":
                self._handle_astm_query(sample_id)
                return

        results = parser.parse_message(raw_bytes, str(self.machine_id or ""), self.param_map)

        for result in results:
            self.db.update_lab_result(result)
            self._log_info(f"Updated DB: {result.sample_id} {result.parameter_code} = {result.result}")

        if protocol == "HL7" and source == "serial" and self._serial:
            try:
                ack_msg = self._build_hl7_ack(message)
                self._serial.write(ack_msg.encode("utf-8"))
                self._log_info("Sent HL7 ACK over serial")
            except Exception:
                pass

    def _send_hl7_ack(self, conn, original_message: str):
        ack_msg = self._build_hl7_ack(original_message)
        conn.sendall(ack_msg.encode("utf-8"))
        self._log_info("Sent HL7 ACK")

    def _build_hl7_ack(self, original_message: str) -> str:
        msh_id = "UNKNOWN"
        for segment in original_message.strip().split("\r"):
            if segment.startswith("MSH"):
                fields = segment.split("|")
                if len(fields) > 9:
                    msh_id = fields[9]
        return (
            f"MSH|^~\\&|LIS|HOSPITAL|DEVICE|HIS|"
            f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}||ACK^R01|"
            f"{msh_id}|P|2.3\rMSA|AA|{msh_id}\x1c\r"
        )

    def _send_astm_ack(self, conn):
        try:
            conn.sendall(b"\x06")
        except Exception:
            pass

    def _send_serial_ack(self) -> None:
        if not self._serial:
            return
        try:
            self._serial.write(b"\x06")
        except Exception:
            pass

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

    def _extract_astm_query_sample_id(self, message: str) -> Optional[str]:
        cleaned = self._strip_controls(message)
        for line in cleaned.splitlines():
            line = self._strip_frame_prefix(line.strip())
            if not line.startswith("Q|"):
                continue
            fields = line.split("|")
            if len(fields) < 3:
                continue
            raw = fields[2]
            for part in raw.split("^"):
                part = part.strip()
                if part and part.isdigit():
                    return part
            return raw.strip() or None
        return None

    def _handle_astm_query(self, sample_id: str) -> None:
        tests = self.db.get_pending_tests(sample_id)
        instrument_tests = self._map_lis_to_instrument_tests(tests)
        self._log_info(
            f"ASTM query for {sample_id}: {len(instrument_tests)} tests"
        )
        if not self._serial:
            return

        response_text = self._build_astm_order_response(sample_id, instrument_tests)
        frames = self._build_astm_frames(response_text)
        try:
            for frame in frames:
                self._serial.write(frame)
            self._serial.write(b"\x04")
        except Exception as exc:
            self._log_error(f"Failed to send ASTM response: {exc}")

    def _map_lis_to_instrument_tests(self, lis_tests: List[str]) -> List[str]:
        pairs = self.db.get_machine_param_pairs(str(self.machine_id or ""))
        lis_to_instr = {}
        instr_to_lis = {}
        for row in pairs:
            instr = (row.get("param_code") or "").strip()
            lis = (row.get("lis_code") or "").strip()
            if instr and lis:
                instr_to_lis[instr.lower()] = lis
                lis_to_instr[lis.lower()] = instr

        mapped = []
        for lis in lis_tests:
            key = (lis or "").strip().lower()
            if not key:
                continue
            if key in lis_to_instr:
                mapped.append(lis_to_instr[key])
            elif key in instr_to_lis:
                mapped.append(key)
        return mapped

    def _build_astm_order_response(self, sample_id: str, tests: List[str]) -> str:
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        test_field = "\\".join(tests)
        lines = [
            "H|\\^&|||LIS|||||||P|1",
            "P|1",
            f"O|1|{sample_id}||{test_field}|R|{timestamp}|||||||||O",
            "L|1|N",
        ]
        return "\r".join(lines) + "\r"

    def _build_astm_frames(self, payload: str) -> List[bytes]:
        max_len = 240
        frames = []
        seq = 1
        text = payload
        while text:
            chunk = text[:max_len]
            text = text[max_len:]
            seq_char = str(seq % 8)
            body = f"{seq_char}{chunk}"
            checksum = self._astm_checksum(body.encode("ascii", errors="ignore"))
            frame = b"\x02" + body.encode("ascii", errors="ignore") + b"\x03" + checksum + b"\r\n"
            frames.append(frame)
            seq += 1
        return frames

    def _astm_checksum(self, data: bytes) -> bytes:
        total = sum(data) % 256
        return f"{total:02X}".encode("ascii")

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

    def _split_message(self, buffer: str):
        terminators = ["\x04", "\x03"]
        indices = [(buffer.find(t), t) for t in terminators if buffer.find(t) != -1]
        if not indices:
            return None
        idx, term = min(indices, key=lambda item: item[0])
        raw_message = buffer[:idx]
        remaining = buffer[idx + len(term):]
        return raw_message, remaining

    def _decode_raw(self, data: bytes) -> str:
        try:
            return data.decode("utf-8", errors="ignore")
        except Exception:
            return data.decode("latin1", errors="ignore")
