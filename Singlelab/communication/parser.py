from typing import List
import re

from core.normalized_result import NormalizedResult

ASTM_START = b'\x02'
ASTM_END = b'\x03'

def _normalize_code(raw_code: str) -> str:
    if not raw_code:
        return ""
    parts = [part for part in raw_code.split("^") if part]
    if not parts:
        return raw_code.strip().lower()
    for part in parts:
        if any(char.isalpha() for char in part):
            return part.strip().lower()
    return parts[-1].strip().lower()

def detect_protocol(msg: bytes) -> str:
    try:
        text = msg.decode(errors="ignore")
        if text.startswith("MSH|"):
            return "HL7"
        elif msg.startswith(ASTM_START) or "OBR|" in text or "OBX|" in text:
            return "ASTM"
    except Exception:
        pass
    return "UNKNOWN"

def parse_hl7(msg: bytes, machine_id: str, param_map: dict) -> List[NormalizedResult]:
    text = msg.decode(errors="ignore")
    lines = text.split("\r")
    results = []
    sample_id = ""

    for line in lines:
        fields = line.split("|")

        if line.startswith("PID"):
            continue

        if line.startswith("OBR") and len(fields) > 3:
            sample_id = fields[3].strip()

        if line.startswith("OBX") and len(fields) >= 6:
            raw_code = fields[3].strip()
            result = fields[5].strip()
            parameter_code = param_map.get(_normalize_code(raw_code))

            if parameter_code and sample_id and result:
                results.append(NormalizedResult(
                    machine_id=machine_id,
                    sample_id=sample_id,
                    parameter_code=parameter_code,
                    result=result
                ))
    return results

def parse_astm(msg: bytes, machine_id: str, param_map: dict) -> List[NormalizedResult]:
    text = msg.decode(errors="ignore")
    records = re.split(r'[\r\n]+', text)
    results = []
    sample_id = ""

    for line in records:
        fields = line.split("|")

        if line.startswith("O") and len(fields) > 2:
            sample_id = fields[2].strip()

        if line.startswith("R") and len(fields) >= 4:
            raw_code = fields[1].strip()
            result = fields[3].strip()
            parameter_code = param_map.get(_normalize_code(raw_code))

            if parameter_code and sample_id and result:
                results.append(NormalizedResult(
                    machine_id=machine_id,
                    sample_id=sample_id,
                    parameter_code=parameter_code,
                    result=result
                ))
    return results

def parse_message(msg: bytes, machine_id: str, param_map: dict) -> List[NormalizedResult]:
    protocol = detect_protocol(msg)

    if protocol == "HL7":
        return parse_hl7(msg, machine_id, param_map)
    elif protocol == "ASTM":
        return parse_astm(msg, machine_id, param_map)
    else:
        print(f"[parser]  Unknown protocol for machine {machine_id}")
        return []
