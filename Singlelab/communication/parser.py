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
        cleaned = re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", raw_code)
        return cleaned.strip().lower()
    for part in parts:
        cleaned = re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", part)
        if any(char.isalpha() for char in cleaned):
            return cleaned.strip().lower()
    cleaned = re.sub(r"^[^A-Za-z0-9]+|[^A-Za-z0-9]+$", "", parts[-1])
    return cleaned.strip().lower()

def _strip_controls(message: str) -> str:
    return (
        message.replace("\x02", "")
        .replace("\x03", "")
        .replace("\x04", "")
        .replace("\x05", "")
        .replace("\x1c", "")
    )


def _strip_frame_prefix(segment: str) -> str:
    while segment and segment[0].isdigit():
        segment = segment[1:]
    return segment


def detect_protocol(msg: bytes) -> str:
    try:
        text = msg.decode(errors="ignore")
        cleaned = _strip_controls(text).lstrip("\x0b").strip()
        if cleaned.startswith("MSH|"):
            return "HL7"
        for line in cleaned.splitlines():
            line = _strip_frame_prefix(line.strip())
            if line.startswith(("H|", "P|", "O|", "R|", "L|", "C|", "M|")):
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

        if line.startswith("PID") and len(fields) > 3:
            sample_id = fields[3].strip() or sample_id

        if line.startswith("OBR") and len(fields) > 3:
            if not sample_id:
                sample_id = fields[2].strip() or sample_id

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
    cleaned = _strip_controls(text)
    records = re.split(r'[\r\n]+', cleaned)
    results = []
    sample_id = ""

    is_astm = any(record.strip().startswith(("H|", "P|", "O|", "R|", "L|", "C|", "M|")) for record in records)
    if not is_astm:
        return _parse_plain_text(records, machine_id, param_map)

    for line in records:
        line = _strip_frame_prefix(line.strip())
        if not line:
            continue
        fields = line.split("|")

        if line.startswith("O") and len(fields) > 2:
            sample_id = fields[2].strip()

        if line.startswith("R") and len(fields) >= 4:
            raw_code = fields[2].strip() if len(fields) > 2 else fields[1].strip()
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


def _parse_plain_text(records: List[str], machine_id: str, param_map: dict) -> List[NormalizedResult]:
    sample_id = ""
    results: List[NormalizedResult] = []
    buffer = []

    for record in records:
        line = record.strip()
        if not line:
            continue
        upper = line.upper()
        if upper.startswith("DATE") or upper.startswith("NO."):
            continue
        if upper.startswith("SAMPLEID"):
            sample_id = line.split(":", 1)[-1].strip().split()[0].strip("-")
            continue
        if upper.startswith("ID:"):
            sample_id = line.split(":", 1)[-1].strip().split()[0].strip("-")
            continue
        buffer.append(line)

    if not sample_id:
        return results

    for line in buffer:
        parts = line.split()
        if len(parts) < 2:
            continue
        code = parts[0].strip(":").strip()
        value = " ".join(parts[1:]).strip()
        if not code or not value:
            continue
        parameter_code = param_map.get(_normalize_code(code))
        if not parameter_code:
            continue
        results.append(NormalizedResult(
            machine_id=machine_id,
            sample_id=sample_id,
            parameter_code=parameter_code,
            result=value,
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
