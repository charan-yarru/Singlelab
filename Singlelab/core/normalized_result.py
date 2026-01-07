from dataclasses import dataclass
from datetime import datetime

@dataclass
class NormalizedResult:
    sample_id: str
    parameter_code: str
    result: str
    machine_id: str
    status: str = "Y"
    updated_at: datetime = datetime.now()
