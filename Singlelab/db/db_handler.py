import threading
from collections import defaultdict
from typing import Dict, List, Optional

import pyodbc

from config.settings import db_config
from core.normalized_result import NormalizedResult

SCHEMA_DEFAULTS = {
    "machine_table": "MachineMaster",
    "machine_name_field": "MachineName",
    "machine_port_field": "CommPort",
    "machine_settings_field": "Settings",
    "machine_id_field": "MachineId",
    "lab_results_table": "LAB_RESULTS",
    "sample_id_col": "sample_id",
    "param_code_col": "parameter_code",
    "result_col": "result",
    "updated_at_col": "updated_at",
    "machine_id_col": "machine_id",
    "status_col": "status",
}


class DBHandler:
    """Lightweight helper class to interact with the LIS database via pyodbc."""

    def __init__(self, settings: Optional[Dict[str, str]] = None) -> None:
        self._cfg = settings or db_config.get("db") or db_config
        if not self._cfg:
            raise ValueError("Database configuration is missing.")

        self._conn: Optional[pyodbc.Connection] = None
        self._lock = threading.Lock()
        self._connection_string = self._build_connection_string()
        self._schema = dict(SCHEMA_DEFAULTS)

    # ------------------------------------------------------------------
    def _build_connection_string(self) -> str:
        driver = self._cfg.get("driver", "{ODBC Driver 17 for SQL Server}")
        server = self._cfg.get("server")
        database = self._cfg.get("database")
        username = self._cfg.get("username")
        password = self._cfg.get("password")

        if not server or not database:
            raise ValueError("Both 'server' and 'database' must be provided in the DB config.")

        parts = [
            f"DRIVER={driver}",
            f"SERVER={server}",
            f"DATABASE={database}",
        ]

        if username and password:
            parts.append(f"UID={username}")
            parts.append(f"PWD={password}")
        else:
            parts.append("Trusted_Connection=yes")

        return ";".join(parts)

    # ------------------------------------------------------------------
    def _ensure_connection(self) -> pyodbc.Connection:
        with self._lock:
            if self._conn is None:
                self._conn = pyodbc.connect(self._connection_string, autocommit=False)
            return self._conn

    # ------------------------------------------------------------------
    def get_connection(self) -> pyodbc.Connection:
        """Return the shared connection, opening it if necessary."""
        return self._ensure_connection()

    # ------------------------------------------------------------------
    def get_new_connection(self) -> pyodbc.Connection:
        """Return a brand new dedicated connection (used by worker threads)."""
        return pyodbc.connect(self._connection_string, autocommit=False)

    # ------------------------------------------------------------------
    def close(self) -> None:
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                finally:
                    self._conn = None

    # ------------------------------------------------------------------
    def _fetch_dicts(self, cursor) -> List[Dict[str, str]]:
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    # ------------------------------------------------------------------
    def get_machines(self) -> List[Dict[str, str]]:
        """Fetch configured machines so the UI/API can list them."""
        table = self._schema.get("machine_table", "MachineMaster")
        name_field = self._schema.get("machine_name_field", "MachineName")
        port_field = self._schema.get("machine_port_field", "CommPort")
        settings_field = self._schema.get("machine_settings_field", "Settings")
        machine_id_field = self._schema.get("machine_id_field")

        base_parts = [
            f"{name_field} AS MachineName",
            f"{port_field} AS CommPort",
            f"{settings_field} AS Settings",
        ]

        optional_parts = []
        if machine_id_field:
            optional_parts.append(f"{machine_id_field} AS MachineId")

        select_parts = base_parts + optional_parts

        try:
            conn = self.get_connection()
        except Exception as exc:
            print(f"[DBHandler] Cannot connect to database: {exc}")
            return []

        def _run_query(parts: List[str]) -> List[Dict[str, str]]:
            sql = f"SELECT {', '.join(parts)} FROM {table}"
            cursor = conn.cursor()
            try:
                cursor.execute(sql)
                return self._fetch_dicts(cursor)
            finally:
                cursor.close()

        try:
            return _run_query(select_parts)
        except Exception as exc:
            if optional_parts:
                print(f"[DBHandler] Error fetching machines with optional columns: {exc}. Retrying without them.")
                try:
                    return _run_query(base_parts)
                except Exception as exc2:
                    print(f"[DBHandler] Fallback machine fetch failed: {exc2}")
                    return []
            print(f"[DBHandler] Error fetching machines: {exc}")
            return []

    # ------------------------------------------------------------------
    def get_recent_samples(self, limit: int = 3) -> Dict[str, List[Dict[str, str]]]:
        """Return the latest sample IDs per machine, capped per machine by limit."""
        limit = max(int(limit or 0), 0)
        if limit <= 0:
            return {}

        table = self._schema.get("lab_results_table", "LAB_RESULTS")
        sample_col = self._schema.get("sample_id_col", "sample_id")
        updated_col = self._schema.get("updated_at_col", "updated_at")
        machine_col = self._schema.get("machine_id_col", "machine_id")

        sql = f"""
        WITH ranked AS (
            SELECT
                CAST({machine_col} AS NVARCHAR(255)) AS machine_id,
                CAST({sample_col} AS NVARCHAR(255)) AS sample_id,
                {updated_col} AS updated_at,
                ROW_NUMBER() OVER (
                    PARTITION BY {machine_col}
                    ORDER BY {updated_col} DESC
                ) AS rn
            FROM {table}
            WHERE {sample_col} IS NOT NULL
        )
        SELECT machine_id, sample_id, updated_at
        FROM ranked
        WHERE rn <= ?
        ORDER BY machine_id, rn
        """

        try:
            conn = self.get_new_connection()
        except Exception as exc:
            print(f"[DBHandler] Cannot connect to database: {exc}")
            return {}

        cursor = conn.cursor()
        try:
            cursor.execute(sql, (limit,))
            rows = self._fetch_dicts(cursor)
        except Exception as exc:
            print(f"[DBHandler] Failed fetching recent samples: {exc}")
            return {}
        finally:
            cursor.close()
            conn.close()

        grouped: Dict[str, List[Dict[str, str]]] = defaultdict(list)
        for row in rows:
            machine_id = str(row.get("machine_id") or "").strip()
            if not machine_id:
                continue
            grouped[machine_id].append(
                {
                    "sample_id": row.get("sample_id"),
                    "updated_at": row.get("updated_at"),
                }
            )
        return dict(grouped)

    # ------------------------------------------------------------------
    def get_param_map(self, machine_id: str) -> Dict[str, str]:
        """
        Return instrument-code -> LIS parameter mapping for a machine.

        Expects a MachineParam table with columns:
          - MachineId
          - ParamCode (instrument code)
          - MachineParamId (LIS parameter code)
        """
        if not machine_id:
            return {}

        sql = """
        SELECT
            CAST(ParamCode AS NVARCHAR(255)) AS param_code,
            CAST(MachineParamId AS NVARCHAR(255)) AS lis_code
        FROM MachineParam
        WHERE MachineId = ?
        """
        try:
            conn = self.get_connection()
        except Exception as exc:
            print(f"[DBHandler] Cannot connect to database: {exc}")
            return {}

        cursor = conn.cursor()
        try:
            cursor.execute(sql, (machine_id,))
            rows = self._fetch_dicts(cursor)
        except Exception as exc:
            print(f"[DBHandler] Failed fetching MachineParam for {machine_id}: {exc}")
            return {}
        finally:
            cursor.close()

        # Build a case-insensitive map
        mapping: Dict[str, str] = {}
        for row in rows:
            key = (row.get("param_code") or "").strip()
            val = (row.get("lis_code") or "").strip()
            if not key or not val:
                continue
            mapping[key.lower()] = val

        def _score(keys):
            score = 0
            for key in keys:
                if key and key.isalnum() and len(key) <= 5:
                    score += 1
            return score

        current_score = _score(mapping.keys())
        swapped = {value.lower(): key for key, value in mapping.items() if value}
        swapped_score = _score(swapped.keys())
        if swapped and swapped_score > current_score:
            return swapped
        return mapping

    # ------------------------------------------------------------------
    def update_lab_result(self, result: NormalizedResult) -> None:
        """Update LAB_RESULTS style table based on normalized payload."""
        try:
            conn = self.get_connection()
        except Exception as exc:
            print(f"[DBHandler] Cannot connect to database: {exc}")
            return
        cursor = conn.cursor()

        sql = (
            f"UPDATE {self._schema.get('lab_results_table', 'LAB_RESULTS')} "
            f"SET {self._schema.get('result_col', 'result')} = ?, "
            f"{self._schema.get('updated_at_col', 'updated_at')} = ?, "
            f"{self._schema.get('machine_id_col', 'machine_id')} = ?, "
            f"{self._schema.get('status_col', 'status')} = ? "
            f"WHERE {self._schema.get('sample_id_col', 'sample_id')} = ? "
            f"AND {self._schema.get('param_code_col', 'parameter_code')} = ?"
        )

        params = (
            result.result,
            result.updated_at,
            result.machine_id,
            result.status,
            result.sample_id,
            result.parameter_code,
        )

        try:
            cursor.execute(sql, params)
            conn.commit()
        except Exception as exc:
            conn.rollback()
            print(f"[DBHandler] Failed to update lab result for {result.sample_id}: {exc}")
        finally:
            cursor.close()

    # ------------------------------------------------------------------
    def test_connection(self) -> bool:
        """Attempt to open/close a temporary connection."""
        try:
            conn = self.get_new_connection()
            conn.close()
            return True
        except Exception as exc:
            print(f"[DBHandler] Connection test failed: {exc}")
            return False
