from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from ._binary import resolve_featurectl_binary


@dataclass
class FeatureResult:
    found: bool
    value: Optional[float]
    event_time_us: Optional[int]
    system_time_us: Optional[int]


class MXDBClient:
    def __init__(
        self, config_path: str, featurectl_bin: Optional[str] = None
    ) -> None:
        self.config_path = str(config_path)
        self.featurectl_bin = resolve_featurectl_binary(featurectl_bin)

    def register_feature(
        self,
        tenant: str,
        entity_type: str,
        feature_id: str,
        feature_name: str,
        value_type: str = "double",
    ) -> None:
        self._run(
            [
                "register-feature",
                tenant,
                entity_type,
                feature_id,
                feature_name,
                value_type,
            ]
        )

    def ingest_double(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        event_time_us: int,
        value: float,
        write_id: str,
        system_time_us: Optional[int] = None,
    ) -> int:
        system_arg = "auto" if system_time_us is None else str(system_time_us)
        out = self._run(
            [
                "ingest",
                tenant,
                entity_type,
                entity_id,
                feature_id,
                str(event_time_us),
                system_arg,
                str(value),
                write_id,
            ]
        )
        parts = self._parse_key_value_line(out)
        return int(parts.get("lsn", "0"))

    def latest_double(
        self, tenant: str, entity_type: str, entity_id: str, feature_id: str
    ) -> FeatureResult:
        out = self._run(["latest", tenant, entity_type, entity_id, feature_id])
        return self._parse_feature_result(out)

    def asof_double(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        event_cutoff_us: int,
        system_cutoff_us: int,
    ) -> FeatureResult:
        out = self._run(
            [
                "asof",
                tenant,
                entity_type,
                entity_id,
                feature_id,
                str(event_cutoff_us),
                str(system_cutoff_us),
            ]
        )
        return self._parse_feature_result(out)

    def checkpoint(self) -> None:
        self._run(["checkpoint"])

    def compact(self) -> None:
        self._run(["compact"])

    def set_read_only(self, enabled: bool) -> None:
        self._run(["readonly", "on" if enabled else "off"])

    def backup(self, destination_dir: str) -> None:
        Path(destination_dir).mkdir(parents=True, exist_ok=True)
        self._run(["backup", destination_dir])

    def _run(self, args: list[str]) -> str:
        cmd = [self.featurectl_bin, self.config_path] + args
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            raise RuntimeError(f"featurectl failed: {proc.stderr.strip()}")
        return proc.stdout.strip()

    @staticmethod
    def _parse_key_value_line(line: str) -> dict[str, str]:
        result: dict[str, str] = {}
        for token in line.split():
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            result[key] = value
        return result

    def _parse_feature_result(self, line: str) -> FeatureResult:
        parsed = self._parse_key_value_line(line)
        found = parsed.get("found", "0") == "1"
        if not found:
            return FeatureResult(False, None, None, None)
        return FeatureResult(
            True,
            float(parsed["value"]),
            int(parsed["event_time_us"]),
            int(parsed["system_time_us"]),
        )
