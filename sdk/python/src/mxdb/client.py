from __future__ import annotations

import base64
import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ._binary import resolve_featurectl_binary


@dataclass
class FeatureResult:
    found: bool
    value: Optional[float]
    event_time_us: Optional[int]
    system_time_us: Optional[int]


@dataclass
class TypedFeatureResult:
    found: bool
    value_type: Optional[str]
    value: Optional[Any]
    event_time_us: Optional[int]
    system_time_us: Optional[int]
    lsn: Optional[int]


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

    def ingest(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        event_time_us: int,
        value: Any,
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
                self._encode_value_literal(value),
                write_id,
            ]
        )
        parts = self._parse_key_value_line(out)
        return int(parts.get("lsn", "0"))

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
        return self.ingest(
            tenant=tenant,
            entity_type=entity_type,
            entity_id=entity_id,
            feature_id=feature_id,
            event_time_us=event_time_us,
            value=value,
            write_id=write_id,
            system_time_us=system_time_us,
        )

    def latest(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        count: int = 1,
    ) -> TypedFeatureResult | list[TypedFeatureResult]:
        if count < 1:
            raise ValueError("count must be >= 1")

        args = ["latest", tenant, entity_type, entity_id, feature_id]
        if count > 1:
            args.append(str(count))

        out = self._run(args)
        if count == 1:
            return self._parse_typed_feature_result(out)
        return self._parse_typed_feature_result_list(out)

    def latest_double(
        self, tenant: str, entity_type: str, entity_id: str, feature_id: str
    ) -> FeatureResult:
        typed = self.latest(tenant, entity_type, entity_id, feature_id, count=1)
        if isinstance(typed, list):
            raise RuntimeError("unexpected list result for count=1")
        if not typed.found:
            return FeatureResult(False, None, None, None)
        if typed.value_type != "double":
            raise TypeError(
                f"feature type is {typed.value_type}, expected double for latest_double"
            )
        return FeatureResult(
            True,
            float(typed.value),
            typed.event_time_us,
            typed.system_time_us,
        )

    def asof(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        event_cutoff_us: int,
        system_cutoff_us: int,
    ) -> TypedFeatureResult:
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
        return self._parse_typed_feature_result(out)

    def asof_double(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        event_cutoff_us: int,
        system_cutoff_us: int,
    ) -> FeatureResult:
        typed = self.asof(
            tenant=tenant,
            entity_type=entity_type,
            entity_id=entity_id,
            feature_id=feature_id,
            event_cutoff_us=event_cutoff_us,
            system_cutoff_us=system_cutoff_us,
        )
        if not typed.found:
            return FeatureResult(False, None, None, None)
        if typed.value_type != "double":
            raise TypeError(
                f"feature type is {typed.value_type}, expected double for asof_double"
            )
        return FeatureResult(
            True,
            float(typed.value),
            typed.event_time_us,
            typed.system_time_us,
        )

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

    @staticmethod
    def _decode_json_b64(encoded: str) -> Any:
        raw = base64.b64decode(encoded.encode("ascii"))
        return json.loads(raw.decode("utf-8"))

    def _parse_typed_feature_result(self, line: str) -> TypedFeatureResult:
        parsed = self._parse_key_value_line(line)
        found = parsed.get("found", "0") == "1"
        if not found:
            return TypedFeatureResult(False, None, None, None, None, None)

        value = self._decode_json_b64(parsed["value_b64"])
        return TypedFeatureResult(
            True,
            parsed.get("value_type"),
            value,
            int(parsed["event_time_us"]),
            int(parsed["system_time_us"]),
            int(parsed["lsn"]) if "lsn" in parsed else None,
        )

    def _parse_typed_feature_result_list(self, line: str) -> list[TypedFeatureResult]:
        parsed = self._parse_key_value_line(line)
        found = parsed.get("found", "0") == "1"
        if not found:
            return []

        payload = self._decode_json_b64(parsed["values_b64"])
        if not isinstance(payload, list):
            raise RuntimeError("invalid latest response payload")

        out: list[TypedFeatureResult] = []
        for item in payload:
            if not isinstance(item, dict):
                raise RuntimeError("invalid latest response item")
            out.append(
                TypedFeatureResult(
                    True,
                    str(item.get("value_type")) if item.get("value_type") is not None else None,
                    item.get("value"),
                    int(item["event_time_us"]),
                    int(item["system_time_us"]),
                    int(item["lsn"]),
                )
            )
        return out

    @staticmethod
    def _encode_value_literal(value: Any) -> str:
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, int) and not isinstance(value, bool):
            return str(value)
        if isinstance(value, float):
            return repr(value)
        if isinstance(value, str):
            return value
        if isinstance(value, list):
            return json.dumps(value, separators=(",", ":"))
        raise TypeError(
            "unsupported value type for ingest; expected bool/int/float/str/list"
        )
