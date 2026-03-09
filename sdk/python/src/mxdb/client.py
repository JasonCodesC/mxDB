from __future__ import annotations

import base64
import json
import subprocess
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ._binary import resolve_featurectl_binary

TimeLike = int | float | str | datetime
TimeRangeLike = tuple[TimeLike, TimeLike] | list[TimeLike]
TimePointOrRange = TimeLike | TimeRangeLike


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


class MXDBEntityClient:
    def __init__(
        self, client: "MXDBClient", tenant: str, entity_type: str, entity_id: str
    ) -> None:
        self._client = client
        self.tenant = tenant
        self.entity_type = entity_type
        self.entity_id = entity_id

    def ingest(
        self,
        feature_id: str,
        event_time_us: int,
        value: Any,
        write_id: str,
        system_time_us: Optional[int] = None,
        operation: str = "upsert",
    ) -> int:
        return self._client._ingest_entity(
            self.tenant,
            self.entity_type,
            self.entity_id,
            feature_id,
            event_time_us,
            value,
            write_id,
            system_time_us,
            operation,
        )

    def ingest_double(
        self,
        feature_id: str,
        event_time_us: int,
        value: float,
        write_id: str,
        system_time_us: Optional[int] = None,
    ) -> int:
        return self._client._ingest_entity(
            self.tenant,
            self.entity_type,
            self.entity_id,
            feature_id,
            event_time_us,
            value,
            write_id,
            system_time_us,
            "upsert",
        )

    def delete(
        self,
        feature_id: str,
        event_time_us: int,
        write_id: str,
        system_time_us: Optional[int] = None,
    ) -> int:
        return self._client._ingest_entity(
            self.tenant,
            self.entity_type,
            self.entity_id,
            feature_id,
            event_time_us,
            0.0,
            write_id,
            system_time_us,
            "delete",
        )

    def latest(
        self, feature_id: str, count: int = 1
    ) -> TypedFeatureResult | list[TypedFeatureResult]:
        return self._client._latest_entity(
            self.tenant, self.entity_type, self.entity_id, feature_id, count
        )

    def latest_double(self, feature_id: str) -> FeatureResult:
        return self._client._latest_double_entity(
            self.tenant, self.entity_type, self.entity_id, feature_id
        )

    def get(self) -> dict[str, TypedFeatureResult]:
        return self._client._get_entity(self.tenant, self.entity_type, self.entity_id)

    def asof(
        self,
        feature_id: str,
        event_cutoff_us: TimePointOrRange,
        system_cutoff_us: TimePointOrRange | None = None,
    ) -> TypedFeatureResult:
        return self._client._asof_entity(
            self.tenant,
            self.entity_type,
            self.entity_id,
            feature_id,
            event_cutoff_us,
            system_cutoff_us,
        )

    def asof_double(
        self,
        feature_id: str,
        event_cutoff_us: TimePointOrRange,
        system_cutoff_us: TimePointOrRange | None = None,
    ) -> FeatureResult:
        return self._client._asof_double_entity(
            self.tenant,
            self.entity_type,
            self.entity_id,
            feature_id,
            event_cutoff_us,
            system_cutoff_us,
        )


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

    def entity(self, tenant: str, entity_type: str, entity_id: str) -> MXDBEntityClient:
        return MXDBEntityClient(self, tenant, entity_type, entity_id)

    def _ingest_entity(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        event_time_us: int,
        value: Any,
        write_id: str,
        system_time_us: Optional[int] = None,
        operation: str = "upsert",
    ) -> int:
        if operation not in ("upsert", "delete"):
            raise ValueError("operation must be 'upsert' or 'delete'")

        system_arg = "auto" if system_time_us is None else str(system_time_us)
        args = [
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
        if operation != "upsert":
            args.append(operation)
        out = self._run(
            args
        )
        parts = self._parse_key_value_line(out)
        return int(parts.get("lsn", "0"))

    def _latest_entity(
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

    def _latest_double_entity(
        self, tenant: str, entity_type: str, entity_id: str, feature_id: str
    ) -> FeatureResult:
        typed = self._latest_entity(tenant, entity_type, entity_id, feature_id, count=1)
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

    def _get_entity(
        self, tenant: str, entity_type: str, entity_id: str
    ) -> dict[str, TypedFeatureResult]:
        out = self._run(["get", tenant, entity_type, entity_id])
        return self._parse_typed_feature_result_map(out)

    def _asof_entity(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        event_cutoff_us: TimePointOrRange,
        system_cutoff_us: TimePointOrRange | None = None,
    ) -> TypedFeatureResult:
        event_range_start, event_cutoff = self._normalize_time_range(event_cutoff_us)
        if system_cutoff_us is None:
            system_range_start = None
            system_cutoff = int(time.time() * 1_000_000)
        else:
            system_range_start, system_cutoff = self._normalize_time_range(
                system_cutoff_us
            )
        out = self._run(
            [
                "asof",
                tenant,
                entity_type,
                entity_id,
                feature_id,
                str(event_cutoff),
                str(system_cutoff),
            ]
        )
        result = self._parse_typed_feature_result(out)
        if not result.found:
            return result

        if (
            event_range_start is not None
            and result.event_time_us is not None
            and result.event_time_us < event_range_start
        ):
            return TypedFeatureResult(False, None, None, None, None, None)

        if (
            system_range_start is not None
            and result.system_time_us is not None
            and result.system_time_us < system_range_start
        ):
            return TypedFeatureResult(False, None, None, None, None, None)

        return result

    def _asof_double_entity(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        event_cutoff_us: TimePointOrRange,
        system_cutoff_us: TimePointOrRange | None = None,
    ) -> FeatureResult:
        typed = self._asof_entity(
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

    def _parse_typed_feature_result_map(
        self, line: str
    ) -> dict[str, TypedFeatureResult]:
        parsed = self._parse_key_value_line(line)
        found = parsed.get("found", "0") == "1"
        if not found:
            return {}

        payload = self._decode_json_b64(parsed["values_b64"])
        if not isinstance(payload, list):
            raise RuntimeError("invalid get response payload")

        out: dict[str, TypedFeatureResult] = {}
        for item in payload:
            if not isinstance(item, dict):
                raise RuntimeError("invalid get response item")
            feature_id = item.get("feature_id")
            if not isinstance(feature_id, str) or not feature_id:
                raise RuntimeError("invalid get response feature_id")

            item_found = bool(item.get("found", False))
            if not item_found:
                out[feature_id] = TypedFeatureResult(False, None, None, None, None, None)
                continue

            out[feature_id] = TypedFeatureResult(
                True,
                str(item.get("value_type")) if item.get("value_type") is not None else None,
                item.get("value"),
                int(item["event_time_us"]),
                int(item["system_time_us"]),
                int(item["lsn"]) if item.get("lsn") is not None else None,
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

    @staticmethod
    def _normalize_time_us(value: int | str | datetime | float) -> int:
        if isinstance(value, datetime):
            dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1_000_000)

        if isinstance(value, int):
            return value

        if isinstance(value, float):
            return int(value * 1_000_000)

        if isinstance(value, str):
            raw = value.strip()
            if raw and (raw.isdigit() or (raw.startswith("-") and raw[1:].isdigit())):
                return int(raw)

            fmt_candidates = [
                "%Y:%m:%d:%H:%M:%S",
                "%Y:%m:%d:%H:%M:%S.%f",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M:%S.%f",
            ]
            for fmt in fmt_candidates:
                try:
                    parsed = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
                    return int(parsed.timestamp() * 1_000_000)
                except ValueError:
                    continue

            try:
                iso_input = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
                parsed = datetime.fromisoformat(iso_input)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return int(parsed.timestamp() * 1_000_000)
            except ValueError as exc:
                raise ValueError(
                    "unsupported time format; use epoch micros or "
                    "YYYY:MM:DD:HH:MM:SS[.ffffff] / ISO-8601"
                ) from exc

        raise TypeError("time value must be int, float, str, or datetime")

    @classmethod
    def _normalize_time_range(cls, value: TimePointOrRange) -> tuple[int | None, int]:
        if isinstance(value, (tuple, list)):
            if len(value) != 2:
                raise ValueError("time range must have exactly two values: (start, end)")

            start = cls._normalize_time_us(value[0])
            end = cls._normalize_time_us(value[1])
            if start > end:
                raise ValueError("time range start must be <= end")
            return start, end

        return None, cls._normalize_time_us(value)
