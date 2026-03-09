from __future__ import annotations

import base64
import json
import subprocess
import uuid
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from ._binary import resolve_featurectl_binary

TimeLike = int | float | str | datetime
RangeLike = TimeLike | tuple[TimeLike, TimeLike] | list[TimeLike]


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
        self, client: "MXDBClient", namespace: str, entity_name: str
    ) -> None:
        self._client = client
        self.namespace = namespace
        self.entity_name = entity_name

    def upsert(
        self,
        feature_id: str,
        event_time: TimeLike,
        value: Any,
    ) -> int:
        event_time_us = self._client._normalize_time_us(event_time)
        write_id = self._client._new_write_id(
            "upsert",
            self.namespace,
            self.entity_name,
            feature_id,
            event_time_us,
        )
        return self._client._ingest_entity(
            self.namespace,
            self._client.entity_type,
            self.entity_name,
            feature_id,
            event_time_us,
            value,
            write_id,
            operation="upsert",
        )

    def delete(
        self,
        feature_id: str,
        event_time: TimeLike,
    ) -> int:
        event_time_us = self._client._normalize_time_us(event_time)
        write_id = self._client._new_write_id(
            "delete",
            self.namespace,
            self.entity_name,
            feature_id,
            event_time_us,
        )
        return self._client._ingest_entity(
            self.namespace,
            self._client.entity_type,
            self.entity_name,
            feature_id,
            event_time_us,
            0.0,
            write_id,
            operation="delete",
        )

    def latest(
        self, feature_id: str, count: int = 1
    ) -> TypedFeatureResult | list[TypedFeatureResult]:
        return self._client._latest_entity(
            self.namespace, self._client.entity_type, self.entity_name, feature_id, count
        )

    def latest_double(self, feature_id: str) -> FeatureResult:
        return self._client._latest_double_entity(
            self.namespace, self._client.entity_type, self.entity_name, feature_id
        )

    def get(self) -> dict[str, TypedFeatureResult]:
        return self._client._get_entity(self.namespace, self._client.entity_type, self.entity_name)

    def get_range(
        self,
        feature_id: str,
        date_range: RangeLike,
        disk: bool = True,
    ) -> list[TypedFeatureResult]:
        return self._client._get_range_entity(
            self.namespace,
            self._client.entity_type,
            self.entity_name,
            feature_id,
            date_range,
            disk,
        )


class MXDBClient:
    def __init__(
        self, config_path: str, featurectl_bin: Optional[str] = None
    ) -> None:
        self.config_path = str(config_path)
        self.featurectl_bin = resolve_featurectl_binary(featurectl_bin)
        self._entity_type = "entity"

    @property
    def entity_type(self) -> str:
        return self._entity_type

    def register_feature(
        self,
        namespace: str,
        feature_name: str,
        value_type: str = "double",
    ) -> None:
        self._run(
            [
                "register-feature",
                namespace,
                self.entity_type,
                feature_name,
                feature_name,
                value_type,
            ]
        )

    def entity(self, namespace: str, entity_name: str) -> MXDBEntityClient:
        return MXDBEntityClient(self, namespace, entity_name)

    @staticmethod
    def _new_write_id(
        operation: str,
        namespace: str,
        entity_name: str,
        feature_id: str,
        event_time_us: int,
    ) -> str:
        nonce = uuid.uuid4().hex
        return (
            f"py-{operation}-{namespace}-{entity_name}-{feature_id}-"
            f"{event_time_us}-{nonce}"
        )

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

    def _get_range_entity(
        self,
        tenant: str,
        entity_type: str,
        entity_id: str,
        feature_id: str,
        date_range: RangeLike,
        disk: bool = True,
    ) -> list[TypedFeatureResult]:
        latest, furthest = self._normalize_latest_furthest_range(date_range)
        if not isinstance(disk, bool):
            raise TypeError("disk must be a bool")

        args = ["range", tenant, entity_type, entity_id, feature_id, str(furthest)]
        if latest is not None:
            args.append(str(latest))
        args.append("disk" if disk else "memory")

        out = self._run(args)
        return self._parse_typed_feature_result_list(out)

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
    def _normalize_latest_furthest_range(
        cls, value: RangeLike
    ) -> tuple[int | None, int]:
        if isinstance(value, (tuple, list)):
            if len(value) != 2:
                raise ValueError(
                    "date_range must be either a single value (furthest) or "
                    "(latest, furthest)"
                )
            latest = cls._normalize_time_us(value[0])
            furthest = cls._normalize_time_us(value[1])
            if latest < furthest:
                raise ValueError("latest must be >= furthest")
            return latest, furthest

        return None, cls._normalize_time_us(value)
