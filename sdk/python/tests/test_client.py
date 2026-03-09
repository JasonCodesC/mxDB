from __future__ import annotations

import os
import tempfile
import time
import unittest
from datetime import datetime, timezone
from pathlib import Path

from mxdb import MXDBClient


class MXDBClientTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="mxdb-sdk-")
        self.tmp_path = Path(self.tmp.name)
        self.config_path = self.tmp_path / "featured.conf"

        data_dir = self.tmp_path / "data"
        self.config_path.write_text(
            "\n".join(
                [
                    f"data_dir={data_dir}",
                    f"metadata_path={data_dir / 'catalog' / 'metadata.db'}",
                    f"wal_dir={data_dir / 'wal'}",
                    f"segment_dir={data_dir / 'segments'}",
                    f"manifest_path={data_dir / 'manifest' / 'manifest.log'}",
                    f"checkpoint_path={data_dir / 'checkpoints' / 'checkpoint.meta'}",
                    "memtable_flush_event_threshold=100",
                    "default_durability_sync=true",
                ]
            )
            + "\n"
        )

        repo_root = Path(__file__).resolve().parents[3]
        default_bin = repo_root / "build" / "featurectl"
        self.featurectl_bin = os.environ.get("FEATURECTL_BIN", str(default_bin))
        self.client = MXDBClient(str(self.config_path), self.featurectl_bin)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_end_to_end_flow(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")

        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 101.5)
        aapl.upsert("f_price", now + 10, 102.5)
        aapl.upsert("f_price", now + 20, 103.5)

        latest = aapl.latest_double("f_price")
        self.assertTrue(latest.found)
        self.assertEqual(latest.value, 103.5)

        bounded = aapl.get_range("f_price", (now + 15, now))
        self.assertEqual([x.value for x in bounded], [102.5, 101.5])

        open_ended = aapl.get_range("f_price", now + 10)
        self.assertEqual([x.value for x in open_ended], [103.5, 102.5])

        latest_many = aapl.latest("f_price", count=5)
        self.assertEqual([x.value for x in latest_many], [103.5, 102.5, 101.5])

        backup_dir = self.tmp_path / "backup"
        self.client.checkpoint()
        self.client.backup(str(backup_dir))
        self.assertTrue((backup_dir / "data").exists())

    def test_generic_type_roundtrip_matrix(self) -> None:
        self.client.register_feature("prod", "f_bool_false", "bool")
        self.client.register_feature("prod", "f_bool_true", "bool")
        self.client.register_feature("prod", "f_int_neg", "int64")
        self.client.register_feature("prod", "f_int_zero", "int64")
        self.client.register_feature("prod", "f_int_big", "int64")
        self.client.register_feature("prod", "f_str_quote", "string")
        self.client.register_feature("prod", "f_str_lines", "string")
        self.client.register_feature("prod", "f_str_true", "string")
        self.client.register_feature("prod", "f_fvec", "float_vector")
        self.client.register_feature("prod", "f_dvec", "double_vector")
        self.client.register_feature("prod", "f_double", "double")
        aapl = self.client.entity("prod", "AAPL")

        now = int(time.time() * 1_000_000)
        writes = [
            ("f_bool_false", False, "bool"),
            ("f_bool_true", True, "bool"),
            ("f_int_neg", -7, "int64"),
            ("f_int_zero", 0, "int64"),
            ("f_int_big", 9_223_372_036_854_775_000, "int64"),
            ("f_str_quote", 'quote " test', "string"),
            ("f_str_lines", "line1\nline2", "string"),
            ("f_str_true", "true", "string"),
            ("f_fvec", [1.25, -2.5, 3.75], "float_vector"),
            ("f_dvec", [1.0, 2.5, 3.25], "double_vector"),
            ("f_double", 101.5, "double"),
        ]

        for idx, (feature_id, value, _) in enumerate(writes):
            aapl.upsert(feature_id, now + idx, value)

        for idx, (feature_id, expected_value, expected_type) in enumerate(writes):
            latest = aapl.latest(feature_id)
            self.assertTrue(latest.found)
            self.assertEqual(latest.value_type, expected_type)
            self.assertEqual(latest.value, expected_value)

            ranged = aapl.get_range(feature_id, now + idx)
            self.assertEqual(len(ranged), 1)
            self.assertEqual(ranged[0].value_type, expected_type)
            self.assertEqual(ranged[0].value, expected_value)

        # Friendly timestamp parsing for get_range().
        furthest_iso = datetime.fromtimestamp(
            (now - 5_000_000) / 1_000_000, tz=timezone.utc
        )
        parsed = aapl.get_range("f_double", furthest_iso.isoformat())
        self.assertEqual(len(parsed), 1)
        self.assertEqual(parsed[0].value_type, "double")
        self.assertEqual(parsed[0].value, 101.5)

        latest_colon = datetime.fromtimestamp(
            (now + 5_000_000) / 1_000_000, tz=timezone.utc
        ).strftime("%Y:%m:%d:%H:%M:%S")
        parsed_colon = aapl.get_range(
            "f_double", (latest_colon, "1970:01:01:00:00:00")
        )
        self.assertEqual(len(parsed_colon), 1)
        self.assertEqual(parsed_colon[0].value_type, "double")
        self.assertEqual(parsed_colon[0].value, 101.5)

    def test_typed_helpers_reject_wrong_type(self) -> None:
        self.client.register_feature("prod", "f_flag", "bool")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.upsert("f_flag", now, True)

        with self.assertRaises(TypeError):
            aapl.latest_double("f_flag")

    def test_get_returns_all_features_for_entity(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        self.client.register_feature("prod", "f_flag", "bool")
        self.client.register_feature("prod", "f_note", "string")
        aapl = self.client.entity("prod", "AAPL")

        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 101.5)
        aapl.upsert("f_flag", now + 1, True)

        snapshot = aapl.get()
        self.assertEqual(set(snapshot.keys()), {"f_price", "f_flag", "f_note"})
        self.assertTrue(snapshot["f_price"].found)
        self.assertEqual(snapshot["f_price"].value_type, "double")
        self.assertEqual(snapshot["f_price"].value, 101.5)
        self.assertTrue(snapshot["f_flag"].found)
        self.assertEqual(snapshot["f_flag"].value_type, "bool")
        self.assertTrue(snapshot["f_flag"].value)
        self.assertFalse(snapshot["f_note"].found)

        missing_entity = self.client.entity("prod", "MSFT").get()
        self.assertEqual(set(missing_entity.keys()), {"f_price", "f_flag", "f_note"})
        self.assertTrue(all(not value.found for value in missing_entity.values()))

    def test_entity_scoped_api_is_compact(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        self.client.register_feature("prod", "f_flag", "bool")

        aapl = self.client.entity("prod", "AAPL")
        self.assertFalse(hasattr(aapl, "ingest"))
        self.assertFalse(hasattr(aapl, "ingest_double"))
        self.assertFalse(hasattr(aapl, "asof"))
        self.assertFalse(hasattr(aapl, "asof_double"))

        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 101.5)
        aapl.upsert("f_flag", now + 1, True)

        latest = aapl.latest("f_price")
        self.assertTrue(latest.found)
        self.assertEqual(latest.value_type, "double")
        self.assertEqual(latest.value, 101.5)

        ranged = aapl.get_range("f_price", now)
        self.assertEqual([x.value for x in ranged], [101.5])

        snapshot = aapl.get()
        self.assertEqual(set(snapshot.keys()), {"f_price", "f_flag"})
        self.assertTrue(snapshot["f_flag"].found)
        self.assertEqual(snapshot["f_flag"].value_type, "bool")
        self.assertTrue(snapshot["f_flag"].value)

    def test_get_range_accepts_latest_furthest_and_disk_scope(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        base = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        base_us = int(base.timestamp() * 1_000_000)

        aapl.upsert("f_price", base_us + 100_000, 10.0)
        aapl.upsert("f_price", base_us + 200_000, 20.0)
        aapl.upsert("f_price", base_us + 400_000, 40.0)

        ranged = aapl.get_range("f_price", (base_us + 250_000, base_us + 50_000))
        self.assertEqual([x.value for x in ranged], [20.0, 10.0])

        miss = aapl.get_range("f_price", (base_us + 350_000, base_us + 250_001))
        self.assertEqual(miss, [])

        range_latest_ms = datetime.fromtimestamp(
            (base_us + 250_000) / 1_000_000, tz=timezone.utc
        ).strftime("%Y:%m:%d:%H:%M:%S.%f")[:-3]
        range_furthest_ms = datetime.fromtimestamp(
            (base_us + 50_000) / 1_000_000, tz=timezone.utc
        ).strftime("%Y:%m:%d:%H:%M:%S.%f")[:-3]
        ranged_strings = aapl.get_range("f_price", (range_latest_ms, range_furthest_ms))
        self.assertEqual([x.value for x in ranged_strings], [20.0, 10.0])

        with self.assertRaises(ValueError):
            aapl.get_range("f_price", (base_us + 100_000, base_us + 300_000))

        mem_only_before = aapl.get_range(
            "f_price", (base_us + 500_000, base_us), disk=False
        )
        self.assertEqual([x.value for x in mem_only_before], [40.0, 20.0, 10.0])

        self.client.checkpoint()
        mem_only_after = aapl.get_range(
            "f_price", (base_us + 500_000, base_us), disk=False
        )
        self.assertEqual(mem_only_after, [])

        disk_after = aapl.get_range("f_price", (base_us + 500_000, base_us), disk=True)
        self.assertEqual([x.value for x in disk_after], [40.0, 20.0, 10.0])

    def test_latest_delete_semantics(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 10.0)
        aapl.upsert("f_price", now + 1, 20.0)
        aapl.delete("f_price", now + 2)

        latest_one = aapl.latest("f_price")
        self.assertFalse(latest_one.found)

        latest_many = aapl.latest("f_price", count=5)
        self.assertEqual(latest_many, [])

        history = aapl.get_range("f_price", (now + 3, now))
        self.assertEqual([x.value for x in history], [20.0, 10.0])

    def test_resolves_binary_from_env(self) -> None:
        old = os.environ.get("MXDB_FEATURECTL_BIN")
        os.environ["MXDB_FEATURECTL_BIN"] = self.featurectl_bin
        try:
            auto_client = MXDBClient(str(self.config_path))
            self.assertEqual(auto_client.featurectl_bin, self.featurectl_bin)
        finally:
            if old is None:
                del os.environ["MXDB_FEATURECTL_BIN"]
            else:
                os.environ["MXDB_FEATURECTL_BIN"] = old


if __name__ == "__main__":
    unittest.main()
