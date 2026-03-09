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
                    "memtable_flush_event_threshold=2",
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
        self.client.register_feature("prod", "instrument", "f_price", "price", "double")
        aapl = self.client.entity("prod", "instrument", "AAPL")

        now = int(time.time() * 1_000_000)
        aapl.ingest_double("f_price", now, 101.5, "w1", now)
        aapl.ingest_double("f_price", now + 10, 102.5, "w2", now + 10)
        aapl.ingest_double("f_price", now + 20, 103.5, "w3", now + 20)

        latest = aapl.latest_double("f_price")
        self.assertTrue(latest.found)
        self.assertEqual(latest.value, 103.5)

        asof = aapl.asof_double("f_price", now + 15, now + 15)
        self.assertTrue(asof.found)
        self.assertEqual(asof.value, 102.5)

        latest_many = aapl.latest("f_price", count=5)
        self.assertEqual([x.value for x in latest_many], [103.5, 102.5, 101.5])

        backup_dir = self.tmp_path / "backup"
        self.client.checkpoint()
        self.client.backup(str(backup_dir))
        self.assertTrue((backup_dir / "data").exists())

    def test_generic_type_roundtrip_matrix(self) -> None:
        self.client.register_feature("prod", "instrument", "f_bool_false", "b0", "bool")
        self.client.register_feature("prod", "instrument", "f_bool_true", "b1", "bool")
        self.client.register_feature("prod", "instrument", "f_int_neg", "i0", "int64")
        self.client.register_feature("prod", "instrument", "f_int_zero", "i1", "int64")
        self.client.register_feature("prod", "instrument", "f_int_big", "i2", "int64")
        self.client.register_feature("prod", "instrument", "f_str_quote", "s0", "string")
        self.client.register_feature("prod", "instrument", "f_str_lines", "s1", "string")
        self.client.register_feature("prod", "instrument", "f_str_true", "s2", "string")
        self.client.register_feature(
            "prod", "instrument", "f_fvec", "fv", "float_vector"
        )
        self.client.register_feature(
            "prod", "instrument", "f_dvec", "dv", "double_vector"
        )
        self.client.register_feature("prod", "instrument", "f_double", "d0", "double")
        aapl = self.client.entity("prod", "instrument", "AAPL")

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
            aapl.ingest(feature_id, now + idx, value, f"w-{idx}", now + idx)

        for feature_id, expected_value, expected_type in writes:
            latest = aapl.latest(feature_id)
            self.assertTrue(latest.found)
            self.assertEqual(latest.value_type, expected_type)
            self.assertEqual(latest.value, expected_value)

            asof = aapl.asof(feature_id, now + 1000, now + 1000)
            self.assertTrue(asof.found)
            self.assertEqual(asof.value_type, expected_type)
            self.assertEqual(asof.value, expected_value)

        # Friendly timestamp parsing + default system cutoff path.
        cutoff_iso = datetime.fromtimestamp((now + 5_000_000) / 1_000_000, tz=timezone.utc)
        parsed = aapl.asof("f_double", cutoff_iso.isoformat(), None)
        self.assertTrue(parsed.found)
        self.assertEqual(parsed.value_type, "double")
        self.assertEqual(parsed.value, 101.5)

        cutoff_colon = cutoff_iso.strftime("%Y:%m:%d:%H:%M:%S")
        parsed_colon = aapl.asof("f_double", cutoff_colon, cutoff_colon)
        self.assertTrue(parsed_colon.found)
        self.assertEqual(parsed_colon.value_type, "double")
        self.assertEqual(parsed_colon.value, 101.5)

    def test_typed_helpers_reject_wrong_type(self) -> None:
        self.client.register_feature("prod", "instrument", "f_flag", "flag", "bool")
        aapl = self.client.entity("prod", "instrument", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.ingest("f_flag", now, True, "w-flag", now)

        with self.assertRaises(TypeError):
            aapl.latest_double("f_flag")

        with self.assertRaises(TypeError):
            aapl.asof_double("f_flag", now + 1, now + 1)

    def test_get_returns_all_features_for_entity(self) -> None:
        self.client.register_feature("prod", "instrument", "f_price", "price", "double")
        self.client.register_feature("prod", "instrument", "f_flag", "flag", "bool")
        self.client.register_feature("prod", "instrument", "f_note", "note", "string")
        aapl = self.client.entity("prod", "instrument", "AAPL")

        now = int(time.time() * 1_000_000)
        aapl.ingest("f_price", now, 101.5, "w-price", now)
        aapl.ingest("f_flag", now + 1, True, "w-flag", now + 1)

        snapshot = aapl.get()
        self.assertEqual(set(snapshot.keys()), {"f_price", "f_flag", "f_note"})
        self.assertTrue(snapshot["f_price"].found)
        self.assertEqual(snapshot["f_price"].value_type, "double")
        self.assertEqual(snapshot["f_price"].value, 101.5)
        self.assertTrue(snapshot["f_flag"].found)
        self.assertEqual(snapshot["f_flag"].value_type, "bool")
        self.assertTrue(snapshot["f_flag"].value)
        self.assertFalse(snapshot["f_note"].found)

        # Unknown entity returns all registered features as not found.
        missing_entity = self.client.entity("prod", "instrument", "MSFT").get()
        self.assertEqual(set(missing_entity.keys()), {"f_price", "f_flag", "f_note"})
        self.assertTrue(all(not value.found for value in missing_entity.values()))

    def test_entity_scoped_api_is_compact(self) -> None:
        self.client.register_feature("prod", "instrument", "f_price", "price", "double")
        self.client.register_feature("prod", "instrument", "f_flag", "flag", "bool")

        aapl = self.client.entity("prod", "instrument", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.ingest("f_price", now, 101.5, "w1", now)
        aapl.ingest("f_flag", now + 1, True, "w2", now + 1)

        latest = aapl.latest("f_price")
        self.assertTrue(latest.found)
        self.assertEqual(latest.value_type, "double")
        self.assertEqual(latest.value, 101.5)

        asof = aapl.asof("f_price", now + 5, now + 5)
        self.assertTrue(asof.found)
        self.assertEqual(asof.value, 101.5)

        snapshot = aapl.get()
        self.assertEqual(set(snapshot.keys()), {"f_price", "f_flag"})
        self.assertTrue(snapshot["f_flag"].found)
        self.assertEqual(snapshot["f_flag"].value_type, "bool")
        self.assertTrue(snapshot["f_flag"].value)

    def test_asof_accepts_time_ranges(self) -> None:
        self.client.register_feature("prod", "instrument", "f_price", "price", "double")
        aapl = self.client.entity("prod", "instrument", "AAPL")
        base = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
        base_us = int(base.timestamp() * 1_000_000)

        aapl.ingest_double("f_price", base_us + 100_000, 10.0, "w1", base_us + 100_000)
        aapl.ingest_double("f_price", base_us + 200_000, 20.0, "w2", base_us + 200_000)
        aapl.ingest_double("f_price", base_us + 400_000, 40.0, "w3", base_us + 400_000)

        ranged = aapl.asof(
            "f_price", (base_us + 50_000, base_us + 250_000), (base_us + 50_000, base_us + 250_000)
        )
        self.assertTrue(ranged.found)
        self.assertEqual(ranged.value_type, "double")
        self.assertEqual(ranged.value, 20.0)

        miss = aapl.asof(
            "f_price", (base_us + 250_001, base_us + 350_000), (base_us + 250_001, base_us + 350_000)
        )
        self.assertFalse(miss.found)

        # Millisecond-style friendly strings are accepted for ranges.
        range_start = datetime.fromtimestamp((base_us + 50_000) / 1_000_000, tz=timezone.utc)
        range_end = datetime.fromtimestamp((base_us + 250_000) / 1_000_000, tz=timezone.utc)
        range_start_ms = range_start.strftime("%Y:%m:%d:%H:%M:%S.%f")[:-3]
        range_end_ms = range_end.strftime("%Y:%m:%d:%H:%M:%S.%f")[:-3]
        ranged_strings = aapl.asof("f_price", (range_start_ms, range_end_ms))
        self.assertTrue(ranged_strings.found)
        self.assertEqual(ranged_strings.value, 20.0)

        with self.assertRaises(ValueError):
            aapl.asof("f_price", (base_us + 300_000, base_us + 100_000))

    def test_latest_delete_semantics(self) -> None:
        self.client.register_feature("prod", "instrument", "f_price", "price", "double")
        aapl = self.client.entity("prod", "instrument", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.ingest_double("f_price", now, 10.0, "w1", now)
        aapl.ingest_double("f_price", now + 1, 20.0, "w2", now + 1)
        aapl.delete("f_price", now + 2, "w3", now + 2)

        latest_one = aapl.latest("f_price")
        self.assertFalse(latest_one.found)

        latest_many = aapl.latest("f_price", count=5)
        self.assertEqual(latest_many, [])

        # As-of before delete still returns the last upsert.
        asof_before_delete = aapl.asof("f_price", now + 1, now + 1)
        self.assertTrue(asof_before_delete.found)
        self.assertEqual(asof_before_delete.value_type, "double")
        self.assertEqual(asof_before_delete.value, 20.0)

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
