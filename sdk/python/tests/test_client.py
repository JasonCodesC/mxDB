from __future__ import annotations

import os
import tempfile
import time
import unittest
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
        self.client.register_feature(
            "prod", "instrument", "f_flag", "flag", "bool"
        )
        self.client.register_feature(
            "prod", "instrument", "f_rank", "rank", "int64"
        )
        self.client.register_feature(
            "prod", "instrument", "f_label", "label", "string"
        )
        self.client.register_feature(
            "prod", "instrument", "f_vec", "vec", "double_vector"
        )

        now = int(time.time() * 1_000_000)
        self.client.ingest_double(
            tenant="prod",
            entity_type="instrument",
            entity_id="AAPL",
            feature_id="f_price",
            event_time_us=now,
            value=101.5,
            write_id="w1",
            system_time_us=now,
        )
        self.client.ingest(
            tenant="prod",
            entity_type="instrument",
            entity_id="AAPL",
            feature_id="f_flag",
            event_time_us=now,
            value=True,
            write_id="w2",
            system_time_us=now,
        )
        self.client.ingest(
            tenant="prod",
            entity_type="instrument",
            entity_id="AAPL",
            feature_id="f_rank",
            event_time_us=now,
            value=7,
            write_id="w3",
            system_time_us=now,
        )
        self.client.ingest(
            tenant="prod",
            entity_type="instrument",
            entity_id="AAPL",
            feature_id="f_label",
            event_time_us=now,
            value="alpha beta",
            write_id="w4",
            system_time_us=now,
        )
        self.client.ingest(
            tenant="prod",
            entity_type="instrument",
            entity_id="AAPL",
            feature_id="f_vec",
            event_time_us=now,
            value=[1.0, 2.5, 3.25],
            write_id="w5",
            system_time_us=now,
        )

        latest = self.client.latest_double("prod", "instrument", "AAPL", "f_price")
        self.assertTrue(latest.found)
        self.assertEqual(latest.value, 101.5)

        asof = self.client.asof_double(
            "prod", "instrument", "AAPL", "f_price", now + 1, now + 1
        )
        self.assertTrue(asof.found)
        self.assertEqual(asof.value, 101.5)

        latest_flag = self.client.latest("prod", "instrument", "AAPL", "f_flag")
        self.assertTrue(latest_flag.found)
        self.assertEqual(latest_flag.value_type, "bool")
        self.assertEqual(latest_flag.value, True)

        asof_rank = self.client.asof(
            "prod", "instrument", "AAPL", "f_rank", now + 1, now + 1
        )
        self.assertTrue(asof_rank.found)
        self.assertEqual(asof_rank.value_type, "int64")
        self.assertEqual(asof_rank.value, 7)

        latest_label = self.client.latest("prod", "instrument", "AAPL", "f_label")
        self.assertTrue(latest_label.found)
        self.assertEqual(latest_label.value_type, "string")
        self.assertEqual(latest_label.value, "alpha beta")

        latest_vec = self.client.latest("prod", "instrument", "AAPL", "f_vec")
        self.assertTrue(latest_vec.found)
        self.assertEqual(latest_vec.value_type, "double_vector")
        self.assertEqual(latest_vec.value, [1.0, 2.5, 3.25])

        self.client.ingest_double(
            tenant="prod",
            entity_type="instrument",
            entity_id="AAPL",
            feature_id="f_price",
            event_time_us=now + 10,
            value=102.5,
            write_id="w6",
            system_time_us=now + 10,
        )
        self.client.ingest_double(
            tenant="prod",
            entity_type="instrument",
            entity_id="AAPL",
            feature_id="f_price",
            event_time_us=now + 20,
            value=103.5,
            write_id="w7",
            system_time_us=now + 20,
        )

        latest_three = self.client.latest(
            "prod", "instrument", "AAPL", "f_price", count=5
        )
        self.assertEqual(len(latest_three), 3)
        self.assertEqual([x.value for x in latest_three], [103.5, 102.5, 101.5])

        backup_dir = self.tmp_path / "backup"
        self.client.checkpoint()
        self.client.backup(str(backup_dir))
        self.assertTrue((backup_dir / "data").exists())

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
