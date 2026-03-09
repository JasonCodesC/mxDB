from __future__ import annotations

import os
import subprocess
import tempfile
import threading
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
        default_featured_bin = repo_root / "build" / "featured"
        self.featurectl_bin = os.environ.get("FEATURECTL_BIN", str(default_bin))
        self.featured_bin = os.environ.get("FEATURED_BIN", str(default_featured_bin))
        self.client = MXDBClient(str(self.config_path), self.featurectl_bin)

    def _data_dir_path(self) -> Path:
        return self.tmp_path / "data"

    def _process_lock_path(self) -> Path:
        data_dir = self._data_dir_path()
        return data_dir.parent / f"{data_dir.name}.mxdb.process.lock"

    def _run_featurectl(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [self.featurectl_bin, str(self.config_path), *args],
            capture_output=True,
            text=True,
        )

    def _start_featured(self) -> subprocess.Popen[str]:
        proc = subprocess.Popen(
            [self.featured_bin, str(self.config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        deadline = time.time() + 5.0
        lock_path = self._process_lock_path()
        while time.time() < deadline:
            if proc.poll() is not None:
                stderr = proc.stderr.read() if proc.stderr is not None else ""
                if proc.stdout is not None:
                    proc.stdout.close()
                if proc.stderr is not None:
                    proc.stderr.close()
                raise AssertionError(f"featured exited early: {stderr.strip()}")
            if lock_path.exists():
                return proc
            time.sleep(0.02)

        proc.terminate()
        proc.wait(timeout=5)
        if proc.stdout is not None:
            proc.stdout.close()
        if proc.stderr is not None:
            proc.stderr.close()
        raise AssertionError("featured did not acquire process lock in time")

    def _stop_featured(self, proc: subprocess.Popen[str]) -> None:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        if proc.stdout is not None:
            proc.stdout.close()
        if proc.stderr is not None:
            proc.stderr.close()

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

    def test_default_config_path_is_auto_created(self) -> None:
        sdk_home = self.tmp_path / "sdk-home"
        previous_home = os.environ.get("MXDB_HOME")
        os.environ["MXDB_HOME"] = str(sdk_home)
        try:
            client = MXDBClient(featurectl_bin=self.featurectl_bin)
            expected_config = sdk_home / "featured.conf"
            self.assertEqual(Path(client.config_path), expected_config)
            self.assertTrue(expected_config.exists())
            self.assertIn("data_dir=", expected_config.read_text())

            client.register_feature("prod", "f_price", "double")
            row = client.entity("prod", "AAPL")
            now = int(time.time() * 1_000_000)
            row.upsert("f_price", now, 101.5)
            latest = row.latest("f_price")
            self.assertTrue(latest.found)
            self.assertEqual(latest.value, 101.5)
        finally:
            if previous_home is None:
                os.environ.pop("MXDB_HOME", None)
            else:
                os.environ["MXDB_HOME"] = previous_home

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
        self.assertIsNone(snapshot["f_price"].lsn)
        self.assertTrue(snapshot["f_flag"].found)
        self.assertEqual(snapshot["f_flag"].value_type, "bool")
        self.assertTrue(snapshot["f_flag"].value)
        self.assertIsNone(snapshot["f_flag"].lsn)
        self.assertFalse(snapshot["f_note"].found)
        self.assertIsNone(snapshot["f_note"].lsn)

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

    def test_stable_write_id_enables_retry_idempotency(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)

        aapl.upsert("f_price", now, 101.5, write_id="order-123-v1")
        aapl.upsert("f_price", now + 1, 999.0, write_id="order-123-v1")

        latest = aapl.latest("f_price")
        self.assertTrue(latest.found)
        self.assertEqual(latest.value, 101.5)

    def test_process_lock_rejects_overlapping_featurectl_commands(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 1.0)

        # Inflate backup copy duration to force overlap with another command.
        data_dir = self.tmp_path / "data"
        filler = data_dir / "backup-load.bin"
        filler.write_bytes(b"x" * (32 * 1024 * 1024))

        backup_error: list[BaseException] = []

        def run_backup() -> None:
            try:
                self.client.backup(str(self.tmp_path / "backup-lock"))
            except BaseException as exc:  # noqa: BLE001
                backup_error.append(exc)

        backup_thread = threading.Thread(target=run_backup)
        backup_thread.start()

        saw_lock_conflict = False
        attempt = 0
        while backup_thread.is_alive() and attempt < 50:
            attempt += 1
            try:
                aapl.upsert("f_price", now + 100 + attempt, 2.0 + attempt)
            except RuntimeError as exc:
                if "already using this data_dir" in str(exc):
                    saw_lock_conflict = True
                    break
            time.sleep(0.01)

        backup_thread.join()
        if backup_error:
            self.assertEqual(len(backup_error), 1)
            self.assertIn("already using this data_dir", str(backup_error[0]))
            saw_lock_conflict = True
        self.assertTrue(
            saw_lock_conflict,
            "expected at least one overlapping featurectl command to be rejected",
        )

    def test_featured_blocks_featurectl_health_same_data_dir(self) -> None:
        if not Path(self.featured_bin).exists():
            self.skipTest("featured binary not available")

        featured = self._start_featured()
        try:
            proc = self._run_featurectl("health")
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("process lock failed", proc.stderr)
        finally:
            self._stop_featured(featured)

    def test_featured_blocks_featurectl_upsert_same_data_dir(self) -> None:
        if not Path(self.featured_bin).exists():
            self.skipTest("featured binary not available")

        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)

        featured = self._start_featured()
        try:
            proc = self._run_featurectl(
                "upsert",
                "prod",
                "AAPL",
                "f_price",
                str(now),
                "101.5",
                "cli-upsert-lock-test",
            )
            self.assertNotEqual(proc.returncode, 0)
            self.assertIn("process lock failed", proc.stderr)
        finally:
            self._stop_featured(featured)

        snapshot = aapl.get()
        self.assertIn("f_price", snapshot)
        self.assertFalse(snapshot["f_price"].found)

    def test_featured_blocks_python_latest_same_data_dir(self) -> None:
        if not Path(self.featured_bin).exists():
            self.skipTest("featured binary not available")

        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 101.5)

        featured = self._start_featured()
        try:
            with self.assertRaises(RuntimeError) as ctx:
                aapl.latest("f_price")
            self.assertIn("process lock failed", str(ctx.exception))
        finally:
            self._stop_featured(featured)

    def test_featured_blocks_python_upsert_same_data_dir(self) -> None:
        if not Path(self.featured_bin).exists():
            self.skipTest("featured binary not available")

        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)

        featured = self._start_featured()
        try:
            with self.assertRaises(RuntimeError) as ctx:
                aapl.upsert("f_price", now, 101.5)
            self.assertIn("process lock failed", str(ctx.exception))
        finally:
            self._stop_featured(featured)

    def test_second_featured_instance_rejected(self) -> None:
        if not Path(self.featured_bin).exists():
            self.skipTest("featured binary not available")

        first = self._start_featured()
        try:
            second = subprocess.run(
                [self.featured_bin, str(self.config_path)],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(second.returncode, 0)
            self.assertIn("process lock failed", second.stderr)
        finally:
            self._stop_featured(first)

    def test_normal_featurectl_exit_cleans_process_lock(self) -> None:
        proc_one = self._run_featurectl("health")
        self.assertEqual(proc_one.returncode, 0)
        proc_two = self._run_featurectl("health")
        self.assertEqual(proc_two.returncode, 0)
        self.assertFalse((self._data_dir_path() / ".featurectl.process.lock").exists())

    def test_failed_featurectl_command_cleans_process_lock(self) -> None:
        failed = self._run_featurectl("register", "prod", "f_price")
        self.assertNotEqual(failed.returncode, 0)

        healthy = self._run_featurectl("health")
        self.assertEqual(healthy.returncode, 0)
        self.assertFalse((self._data_dir_path() / ".featurectl.process.lock").exists())

    def test_stale_process_lock_has_recovery_path(self) -> None:
        legacy_lock_dir = self._data_dir_path() / ".featurectl.process.lock"
        legacy_lock_dir.mkdir(parents=True, exist_ok=True)
        (legacy_lock_dir / "orphan").write_text("stale")

        proc = self._run_featurectl("health")
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("legacy lock artifact detected", proc.stderr)
        self.assertIn(".featurectl.process.lock", proc.stderr)

    def test_backup_snapshot_excludes_process_lock_artifact(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 101.5)

        backup_dir = self.tmp_path / "backup-lock-filter"
        self.client.backup(str(backup_dir))

        self.assertFalse((backup_dir / "data" / ".featurectl.process.lock").exists())

    def test_restore_output_excludes_process_lock_artifact(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 101.5)

        backup_dir = self.tmp_path / "backup-restore-lock-filter"
        self.client.backup(str(backup_dir))

        legacy_lock_dir = backup_dir / "data" / ".featurectl.process.lock"
        legacy_lock_dir.mkdir(parents=True, exist_ok=True)
        (legacy_lock_dir / "stale").write_text("stale")

        restore = self._run_featurectl("restore", str(backup_dir))
        self.assertEqual(restore.returncode, 0, msg=restore.stderr)
        self.assertFalse((self._data_dir_path() / ".featurectl.process.lock").exists())

    def test_cli_upsert_explicit_write_id_is_retry_idempotent(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)

        first = self._run_featurectl(
            "upsert", "prod", "AAPL", "f_price", str(now), "101.5", "cli-upsert-w1"
        )
        self.assertEqual(first.returncode, 0, msg=first.stderr)
        second = self._run_featurectl(
            "upsert", "prod", "AAPL", "f_price", str(now + 1), "999.0", "cli-upsert-w1"
        )
        self.assertEqual(second.returncode, 0, msg=second.stderr)

        latest = aapl.latest("f_price")
        self.assertTrue(latest.found)
        self.assertEqual(latest.value, 101.5)

    def test_cli_delete_explicit_write_id_is_retry_idempotent(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 101.5)

        first = self._run_featurectl(
            "delete", "prod", "AAPL", "f_price", str(now + 1), "cli-delete-w1"
        )
        self.assertEqual(first.returncode, 0, msg=first.stderr)
        second = self._run_featurectl(
            "delete", "prod", "AAPL", "f_price", str(now + 2), "cli-delete-w1"
        )
        self.assertEqual(second.returncode, 0, msg=second.stderr)

        latest = aapl.latest("f_price")
        self.assertFalse(latest.found)
        history = aapl.get_range("f_price", (now + 10, now))
        self.assertEqual([item.value for item in history], [101.5])

    def test_python_delete_explicit_write_id_is_retry_idempotent(self) -> None:
        self.client.register_feature("prod", "f_price", "double")
        aapl = self.client.entity("prod", "AAPL")
        now = int(time.time() * 1_000_000)
        aapl.upsert("f_price", now, 101.5)

        aapl.delete("f_price", now + 1, write_id="py-delete-w1")
        aapl.delete("f_price", now + 2, write_id="py-delete-w1")

        latest = aapl.latest("f_price")
        self.assertFalse(latest.found)
        history = aapl.get_range("f_price", (now + 10, now))
        self.assertEqual([item.value for item in history], [101.5])

    def test_featurectl_help_matches_readme_usage(self) -> None:
        help_proc = subprocess.run(
            [self.featurectl_bin, "--help"], capture_output=True, text=True
        )
        self.assertEqual(help_proc.returncode, 0)
        help_text = help_proc.stdout + help_proc.stderr

        readme_text = (Path(__file__).resolve().parents[3] / "README.md").read_text()
        upsert_usage = (
            "upsert <namespace> <entity_name> <feature_id> <event_us> <value> [write_id]"
        )
        delete_usage = "delete <namespace> <entity_name> <feature_id> <event_us> [write_id]"
        self.assertIn(upsert_usage, help_text)
        self.assertIn(delete_usage, help_text)
        self.assertIn(upsert_usage, readme_text)
        self.assertIn(delete_usage, readme_text)

    def test_featurectl_missing_config_fails(self) -> None:
        missing = self.tmp_path / "missing.conf"
        proc = subprocess.run(
            [self.featurectl_bin, str(missing), "health"],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("config load failed", proc.stderr)

    def test_featured_missing_config_fails(self) -> None:
        if not Path(self.featured_bin).exists():
            self.skipTest("featured binary not available")
        missing = self.tmp_path / "missing.conf"
        proc = subprocess.run(
            [self.featured_bin, str(missing)],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("config load failed", proc.stderr)

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
