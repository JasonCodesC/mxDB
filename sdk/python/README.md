# mxDB Python SDK

`mxdb` is a thin Python SDK that shells out to `featurectl`.

For published wheels, `featurectl` is bundled inside the wheel and resolved automatically by `MXDBClient`.
The wheel build pipeline targets macOS, Linux, and Windows.

Public SDK usage is entity-scoped: bind an entity with
`client.entity(tenant, entity_type, entity_id)` and call reads/writes on the
returned `MXDBEntityClient`.

## Install

```bash
pip install mxdb
```

No local compiler/toolchain is required for standard wheel installs.

## Basic Usage

```python
from mxdb import MXDBClient

client = MXDBClient("featured.conf")

client.register_feature("prod", "instrument", "f_price", "price", "double")
client.register_feature("prod", "instrument", "f_vec", "vec", "double_vector")

# Bind the entity once, then use short calls.
aapl = client.entity("prod", "instrument", "AAPL")

aapl.ingest("f_price", 100, 101.5, "w1", system_time_us=100)
aapl.ingest("f_vec", 101, [1.0, 2.5, 3.25], "w2", system_time_us=101)

latest = aapl.latest("f_price")
print(latest.value_type, latest.value)  # double 101.5

latest_many = aapl.latest("f_price", count=5)
print([x.value for x in latest_many])  # latest 5 (or fewer) values

snapshot = aapl.get()
print(snapshot["f_price"].value)  # 101.5

asof = aapl.asof("f_vec", 200, 200)
print(asof.value_type, asof.value)  # double_vector [1.0, 2.5, 3.25]

# Range form: latest value within [start, end] (inclusive).
asof_range = aapl.asof(
    "f_price", ("2026:03:09:12:00:00.000", "2026:03:09:12:10:00.000")
)
print(asof_range.value)

aapl.delete("f_price", 300, "w-delete-1", system_time_us=300)
```

## Supported Python Value Types

`MXDBEntityClient.ingest()` accepts:

- `bool` -> `bool`
- `int` -> `int64`
- `float` -> `double`
- `str` -> `string`
- `list[float]` -> `float_vector` / `double_vector` (depending on registered metadata type)

Read APIs:

- `client.entity(tenant, entity_type, entity_id)` returns an entity-scoped client
- `entity.latest(..., count=1)` returns one typed result
- `entity.latest(..., count=N)` with `N > 1` returns up to `N` typed results
- `entity.get()` returns all registered features for that entity key
- `entity.asof(...)` returns one typed result

Double-only helpers remain available on the entity client: `entity.latest_double(...)` and `entity.asof_double(...)`.

Write APIs:

- `entity.ingest(..., operation="upsert"|"delete")`
- `entity.delete(...)` convenience wrapper for tombstone writes

## Return Objects

`entity.latest()` and `entity.asof()` return `TypedFeatureResult` objects with:

- `found: bool`
- `value_type: str | None`
- `value: Any | None`
- `event_time_us: int | None`
- `system_time_us: int | None`
- `lsn: int | None` (`latest()` paths include `lsn`; `asof()` is `None`)

When `count > 1`, `entity.latest()` returns `list[TypedFeatureResult]` in newest-first order.
`entity.get()` returns `dict[str, TypedFeatureResult]` keyed by `feature_id`.

Delete/latest contract:

- if the latest visible event is a tombstone, `entity.latest(..., count=1)` returns `found=False`
- `entity.latest(..., count>1)` returns `[]` when the latest visible event is a tombstone

## Binary Resolution Order

`MXDBClient` resolves `featurectl` in this order:

1. explicit `featurectl_bin=` argument
2. `MXDB_FEATURECTL_BIN` environment variable
3. bundled wheel binary payload (`mxdb/bin/featurectl` or `mxdb/bin/featurectl.exe`)
4. `featurectl` on `PATH`

If none are found, construction fails with a clear error.

## `asof` Time Inputs

`event_cutoff_us` / `system_cutoff_us` accept:

- epoch microseconds (`int`)
- `datetime` objects
- strings in `YYYY:MM:DD:HH:MM:SS[.ffffff]`
- ISO-8601 strings (for example `2026-03-09T12:34:56Z`)
- ranges as `(start, end)` or `[start, end]` using any supported time input
  above

`system_cutoff_us` is optional; if omitted, it defaults to current time.
For range inputs, `entity.asof(...)` returns the latest visible value in the closed interval.
Millisecond strings work via fractional seconds (for example `.123`).

## Building Wheels With Bundled `featurectl`

```bash
python sdk/python/scripts/build_featurectl_for_wheel.py
cd sdk/python
python -m build
```

For source builds on Windows, install SQLite via vcpkg and pass the toolchain:

```powershell
python sdk/python/scripts/install_windows_sqlite.py
$env:MXDB_CMAKE_TOOLCHAIN_FILE="C:/vcpkg/scripts/buildsystems/vcpkg.cmake"
$env:MXDB_VCPKG_TARGET_TRIPLET="x64-windows"
python sdk/python/scripts/build_featurectl_for_wheel.py
```
