# mxDB Python SDK

`mxdb` is a thin Python SDK that shells out to `featurectl`.

For published wheels, `featurectl` is bundled inside the wheel and resolved
automatically by `MXDBClient`.

## Install

```bash
pip install mxdb
```

## API Shape

The public API is namespace + entity-centric:

- `client.register_feature(namespace, feature_name, value_type)`
- `row = client.entity(namespace, entity_name)`
- `row.upsert(feature_id, event_time, value)`
- `row.delete(feature_id, event_time)`
- `row.latest(feature_id, count=...)`
- `row.get()`
- `row.get_range(feature_id, date_range, disk=True)`

`system_time` and `write_id` are intentionally hidden in Python.

Equivalent `featurectl` commands used under the hood:

- `register <namespace> <feature_name> <value_type>`
- `upsert <namespace> <entity_name> <feature_id> <event_us> <value>`
- `delete <namespace> <entity_name> <feature_id> <event_us>`
- `get <namespace> <entity_name>`
- `latest <namespace> <entity_name> <feature_id> [count]`
- `range <namespace> <entity_name> <feature_id> <furthest> [latest] [disk|memory]`

## `get_range` Semantics

`row.get_range(feature_id, date_range, disk=True)` returns a newest-first list of
typed values for one feature.

- `date_range=(latest, furthest)`:
  returns values with `event_time` in `[furthest, latest]`
- `date_range=furthest`:
  returns everything after `furthest` (inclusive)
- `disk=True`:
  include values from memory + immutable segments
- `disk=False`:
  include only values currently in memory

`date_range` accepts epoch micros, epoch seconds (`float`), `datetime`,
`YYYY:MM:DD:HH:MM:SS[.ffffff]`, and ISO-8601 strings.

## Full Example

```python
from datetime import datetime, timezone, timedelta

from mxdb import MXDBClient

client = MXDBClient("featured.conf")

# 1) Register schema in namespace "quant"
client.register_feature("quant", "f_price", "double")
client.register_feature("quant", "f_flag", "bool")
client.register_feature("quant", "f_note", "string")
client.register_feature("quant", "f_vec", "double_vector")
client.register_feature("quant", "f_size", "int64")

# 2) Bind one entity key (row/index)
aapl = client.entity("quant", "AAPL")

# 3) Write values
base = datetime(2026, 3, 9, 12, 0, 0, tzinfo=timezone.utc)
aapl.upsert("f_price", base, 101.5)
aapl.upsert("f_price", base + timedelta(seconds=5), 102.0)
aapl.upsert("f_flag", base + timedelta(seconds=1), True)
aapl.upsert("f_note", base + timedelta(seconds=2), 'quote " ok')
aapl.upsert("f_vec", base + timedelta(seconds=3), [1.0, 2.5, 3.25])
aapl.upsert("f_size", base + timedelta(seconds=4), 1_500_000)

# 4) Latest reads
latest_price = aapl.latest("f_price")
latest_price_history = aapl.latest("f_price", count=5)
latest_price_typed = aapl.latest_double("f_price")

# 5) Get all registered features for this entity
snapshot = aapl.get()

# 6) Range reads (latest->furthest)
bounded = aapl.get_range(
    "f_price",
    ("2026:03:09:12:00:05.000", "2026:03:09:12:00:00.000"),
)
# latest omitted => everything after furthest
open_ended = aapl.get_range("f_price", "2026:03:09:12:00:00")
# memory-only view
memory_only = aapl.get_range("f_price", (base + timedelta(seconds=10), base), disk=False)
# ISO-8601 timestamps are also supported
iso_range = aapl.get_range("f_price", ("2026-03-09T12:00:05Z", base))

# 7) Delete latest value (tombstone)
aapl.delete("f_price", base + timedelta(seconds=10))
after_delete_latest = aapl.latest("f_price")         # found=False
after_delete_history = aapl.latest("f_price", 5)     # []

print(latest_price)
print(latest_price_history)
print(latest_price_typed)
print(snapshot)
print(bounded)
print(open_ended)
print(memory_only)
print(iso_range)
print(after_delete_latest)
print(after_delete_history)
```

## Additional Example: Multi-Entity Reads

```python
from mxdb import MXDBClient

client = MXDBClient("featured.conf")
client.register_feature("quant", "f_price", "double")

for symbol, px in [("AAPL", 101.5), ("MSFT", 402.25), ("NVDA", 950.0)]:
    row = client.entity("quant", symbol)
    row.upsert("f_price", 1_700_000_000_000_000, px)

snapshots = {
    symbol: client.entity("quant", symbol).get()["f_price"].value
    for symbol in ("AAPL", "MSFT", "NVDA")
}
print(snapshots)
```

## Value Types

`row.upsert(...)` accepts:

- `bool` -> `bool`
- `int` -> `int64`
- `float` -> `double`
- `str` -> `string`
- `list[float]` -> `float_vector` / `double_vector` (based on feature metadata)

## Return Types

`row.latest(..., count=1)` returns `TypedFeatureResult`.
`row.latest(..., count>1)` and `row.get_range(...)` return
`list[TypedFeatureResult]` in newest-first order.
`row.get()` returns `dict[str, TypedFeatureResult]`.

`TypedFeatureResult` fields:

- `found: bool`
- `value_type: str | None`
- `value: Any | None`
- `event_time_us: int | None`
- `system_time_us: int | None`
- `lsn: int | None`

## Client Methods

`MXDBClient` also exposes:

- `checkpoint()`
- `compact()`
- `set_read_only(enabled: bool)`
- `backup(destination_dir: str)`

## Binary Resolution Order

`MXDBClient` resolves `featurectl` in this order:

1. explicit `featurectl_bin=` argument
2. `MXDB_FEATURECTL_BIN` environment variable
3. bundled wheel binary payload (`mxdb/bin/featurectl` or `mxdb/bin/featurectl.exe`)
4. `featurectl` on `PATH`

If none are found, construction fails with a clear error.
