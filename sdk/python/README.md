# mxDB Python SDK

`mxdb` is a thin Python SDK that shells out to `featurectl`.

For published wheels, `featurectl` is bundled inside the wheel and resolved automatically by `MXDBClient`.
The wheel build pipeline targets macOS, Linux, and Windows.

## Install

```bash
pip install mxdb
```

## Basic Usage

```python
from mxdb import MXDBClient

client = MXDBClient("featured.conf")

client.register_feature("prod", "instrument", "f_price", "price", "double")
client.register_feature("prod", "instrument", "f_vec", "vec", "double_vector")

client.ingest(
    tenant="prod",
    entity_type="instrument",
    entity_id="AAPL",
    feature_id="f_price",
    event_time_us=100,
    system_time_us=100,
    value=101.5,  # bool/int/float/str/list supported
    write_id="w1",
)
client.ingest(
    tenant="prod",
    entity_type="instrument",
    entity_id="AAPL",
    feature_id="f_vec",
    event_time_us=101,
    system_time_us=101,
    value=[1.0, 2.5, 3.25],
    write_id="w2",
)

latest = client.latest("prod", "instrument", "AAPL", "f_price")
print(latest.value_type, latest.value)  # double 101.5

latest_many = client.latest("prod", "instrument", "AAPL", "f_price", count=5)
print([x.value for x in latest_many])  # latest 5 (or fewer) values

asof = client.asof("prod", "instrument", "AAPL", "f_vec", 200, 200)
print(asof.value_type, asof.value)  # double_vector [1.0, 2.5, 3.25]
```

## Supported Python Value Types

`MXDBClient.ingest()` accepts:

- `bool` -> `bool`
- `int` -> `int64`
- `float` -> `double`
- `str` -> `string`
- `list[float]` -> `float_vector` / `double_vector` (depending on registered metadata type)

Read APIs:

- `latest(..., count=1)` returns one typed result
- `latest(..., count=N)` with `N > 1` returns up to `N` typed results
- `asof(...)` returns one typed result

Legacy `*_double` methods remain available for double-only code paths.

## Binary Resolution Order

`MXDBClient` resolves `featurectl` in this order:

1. explicit `featurectl_bin=` argument
2. `MXDB_FEATURECTL_BIN` environment variable
3. bundled wheel binary (`mxdb/bin/featurectl`)
4. `featurectl` on `PATH`

If none are found, construction fails with a clear error.

## Building Wheels With Bundled `featurectl`

```bash
python sdk/python/scripts/build_featurectl_for_wheel.py
cd sdk/python
python -m build
```
