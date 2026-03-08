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
client.ingest_double(
    tenant="prod",
    entity_type="instrument",
    entity_id="AAPL",
    feature_id="f_price",
    event_time_us=100,
    system_time_us=100,
    value=101.5,
    write_id="w1",
)

latest = client.latest_double("prod", "instrument", "AAPL", "f_price")
print(latest)
```

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
