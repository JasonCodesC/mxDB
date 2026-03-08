# Python Packaging and Wheel Bundling

This project ships `mxdb` Python wheels with a bundled native `featurectl` binary.

## Why

`MXDBClient` is CLI-backed in v1, so bundling `featurectl` avoids extra manual setup after `pip install mxdb`.

## Binary Resolution in SDK

`MXDBClient` resolves binaries in this order:

1. explicit `featurectl_bin` argument
2. `MXDB_FEATURECTL_BIN` environment variable
3. bundled wheel binary (`mxdb/bin/featurectl`)
4. `featurectl` on `PATH`

## Local Wheel Build

```bash
python sdk/python/scripts/build_featurectl_for_wheel.py
cd sdk/python
python setup.py bdist_wheel
```

## CI Wheel Build

Workflow: `.github/workflows/python-wheels.yml`

- uses `cibuildwheel`
- builds CPython wheels for macOS/Linux/Windows
- runs `sdk/python/scripts/build_featurectl_for_wheel.py` before wheel build
- validates wheel import and binary resolution
- on `v*` tags, publishes wheels + sdist to PyPI using trusted publishing

## Publish

After CI artifacts are validated, upload wheels and sdist to PyPI.

Example manual flow:

```bash
python -m pip install twine
python -m twine upload wheelhouse/*.whl
python -m twine upload sdk/python/dist/*.tar.gz
```
