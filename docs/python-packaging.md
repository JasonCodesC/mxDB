# Python Packaging and Wheel Bundling

This project ships `mxdb` Python wheels with a bundled native `featurectl` binary.

## Why

`MXDBClient` is CLI-backed in v1, so bundling `featurectl` avoids extra manual setup after `pip install mxdb`.

## Binary Resolution in SDK

`MXDBClient` resolves binaries in this order:

1. explicit `featurectl_bin` argument
2. `MXDB_FEATURECTL_BIN` environment variable
3. bundled wheel binary payload (`mxdb/bin/featurectl` or `mxdb/bin/featurectl.exe`)
4. `featurectl` on `PATH`

Public SDK read/write usage is entity-scoped through
`MXDBClient.entity(tenant, entity_type, entity_id)`.

## Local Wheel Build

```bash
python sdk/python/scripts/build_featurectl_for_wheel.py
cd sdk/python
python -m build --wheel
```

Windows source-build prerequisites (for local wheel builds):

```powershell
python sdk/python/scripts/install_windows_sqlite.py
$env:MXDB_CMAKE_TOOLCHAIN_FILE="C:/vcpkg/scripts/buildsystems/vcpkg.cmake"
$env:MXDB_VCPKG_TARGET_TRIPLET="x64-windows"
python sdk/python/scripts/build_featurectl_for_wheel.py
cd sdk/python
python -m build --wheel
```

## CI Wheel Build

Workflow: `.github/workflows/python-wheels.yml`

- uses `cibuildwheel`
- builds CPython wheels for macOS/Linux/Windows
- runs `sdk/python/scripts/build_featurectl_for_wheel.py` before wheel build
- runs `sdk/python/scripts/repair_linux_wheel.py` as the Linux repair hook:
  it uses `auditwheel` first and falls back to explicit manylinux retagging when
  the wheel only bundles a standalone executable payload
- validates wheel import and binary resolution
- on `v*` tags, publishes wheels + sdist to PyPI using trusted publishing
- enforces a publish-time guard that fails if any wheel still has `-linux_` tags
- wheel build step is retried automatically for transient upstream rate limits/errors

## Publish

After CI artifacts are validated, upload wheels and sdist to PyPI.

Example manual flow:

```bash
python -m pip install twine
python -m twine upload wheelhouse/*.whl
python -m twine upload sdk/python/dist/*.tar.gz
```

For trusted publishing in CI, prefer tag-driven release (`v*`) and let
`.github/workflows/python-wheels.yml` publish from downloaded artifacts.
