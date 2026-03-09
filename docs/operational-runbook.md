# Operational Runbook

## Build

```bash
cmake -S . -B build
cmake --build build -j8
ctest --test-dir build --output-on-failure
```

## Start Engine

```bash
cp deploy/config/featured.conf.example featured.conf
build/featured featured.conf
```

`featured` is a long-running process.

For now, do not run `featurectl` commands against the same `data_dir` while a
`featured` process is running. The v1 transport surface is CLI-based and there
is no network API/multiprocess coordination layer yet.

## Health

```bash
build/featurectl featured.conf health
```

## Register, Ingest, Query

```bash
build/featurectl featured.conf register-feature prod instrument f_price price double
build/featurectl featured.conf register-feature prod instrument f_flag flag bool
build/featurectl featured.conf ingest prod instrument AAPL f_price 100 100 101.5 w1
build/featurectl featured.conf ingest prod instrument AAPL f_flag 101 101 true w2
build/featurectl featured.conf latest prod instrument AAPL f_price
build/featurectl featured.conf latest prod instrument AAPL f_price 5
build/featurectl featured.conf asof prod instrument AAPL f_price 100 100
```

Supported CLI value types:
`double`, `int64`, `string`, `bool`, `float_vector`, `double_vector`.

## Checkpoint and Compaction

```bash
build/featurectl featured.conf checkpoint
build/featurectl featured.conf compact
```

## Backup and Restore

```bash
build/featurectl featured.conf backup /tmp/mxdb-backup
build/featurectl featured.conf restore /tmp/mxdb-backup readonly
```

## Read-Only Mode

```bash
build/featurectl featured.conf readonly on
build/featurectl featured.conf readonly off
```
