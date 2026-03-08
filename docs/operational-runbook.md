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

## Health

```bash
build/featurectl featured.conf health
```

## Register, Ingest, Query

```bash
build/featurectl featured.conf register-feature prod instrument f_price price double
build/featurectl featured.conf ingest prod instrument AAPL f_price 100 100 101.5 w1
build/featurectl featured.conf latest prod instrument AAPL f_price
build/featurectl featured.conf asof prod instrument AAPL f_price 100 100
```

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
