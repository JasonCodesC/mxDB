# Latest Read Workload

- Entities: 1,000
- Writes: 10,000
- Reads: 10,000
- Distribution: uniform random by entity
- Query shape: single entity + single feature latest lookup

Use:

```bash
PYTHONPATH=sdk/python/src python3 tools/benchmark_runner.py \
  --config deploy/config/featured.conf.example \
  --featurectl-bin build/featurectl
```
