# Contributing

## Build

```bash
cmake -S . -B build
cmake --build build -j8
```

## Test

```bash
ctest --test-dir build --output-on-failure
```

## Development Rules

- Follow `AGENT.md` authority order and milestone sequence.
- Keep docs and backlog updated with code changes.
- Add tests with every subsystem-level change.
