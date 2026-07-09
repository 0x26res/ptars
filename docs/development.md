# Development

This guide covers how to set up a development environment and contribute to ptars.

## Prerequisites

- Python 3.10+
- Rust (latest stable)
- [just](https://github.com/casey/just) as a command runner
- [uv](https://github.com/astral-sh/uv) for Python dependency management
- [maturin](https://github.com/PyO3/maturin) for building the Rust/Python package

## Setting Up the Development Environment

Most development tasks are available via the [justfile](https://github.com/casey/just).

### Create Virtual Environment and Install Dependencies

```bash
just develop
```

This will:

1. Create a virtual environment in `.venv`
2. Install dependencies using uv
3. Compile the protobuf files
4. Build the Rust extension with maturin

### Special Case for ARM Mac

Add this to your `.bashrc` or `.zshrc`:

```bash
export CARGO_BUILD_TARGET=aarch64-apple-darwin
```

## Running Tests

Run both Python and Rust tests:

```bash
just test
```

Or run them separately:

```bash
# Python tests only
. .venv/bin/activate
pytest python/test

# Rust tests only
cargo test
```

## Building

Build the package locally:

```bash
just build
```

Build distribution wheels using Docker:

```bash
just dist
```

## Linting

Run all linters (Rust formatter, clippy, prek):

```bash
just lint
```

## Code Coverage

First, install coverage tools:

```bash
just coverage-env
```

Then run coverage:

```bash
just coverage
```

## Benchmarking

Run benchmarks comparing ptars to protarrow:

```bash
just benchmark
```

!!! note
    Make sure to install the release version for accurate benchmarks.
    The debug build is much slower.

## Releasing

Create a release from a new tag in Github. The CI takes care of the rest.

## CI

The CI script for releases is auto-generated, though it had to be customized.

```bash
just generate-ci
```

## Project Structure

```text
ptars/
├── ptars-core/         # Core Rust library (arrow-rs-typed API, pins an arrow major version)
├── ptars/              # Stable Rust facade over ptars-core (Arrow C Data Interface only,
│                       # works with any arrow version on the consumer's side)
├── ptars-python/       # Python bindings (PyO3, depends on ptars-core)
├── python/
│   ├── ptars/          # Python package
│   └── test/           # Python tests
├── tests/
│   └── arrow-version-independence/  # Consumer crate pinned to a different arrow
│                                    # major version; proves ptars is version-independent
├── protos/             # Protobuf definitions for tests
├── docs/               # Documentation (MkDocs)
└── scripts/            # Build scripts
```

### Rust crate hierarchy

Two crates are published to crates.io:

- **`ptars-core`** contains the actual implementation. Its API exposes arrow-rs
  and prost-reflect types, so consumers must use the same major versions as
  ptars-core does.
- **`ptars`** is a thin facade over ptars-core (pinned with an exact `=`
  version). Its public API contains no arrow or prost types: Arrow data is
  exchanged zero-copy through the Arrow C Data Interface, and protobuf
  descriptors/messages are passed as serialized bytes. Consumers can therefore
  combine it with any arrow version.

Two CI safeguards keep the `ptars` API version-independent: the
`tests/arrow-version-independence` crate is built against a different arrow
major version, and a `cargo public-api` check fails if any `arrow*::` or
`prost*::` path appears in the public API.

## Resources

- [Blog on developing Python modules in Rust](https://blog.yossarian.net/2020/08/02/Writing-and-publishing-a-python-module-in-rust)
- [PyO3 User Guide](https://pyo3.rs/)
- [Maturin Documentation](https://www.maturin.rs/)
- [Arrow Rust Builders Example](https://github.com/apache/arrow-rs/blob/master/arrow/examples/builders.rs)
