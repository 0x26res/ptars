# Development

This guide covers how to set up a development environment and contribute to ptars.

## Prerequisites

- Python 3.10+
- Rust (latest stable)
- [uv](https://github.com/astral-sh/uv) for Python dependency management
- [maturin](https://github.com/PyO3/maturin) for building the Rust/Python package

## Setting Up the Development Environment

Most development tasks are available via the Makefile.

### Create Virtual Environment and Install Dependencies

```bash
make develop
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
make test
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
make build
```

Build distribution wheels using Docker:

```bash
make dist
```

## Linting

Run all linters (Rust formatter, clippy, prek):

```bash
make lint
```

## Code Coverage

First, install coverage tools:

```bash
make coverage-env
```

Then run coverage:

```bash
make coverage
```

## Benchmarking

Run benchmarks comparing ptars to protarrow:

```bash
make benchmark
```

!!! note
    Make sure to install the release version for accurate benchmarks.
    The debug build is much slower.

## Releasing

1. Update the version in `Cargo.toml`
2. Update the cargo lock file: `cargo generate-lockfile`
3. Update the version in `pyproject.toml`
4. Update the uv lock file: `uv lock --upgrade`
5. Run prek: `prek autoupdate && prek run --all-files`
6. Tag and push (prepend `v` to the version, e.g., `v0.0.8`)

The CI script for releases is auto-generated:

```bash
make generate-ci
```

## Project Structure

```text
ptars/
├── ptars/              # Core Rust library
├── ptars-python/       # Python bindings (PyO3)
├── python/
│   ├── ptars/          # Python package
│   └── test/           # Python tests
├── protos/             # Protobuf definitions for tests
├── docs/               # Documentation (MkDocs)
└── scripts/            # Build scripts
```

## Resources

- [Blog on developing Python modules in Rust](https://blog.yossarian.net/2020/08/02/Writing-and-publishing-a-python-module-in-rust)
- [PyO3 User Guide](https://pyo3.rs/)
- [Maturin Documentation](https://www.maturin.rs/)
- [Arrow Rust Builders Example](https://github.com/apache/arrow-rs/blob/master/arrow/examples/builders.rs)
