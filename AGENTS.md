# Agent Instructions

This document provides instructions for agents working with this codebase.

## Development Environment

To set up the development environment, run:

```bash
make develop
```

This will create a virtual environment and install all necessary
dependencies.

## Rust Toolchain

The Rust version is pinned in `rust-toolchain.toml`. rustup applies it to every
`cargo` invocation, so local checkouts and CI use the same `rustfmt` and
`clippy`.

It is not updated automatically. Bump the `channel` by hand, then re-run the
linters, since a newer toolchain may reformat code or raise new warnings.

## Running Linters

This project uses `prek` to run a suite of linters.
To run the linters on all files, use the following command:

```bash
prek run --all-files
```

To run the Rust linter (`clippy`) specifically, use the following command:

```bash
make lint
```

## Python Bindings

Every `#[pymethods]` function holds the GIL for its whole body. When adding one, wrap
the pure-Rust work in `py.detach(|| ...)` so other Python threads keep running. Only
argument reading and result conversion need the GIL. Keep borrowed values alive past
the closure, so no Arrow release callback runs while the GIL is down.

## Running Tests

To run the Python and Rust tests, use the following command:

```bash
make test
```

This will run the Python unit tests using `pytest` and the Rust tests using
`cargo test`.

## Building the Project

To build the Rust project, use the following command:

```bash
make build
```

This will create a development build of the project. To create a release
build, you can use `make dist`.
