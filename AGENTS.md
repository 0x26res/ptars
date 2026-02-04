# Agent Instructions

This document provides instructions for agents working with this codebase.

## Development Environment

To set up the development environment, run:

```bash
make develop
```

This will create a virtual environment and install all necessary
dependencies.

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
