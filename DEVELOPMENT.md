# Development

Most of the development tasks are expressed in the [Makefile](./Makefile)

## Set up venv

```shell
make develop
```

### Special case for ARM mac

Add this to your bash/rc profile:

```shell
export CARGO_BUILD_TARGET=aarch64-apple-darwin
```

## Testing

```shell
cargo build && maturin develop && RUST_BACKTRACE=1 pytest python/test
```

## Releasing

### Using bump-my-version (recommended)

The repository uses tag to drive version.

Effectively, the CI reads the version from the tag, replace it in the [./Cargo.toml](./Cargo.toml).
Maturin picks it up from there and passes it to the python code.

To make a new release, just push a tag eg `v0.0.10`.
For release candidates use `v0.0.10-rc0`.

## Benchmarking

Make sure to install the release version of ptars.
The locally built version is much slower.

```shell
make benchmark
```

## TODO

- [ ] add configuration for enums, timestamp, date, wrapped types, duration

## Resources

- [Blog on how to develop](https://blog.yossarian.net/2020/08/02/Writing-and-publishing-a-python-module-in-rust?utm_source=pocket_saves)
  and [Corresponding repo](https://github.com/woodruffw/procmaps.py)
- [PyO3 get started](https://pyo3.rs/v0.4.1/) and  [Pyo3 with poetry](https://github.com/nbigaouette/python-poetry-rust-wheel/)
- [Maturin "Mixed Source"](https://www.maturin.rs/#mixed-rustpython-projects)
- [arrow builder example](https://github.com/apache/arrow-rs/blob/master/arrow/examples/builders.rs)
