# ptars

Protobuf to Arrow, using Rust

## Development

### Set up venv

```shell
python3 -m venv --clear env
source env/bin/activate
poetry install
python ./script/protoc.py
```
### Special case for ARM mac

Add this to your bash/rc profile

```shell
export CARGO_BUILD_TARGET=x86_64-apple-darwin
```

```shell
cargo build
maturin develop
pytest test
```


## TODO

- [ ] make singluar primitive generic
- [ ] repeated messages
- [ ] add rust unit tests
- [ ] publish package
- [ ] add configuration for enums
- [ ] maps
- [ ] timestamp, date, wrapped types, duration
- [ ] reuse protarrow tests
- [ ] arrow to proto 

## Resources

- [Blog on how to develop](https://blog.yossarian.net/2020/08/02/Writing-and-publishing-a-python-module-in-rust?utm_source=pocket_saves) and [Corresponding repo](https://github.com/woodruffw/procmaps.py)
- [PyO3 get started](https://pyo3.rs/v0.4.1/) and  [Pyo3 with poetry](https://github.com/nbigaouette/python-poetry-rust-wheel/)
- [Maturin "Mixed Source"](https://www.maturin.rs/#mixed-rustpython-projects)
- [arrow builder example](https://github.com/apache/arrow-rs/blob/master/arrow/examples/builders.rs)
