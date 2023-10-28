# protarrowrs

Convert arrow to proto and back fast

## Development

### Set up venv

```shell
python3 -m venv --clear .env
source .env/bin/activate
pip install -U pip setuptools maturin pyarrow
```
### Special case for ARM mac

Add this to your bash/rc profile

```shell
export CARGO_BUILD_TARGET=x86_64-apple-darwin
```

```shell
cargo build
maturin develop
python -c "import protarrowrs;print(protarrowrs.get_a_table())"
python -c "import protarrowrslib;print(protarrowrslib.get_a_table())"
```


## Resources

- [Blog on how to develop](https://blog.yossarian.net/2020/08/02/Writing-and-publishing-a-python-module-in-rust?utm_source=pocket_saves)
- [Corresponding repo](https://github.com/woodruffw/procmaps.py)
- [PyO3 get started](https://pyo3.rs/v0.4.1/)
- [Pyo3 with poetry](https://github.com/nbigaouette/python-poetry-rust-wheel/)
- [Maturin "Mixed Source"](https://www.maturin.rs/#mixed-rustpython-projects)