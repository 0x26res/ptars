# protarrowrs

Convert arrow to proto and back fast

## Development

### Set up venv

```shell
python3 -m venv --clear .env
source .env/bin/activate
pip install -U pip setuptools maturin pyarrow
```

```shell
cargo build
cargo rustc --release -- -C link-arg=-undefined -C link-arg=dynamic_lookup
# Or:
CARGO_BUILD_TARGET=x86_64-apple-darwin cargo build
cargo build --target=x86_64-apple-darwin
maturin develop
python -c "import protarrowrs;print(protarrowrs.get_a_table())"
```


## Resources

- [Blog on how to develop](https://blog.yossarian.net/2020/08/02/Writing-and-publishing-a-python-module-in-rust?utm_source=pocket_saves)
- [PyO3 get started](https://pyo3.rs/v0.4.1/)