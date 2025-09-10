.PHONY: all
all:
	@echo "Run my targets individually!"

.PHONY: env
env:
	test -d .venv || python3 -m venv .venv
	. .venv/bin/activate && \
		uv sync --all-groups


.PHONY: develop
develop: env protoc
	. .venv/bin/activate && \
		maturin develop

.PHONY: test
test: develop
	. .venv/bin/activate && RUST_BACKTRACE=1 python -m pytest python/test/unit && cargo test

.PHONY: build
build: env
	. .venv/bin/activate && \
		maturin build

.PHONY: dist
dist: env
	. .venv/bin/activate && \
		docker run --rm -v $(shell pwd):/io ghcr.io/pyo3/maturin build --release --strip --out dist


.PHONY: protoc
protoc: env
	. .venv/bin/activate && python scripts/protoc.py

.PHONY: lint
lint:
	cargo clippy -- -D warnings

.PHONY: coverage-env
coverage-env:
	cargo install grcov
	rustup component add llvm-tools

.PHONY: coverage
coverage: develop coverage-env
	. .venv/bin/activate && \
		coverage run --source=python/ptars -m pytest python/test/unit && \
		coverage xml -o coverage.xml
	CARGO_INCREMENTAL=0 RUSTFLAGS="-Cinstrument-coverage" LLVM_PROFILE_FILE="ptars-%p-%m.profraw" cargo test
	grcov . -s . --binary-path ./target/debug/ -t lcov --branch --ignore-not-existing --ignore "target/*" --ignore "python/*" -o lcov.info
