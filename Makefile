.PHONY: all
all:
	@echo "Run my targets individually!"

.PHONY: env
env:
	test -d .venv || python3 -m venv .venv
	. .venv/bin/activate && \
		uv sync --all-groups


.PHONY: develop
develop: env
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
