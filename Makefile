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
	. env/bin/activate && \
		maturin develop

.PHONY: test
test: develop
	. env/bin/activate && RUST_BACKTRACE=1 python -m pytest python/test/unit && cargo test

.PHONY: build
build: env
	. env/bin/activate && \
		maturin build

.PHONY: dist
dist: env
	. env/bin/activate && \
		docker run --rm -v $(shell pwd):/io ghcr.io/pyo3/maturin build --release --strip --out dist


.PHONY: protoc
protoc: env
	. env/bin/activate && python scripts/protoc.py
