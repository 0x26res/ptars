.PHONY: all
all:
	@echo "Run my targets individually!"

.PHONY: clean
clean:
	cargo clean && rm -rf target

.PHONY: env
env:
	test -d .venv || python3 -m venv .venv
	. .venv/bin/activate && \
		uv sync --group=test --no-dev


.PHONY: develop
develop: env protoc
	. .venv/bin/activate && \
		uv run maturin develop

.PHONY: test
test: develop
	RUST_BACKTRACE=1 uv run python -m pytest python/test/unit && cargo test

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
	cargo fmt && \
		cargo clippy -- -D warnings && \
		pre-commit run --all-files

.PHONY: coverage-env
coverage-env:
	cargo install grcov
	rustup component add llvm-tools

.PHONY: coverage
coverage: develop coverage-env
	uv run coverage run --source=python/ptars -m pytest python/test/unit && \
		uv run xml -o coverage.xml
	CARGO_INCREMENTAL=0 RUSTFLAGS="-Cinstrument-coverage" LLVM_PROFILE_FILE="ptars-%p-%m.profraw" cargo test
	grcov . -s . --binary-path ./target/debug/ -t lcov --branch --ignore-not-existing --ignore "target/*" --ignore "python/*" -o lcov.info

.PHONY: update
update:
	cargo generate-lockfile && \
		uv lock --upgrade && \
		pre-commit autoupdate && pre-commit run --all-files && \
		uv pip compile docs/requirements.txt.in > docs/requirements.txt

.PHONY: benchmark
benchmark: develop
	maturin develop --release && \
    	uv run pytest python/test/benchmark \
		  --benchmark-name=short \
		  --benchmark-columns=mean \
		  --benchmark-sort=name

.PHONY: generate-ci
generate-ci: develop
	maturin generate-ci github --output=.github/workflows/release.yaml

.PHONY: docs
docs:
	uv run --group=docs mkdocs serve

.PHONY: docs-build
docs-build:
	uv run --group=docs mkdocs build --strict
