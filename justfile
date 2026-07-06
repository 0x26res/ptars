# Default recipe
default:
    @echo "Run recipes individually! Use 'just --list' to see available recipes."

# Remove all build artifacts
clean:
    cargo clean
    rm -rf target
    find . -name "*.profraw" | xargs rm -f
    rm -f python/ptars/*.so
    rm -rf ptars_protos
    rm -rf site
    rm -f .coverage
    rm -f coverage.xml
    rm -f lcov.info
    rm -rf .venv

# Create virtual environment and install dependencies
env:
    test -d .venv || uv venv
    . .venv/bin/activate && uv sync --group=test --no-dev

# Build the Python extension in development mode
develop: env protoc
    . .venv/bin/activate && uv run maturin develop

# Run all tests
test: develop
    cargo test && RUST_BACKTRACE=1 uv run python -m pytest python/test/unit

# Build the Python package
build: env
    . .venv/bin/activate && maturin build

# Build distributable wheels via Docker
dist: env
    . .venv/bin/activate && docker run --rm -v $(pwd):/io ghcr.io/pyo3/maturin build --release --strip --out dist

# Generate protobuf Python bindings
protoc: env
    . .venv/bin/activate && python scripts/protoc.py

# Format and lint
lint:
    cargo fmt && cargo clippy --all-targets -- -D warnings && prek run --all-files

# Install coverage tools
coverage-env:
    cargo install grcov
    rustup component add llvm-tools

# Run coverage for both Python and Rust
coverage: develop coverage-env
    uv run coverage run --source=python/ptars -m pytest python/test/unit && \
        uv run coverage xml -o coverage.xml
    CARGO_INCREMENTAL=0 RUSTFLAGS="-Cinstrument-coverage" LLVM_PROFILE_FILE="ptars-%p-%m.profraw" cargo test
    grcov . -s . --binary-path ./target/debug/ -t lcov --branch --ignore-not-existing --ignore "target/*" --ignore "python/*" -o lcov.info
    @echo ""
    @echo "=== Python Coverage Summary ==="
    uv run coverage report
    @echo ""
    @echo "=== Rust Coverage Summary ==="
    grcov . -s . --binary-path ./target/debug/ -t markdown --branch --ignore-not-existing --ignore "target/*" --ignore "python/*" --ignore "/*/.cargo/*" --ignore "/*/.rustup/*"

# Update dependencies
update:
    cargo install cargo-edit
    cargo upgrade
    cargo generate-lockfile
    uv lock --upgrade
    prek autoupdate
    -prek run --all-files
    prek run --all-files
    uv pip compile docs/requirements.txt.in > docs/requirements.txt

# Update dependencies and create a PR branch
update-e2e:
    #!/usr/bin/env bash
    set -euo pipefail
    branch=$(git rev-parse --abbrev-ref HEAD)
    if [ "$branch" != "main" ]; then
        echo "Error: must be on main branch (currently on '$branch')"
        exit 1
    fi
    if [ -n "$(git diff --stat)" ]; then
        echo "Error: working directory has uncommitted changes"
        exit 1
    fi
    just update
    if [ -n "$(git diff --stat)" ]; then
        date=$(date +%Y-%m-%d)
        git checkout -b "update-$date"
        git add -A
        git commit -m "Update $date"
        git push -u origin "update-$date"
    else
        echo "No changes after update"
    fi

# Run benchmarks
benchmark: develop
    maturin develop --release && \
        uv run pytest python/test/benchmark \
            --benchmark-name=short \
            --benchmark-columns=mean \
            --benchmark-sort=name

# Generate CI release workflow
generate-ci: develop
    maturin generate-ci github --output=.github/workflows/release.yaml

# Serve documentation locally
docs:
    uv run --group=docs mkdocs serve

# Build documentation (strict mode)
docs-build:
    uv run --group=docs mkdocs build --strict
