#!/bin/bash

# Default vals
SUPPRESS_WARNINGS=false
RUN_COUNT=1

# scripts --> runs main a bunch of times
PYTHON_SCRIPTS=("testers/__main__.py" "testers/m1_tester.py")


# Default values
SUPPRESS_WARNINGS=false
RUN_COUNT=1

# Parse cmdline
while [[ $# -gt 0 ]]; do
    case $1 in
        -s|--suppress-warnings)
            SUPPRESS_WARNINGS=true
            shift
            ;;
        --runs)
            RUN_COUNT="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  -s, --suppress-warnings    Suppress warning messages"
            echo "  --runs NUMBER              Number of test runs (default: 1)"
            echo "  -h, --help                 Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use -h or --help for usage information"
            exit 1
            ;;
    esac
done

# Create venv
VENV_DIR=".venv"
if [ -z "$VIRTUAL_ENV" ]; then
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creating virtual environment in $VENV_DIR..."
        python3 -m venv "$VENV_DIR"
    fi
    source "$VENV_DIR/bin/activate"
fi

# Suppress warnings
if [ "$SUPPRESS_WARNINGS" = true ]; then
    export RUSTFLAGS="-Awarnings"
fi

# Build
uv run maturin develop --release
if [ $? -ne 0 ]; then
    echo "Build failed, aborting."
    exit 1
fi

# Run scripts
for script in "${PYTHON_SCRIPTS[@]}"; do
    if [[ "$script" == "testers/__main__.py" ]]; then
        count=$RUN_COUNT
    else
        count=1
    fi

    for ((i=1; i<=count; i++)); do
        uv run python "$script"
    done
done
