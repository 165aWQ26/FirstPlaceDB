#!/bin/bash
#

# Default vals
SUPPRESS_WARNINGS=false
RUN_COUNT=1
PYTHON_SCRIPT="testers/__main__.py"

# Parse
while [[ $# -gt 0 ]]; do
    case $1 in
        -s)
            SUPPRESS_WARNINGS=true
            shift
            ;;
        --runs)
            RUN_COUNT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Create venv
if [ -z "$VIRTUAL_ENV" ]; then
    if [ ! -d "$VENV_DIR" ]; then
        echo "Creating virtual environment in $VENV_DIR..."
        python3 -m venv "$VENV_DIR"
    fi

    # Activate it
    source "$VENV_DIR/bin/activate" 
fi

# Set flag if requested
if [ "$SUPPRESS_WARNINGS" = true ]; then
    export RUSTFLAGS="-Awarnings"
fi

# Build the Rust/Python project
uv run maturin develop --release
if [ $? -ne 0 ]; then
    echo "Build failed, aborting."
    exit 1
fi

# Run the Python script the requested number of times
for ((i=1; i<=RUN_COUNT; i++)); do
    echo "Running Script "
    uv run python "$PYTHON_SCRIPT"
done

