#!/bin/bash
# Default values
SUPPRESS_WARNINGS=false
RUN_COUNT=1
USE_RELEASE=true
# Parse cmdline
while [[ $# -gt 0 ]]; do
	case $1 in
	-s | --suppress-warnings)
		SUPPRESS_WARNINGS=true
		shift
		;;
	--runs | -r)
		RUN_COUNT="$2"
		shift 2
		;;
	--no-release)
		USE_RELEASE=false
		shift
		;;
	-h | --help)
		echo "Usage: $0 [OPTIONS]"
		echo "Options:"
		echo "  -s, --suppress-warnings    Suppress warning messages"
		echo "  -r, --runs NUMBER              Number of test runs (default: 1)"
		echo "  --no-release               Disable release mode build"
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

check_error() {
	if [ $? -ne 0 ]; then
		echo "Build failed, aborting."
		exit 1
	fi
}

clean_data_dirs() {
	rm -rf db_data/Grades/ db_data/.ECS165/
}

clean_m2_extended_dir() {
	rm -rf db_data/.M2/ db_data/.MT/ db_data/.CT/
}

# Suppress warnings
if [ "$SUPPRESS_WARNINGS" = true ]; then
	export RUSTFLAGS="-Awarnings"
fi
export RUST_BACKTRACE=full
export RUST_LIB_BACKTRACE=1

#
if [ "$USE_RELEASE" = true ]; then
	uv run maturin develop --release
	check_error
	echo "Running in release mode"
else
	uv run maturin develop
	check_error
	echo "Running in debug mode.. Unoptimized"
fi

for ((i = 1; i <= RUN_COUNT; i++)); do
	echo "========================================"
	echo "  Run $i / $RUN_COUNT"
	echo "========================================"

	## __main__
	clean_data_dirs
	uv run python "testers/__main__.py"

	## m1_tester
	clean_data_dirs
	uv run python "testers/m1_tester.py"

	## m1 new
	clean_data_dirs
	uv run python "testers/m1_tester_new.py"

	## m2
	clean_data_dirs
	uv run python "testers/m2_tester_part1.py"
	uv run python "testers/m2_tester_part2.py"

	## m2 new
	clean_data_dirs
	uv run python "testers/m2_tester_part1_new.py"
	uv run python "testers/m2_tester_part2_new.py"

	## m2 extended
	clean_data_dirs
	uv run python "testers/m2_extended.py"
	clean_m2_extended_dir

	## m3
	clean_data_dirs
	uv run python "testers/m3_tester_part_1.py"
	uv run python "testers/m3_tester_part_2.py"

	## m3 new
	clean_data_dirs
	uv run python "testers/m3_tester_part_1new.py"
	uv run python "testers/m3_tester_part_2new.py"

	clean_data_dirs
done
