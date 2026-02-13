# FirstPlaceDB

### Build the binding
git clone https://github.com/165aWQ26/FirstPlaceDB.git
cd FirstPlaceDB
pip install uv
uv sync
uv run maturin develop --release
## Run the code
uv run python NAME_OF THE TEST FILE

## Build

This compiles the Rust code and installs the `lstore` package into your virtual environment.

## Run testers

With the virtual environment activated:

```bash
python -m testers
```

Individual milestone testers:

```bash
python testers/m1_tester.py
python testers/m2_tester_part1.py
python testers/m2_tester_part2.py
python testers/m3_tester_part_1.py
python testers/m3_tester_part_2.py
```
