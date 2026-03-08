# FirstPlaceDB

### Build the bindings

This compiles the Rust code and installs the `lstore` package into your virtual environment.
```bash
git clone https://github.com/165aWQ26/FirstPlaceDB.git
cd FirstPlaceDB
pip install uv
uv sync
uv run maturin develop --release
```

## Run the code
```bash
uv run python testers/<TEST_FILE>
```

Individual milestone testers:

```bash
__main__.py
m1_tester.py
m1_tester_new.py
m2_extended.py
m2_tester_part1.py
m2_tester_part1_new.py
m2_tester_part2.py
m2_tester_part2_new.py
m3_tester_part_1.py
m3_tester_part_1new.py
m3_tester_part_2.py
m3_tester_part_2new.py
```
