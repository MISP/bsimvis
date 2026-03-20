# BSimVis

BSimVis is a tool to upload large quantities of decompiled binaries from Ghidra to a redis/kvrocks server for analyzing similarity, clustering and diffing functions based on Ghidra BSIM feature vectors.
Binary analysis is done using Ghidra's decompiler thanks to Pyghidra scripting. 

# Features

- Upload decompiled functions and BSIM vectors from Ghidra to a redis/kvrocks server
- API / web interface for :
    - Correlation of decompiled funciton and BSIM features
    - Function diffing based on BSIM features
    - Feature correlation decompiled C tokens


- In the future we plan to add:
    - BSIM vector distance (cosine and others)
    - Function/binary family clustering
    - Upload function/binary families to MISP

# Web UI Diffing

![alt text](img/diffing.png)

# Web UI Feature usage in decompiled functions

![alt text](img/feature_usage.png)

# Requirements

- Ghidra and pyghidra install
- Redis/kvrocks server

# Upload BSIM data from CLI tool

## Usage 

```bash
usage: BSimVis CLI [-h] [-v] -H HOST [-n THREADS] [-t TAG] [-c NAME] [-C FILE] [--va] [--temp-dir DIR] [-p PROFILE]
                   [--min-func-len MIN_FUNC_LEN] [--max-ram-percent MAX_RAM_PERCENT] [--print-flags] [--jvm-args [JVM_ARGS]]
                   [--batch-uuid BATCH_UUID] [--batch-name BATCH_NAME]
                   targets [targets ...]

...

positional arguments:
  targets               Path to Ghidra project (.gpr), a specific binary, or a directory/*

options:
  -h, --help            show this help message and exit
  -v, --verbose         Increase output verbosity (e.g., -v, -vv, -vvv)
  -H, --host HOST       Host address (can be specified multiple times)
  -n, --threads THREADS
                        Number of threads to use (default: 1)
  -t, --tag TAG         Tag to filter by (can be specified multiple times)
  -c, --collection NAME
                        Collections to include (default: 'main' if none provided)
  -C, --config FILE     Config file

Decompilation options:
  --va, --verbose-analysis
                        Verbose logging for analysis step.
  --temp-dir DIR
  -p, --profile PROFILE
                        Profile for ghidra analysis options
  --min-func-len MIN_FUNC_LEN
                        Minimum function length to be considered

JVM Options:
  --max-ram-percent MAX_RAM_PERCENT
                        Set JVM Max Ram % of host RAM
  --print-flags         Print JVM flags at start
  --jvm-args [JVM_ARGS]
                        JVM args to add at start

Batch Options:
  --batch-uuid BATCH_UUID
                        Batch uuid
  --batch-name BATCH_NAME
                        Batch name

...
```

Assuming you have a redis/kvrocks server running on localhost:6667, you can upload data using the following command:

```bash
uv run bsimvis/cli/bsimvis_upload.py <target1> <target2> ... <targetN> --host localhost:6667 -c <collection_name> -t <tag> -n <num_threads> --config <config_file> --profile <profile_name>
```

See bsimvis_config.toml for an example config file (default config file if none is provided)

# API App

```bash
uv run app.py
```

# Build feature inverted Index

```bash
uv run build_index.py
```
