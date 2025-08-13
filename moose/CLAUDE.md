# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This repository contains **MooseLSM**, a flexible LSM-tree key-value store developed based on RocksDB. The project removes the constraint of fixed size ratios between consecutive levels and allows users to set arbitrary capacities for each level and number of runs. This enables implementation of various academic compaction policies (Tiering, Dostoevsky, Wacky, etc.).

The main innovation is the removal of structural constraints in LSM-trees:
- Arbitrary level capacities via `options.level_capacities`
- Multiple sorted runs per level via `options.run_numbers`
- Support for optimal tradeoff configurations as described in the SIGMOD 2024 paper

## Build System

### Main Build Process
```bash
# Build the MooseLSM library
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make install

# Alternative build with benchmarks and tools
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_BENCHMARK_TOOLS=ON -DWITH_CORE_TOOLS=ON ..
make -j
```

### Dependencies
- **Required**: CMake 3.10+, C++17 compliant compiler (GCC >= 7, Clang >= 5), gflags
- **Optional**: gtest (for testing), JeMalloc, Snappy, LZ4, Zlib, Zstd
- **Platform**: Linux/Unix (Windows support with Visual Studio 2019+)

### Build Configuration Options
- `WITH_JEMALLOC=ON/OFF` - Use JeMalloc allocator
- `WITH_BENCHMARK_TOOLS=ON/OFF` - Build db_bench and other benchmarks
- `WITH_TESTS=ON/OFF` - Build test suite (default ON in Debug)
- `WITH_CORE_TOOLS=ON/OFF` - Build ldb and sst_dump tools
- `ROCKSDB_BUILD_SHARED=ON/OFF` - Build shared libraries

## Architecture

### Core Components

**MooseLSM Library** (`MooseLSM/`):
- Based on RocksDB with custom compaction policies
- Custom compaction picker in `db/compaction/compaction_picker_moose.cc`
- Flexible LSM structure with configurable level capacities and run numbers
- Support for various compaction strategies (Leveling, Tiering, Hybrid)

**Key Source Files**:
- `db/compaction/compaction_picker_moose.h/cc` - Custom Moose compaction logic
- `include/rocksdb/options.h` - Extended options for level_capacities and run_numbers
- `tools/db_bench.cc` - Primary benchmarking tool

### Programming Interface
```cpp
#include "rocksdb/db.h"
#include "rocksdb/options.h"

rocksdb::Options options;
options.level_capacities = {2097152, 4893354, 34253482, ...}; // Level capacities in bytes
options.run_numbers = {1, 3, 2, ...}; // Number of runs per level
auto status = rocksdb::Open(options, "/path/to/db", &db);
```

## Common Development Commands

### Building and Running Benchmarks
```bash
# Build benchmark tools
cmake -DCMAKE_BUILD_TYPE=Release -DWITH_BENCHMARK_TOOLS=ON ..
make db_bench

# Run basic benchmark with MooseLSM configuration
./build/db_bench -benchmarks=fillrandom,readrandom \
  -level_capacities="2097152,4893354,34253482" \
  -run_numbers="1,3,2" \
  -compaction_style=4 \
  -num=1000000

# Run the provided benchmark script
./moose_bench.sh
```

### Testing
```bash
# Build and run tests
cmake -DCMAKE_BUILD_TYPE=Debug -DWITH_TESTS=ON ..
make -j
make check

# Run specific test
./build/db_basic_test
```

### Using Core Tools
```bash
# Build LDB tool for database inspection
make ldb

# Inspect database contents
./build/ldb scan --db=/path/to/db

# Dump SST file information
./build/sst_dump --file=/path/to/file.sst --command=scan
```

## Benchmarking and Performance

### Key Benchmark Parameters
- **Compaction Style**: Use `-compaction_style=4` for MooseLSM's custom compaction
- **Level Configuration**: Set via `-level_capacities` and `-run_numbers` parameters
- **Workload Types**: fillrandom, readrandom, overwrite, seekrandomwhilewriting
- **Value/Key Sizes**: Configurable via `-value_size` and `-key_size`

### Sample Benchmark Commands
```bash
# Write-heavy workload with custom LSM structure
./build/db_bench -benchmarks=overwrite -write_buffer_size=2097152 \
  -level_capacities="2097152,4893354,4893354,34253482" \
  -run_numbers="1,3,3,2" -compaction_style=4 \
  -num=11000000 -value_size=1000

# Read workload with background writes
./build/db_bench -benchmarks=seekrandomwhilewriting \
  -use_existing_db=true -num=1000000
```

## Configuration Guidelines

### LSM Structure Design
- **Level Capacities**: Define the maximum size for each level in bytes
- **Run Numbers**: Set how many sorted runs each level can contain
- **Compaction Style**: Always use `compaction_style=4` for MooseLSM features

### Memory and Performance Tuning
- `write_buffer_size` - Controls memtable size (default: 2MB)
- `level0_slowdown_writes_trigger` - Slowdown threshold for L0 files
- `level0_stop_writes_trigger` - Stop writes threshold for L0 files
- Use `compression_type=none` for pure performance testing

## Front-end Web Interface

The repository includes a basic web frontend in `front-end/` with:
- Docker configuration for deployment
- Python server (`server.py`) for database interaction
- TypeScript/React components (in `front-end/moose/`)
- YAML configuration (`moose_calculator.yaml`)

## Testing and Validation

### Memory Testing
The build system includes several sanitizer options:
- `WITH_ASAN=ON` - AddressSanitizer for memory errors
- `WITH_TSAN=ON` - ThreadSanitizer for race conditions  
- `WITH_UBSAN=ON` - UndefinedBehaviorSanitizer

### Continuous Integration
- Tests are excluded from Release builds by default
- Use `WITH_ALL_TESTS=ON` to build complete test suite
- Automated test discovery with Google Test framework

## Important Implementation Notes

### Custom Compaction Logic
MooseLSM's core innovation lies in `compaction_picker_moose.cc`, which implements flexible compaction policies based on the level_capacities and run_numbers configuration.

### Thread Safety and Concurrency
- Inherits RocksDB's thread-safety guarantees
- Optimistic concurrency control for reads
- Write operations are serialized through write threads

### Platform Compatibility
- Primary development on Linux x86_64
- Windows support through Visual Studio and MinGW
- ARM64/AArch64 support with optimized CRC32 instructions
- PowerPC and s390x architecture support