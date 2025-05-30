#!/bin/bash
set -e

echo "[INFO] Creating build directory..."
mkdir -p build
cd build

echo "[INFO] Running CMake..."
# cmake -DCMAKE_BUILD_TYPE=Debug ..
cmake ..

echo "[INFO] Building project..."
make -j$(nproc)

echo "[INFO] Build complete. Executable is in build/btree" 