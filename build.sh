#!/bin/bash
set -e

echo "[INFO] Creating build directory..."
mkdir -p build
cd build

echo "[INFO] Running CMake..."
cmake ..

echo "[INFO] Building project..."
make -j$(nproc)

echo "[INFO] Build complete. Executable is in build/btree" 