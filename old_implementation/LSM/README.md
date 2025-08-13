# LSM (Log-Structured Merge) Implementation

This directory contains a step-by-step implementation of an LSM-based datastructure, starting with the most performant memtable implementation.

## Architecture Overview

The LSM implementation follows a layered approach:

1. **Memtable Layer** - High-performance in-memory data structure
2. **SSTable Layer** - On-disk sorted string tables (future implementation)
3. **Bloom Filter Layer** - Probabilistic data structures for fast lookups (future implementation)
4. **Compression Layer** - Data compression for storage efficiency (future implementation)

## Current Implementation: Step 1 - Memtable

### SkipList Memtable

The current implementation features a high-performance concurrent skiplist-based memtable with the following characteristics:

#### Features
- **Concurrent Operations**: Thread-safe put, get, delete, and update operations
- **Memory Management**: Configurable memory limits with automatic overflow handling
- **High Performance**: Optimized memory ordering and lock-free operations where possible
- **Statistics Tracking**: Comprehensive operation counters and performance metrics
- **Batch Operations**: Efficient batch insertions for bulk data loading
- **Iterator Support**: Forward iteration over all key-value pairs

#### Key Components

1. **SkipListNode**: Individual node in the skiplist with atomic operations
2. **SkipListMemtable**: Main memtable class with concurrent operations
3. **ThreadLocalRandom**: Thread-local random number generation for level distribution
4. **KeyValue**: Simple key-value pair structure

#### Performance Optimizations

- **Memory Ordering**: Uses appropriate memory ordering (acquire/release) for optimal performance
- **Lock-Free Reads**: Read operations use shared locks for better concurrency
- **Atomic Operations**: Critical sections use atomic operations to minimize contention
- **NUMA Awareness**: Thread affinity for better cache locality
- **Memory Pooling**: Efficient memory allocation patterns

## Building and Testing

### Prerequisites
- CMake 3.10 or higher
- C++17 compiler
- NUMA library (libnuma-dev)
- pthread library

### Build Instructions

```bash
cd IndexResearch
mkdir -p build
cd build
cmake ..
make -j$(nproc)
```

### Running Tests

```bash
# Run all memtable tests
./LSM/memtable_test

# Run specific test (from test directory)
cd test
./memtable_test
```

### Running Benchmarks

```bash
# Run with default settings (1 thread)
./LSM/memtable_benchmark

# Run with specific thread count
./LSM/memtable_benchmark 4

# Run with custom output file
./LSM/memtable_benchmark 8 custom_results.csv

# Run full benchmark suite
./run_memtable.sh
```

## Benchmark Results

The benchmark system measures:

- **Throughput**: Operations per second (ops/sec)
- **Latency**: Average operation latency in nanoseconds
- **Memory Usage**: Current memory consumption
- **Concurrency**: Performance scaling with thread count

### Workload Types

1. **Read-Heavy (Workload A)**: 50% reads, 50% writes
2. **Read-Mostly (Workload B)**: 95% reads, 5% writes  
3. **Read-Only**: 100% reads for pure read performance

### Output Format

Results are saved in CSV format with columns:
- Thread Count
- Throughput (ops/s)
- Average Latency (ns/op)
- Average Read Latency (ns/op)
- Average Write Latency (ns/op)

## API Reference

### SkipListMemtable

#### Constructor
```cpp
SkipListMemtable(size_t max_size = 64 * 1024 * 1024);
```

#### Core Operations
```cpp
bool put(const std::string& key, const std::string& value);
bool get(const std::string& key, std::string& value) const;
bool delete_key(const std::string& key);
bool update(const std::string& key, const std::string& value);
```

#### Batch Operations
```cpp
bool putBatch(const std::vector<KeyValue>& kvs);
std::vector<KeyValue> getAll() const;
```

#### Memory Management
```cpp
size_t size() const;
size_t memoryUsage() const;
size_t maxSize() const;
bool isFull() const;
```

#### Statistics
```cpp
uint64_t getTotalInserts() const;
uint64_t getTotalLookups() const;
uint64_t getTotalDeletes() const;
uint64_t getTotalUpdates() const;
```

#### Iterator
```cpp
Iterator begin() const;
Iterator end() const;
```

## Performance Characteristics

### Expected Performance (64MB memtable, 1M keys)
- **Insert Throughput**: 500K-1M ops/sec (single thread)
- **Lookup Throughput**: 1M-2M ops/sec (single thread)
- **Memory Overhead**: ~200 bytes per key-value pair
- **Concurrency**: Linear scaling up to 8-16 threads

### Memory Usage
- **Node Overhead**: ~64 bytes per SkipListNode
- **String Storage**: Actual key and value sizes
- **Level Arrays**: 32 pointers per node (256 bytes on 64-bit)
- **Total**: ~200-300 bytes per entry

## Future Steps

### Step 2: SSTable Implementation
- Sorted string table format
- On-disk storage with efficient I/O
- Merge operations for compaction

### Step 3: Bloom Filters
- Probabilistic membership testing
- Reduced disk I/O for lookups
- Configurable false positive rates

### Step 4: Compression
- Snappy/LZ4 compression
- Block-level compression
- Compression ratio optimization

## Testing Strategy

### Unit Tests
- Basic operations (put, get, delete, update)
- Memory limits and overflow handling
- Batch operations
- Iterator functionality
- Concurrent operations
- Statistics tracking
- Data structure validation

### Performance Tests
- Single-threaded performance
- Multi-threaded scaling
- Memory usage patterns
- Workload-specific benchmarks

### Stress Tests
- High-concurrency scenarios
- Memory pressure testing
- Long-running stability tests

## Contributing

When adding new features or optimizations:

1. **Test Coverage**: Ensure comprehensive test coverage
2. **Performance Impact**: Measure performance before/after changes
3. **Memory Usage**: Monitor memory consumption patterns
4. **Concurrency**: Verify thread safety and scaling
5. **Documentation**: Update API documentation and examples

## Troubleshooting

### Common Issues

1. **Build Failures**: Ensure all dependencies are installed
2. **Performance Issues**: Check NUMA configuration and thread affinity
3. **Memory Issues**: Verify memory limits and allocation patterns
4. **Concurrency Issues**: Check for race conditions in custom implementations

### Debugging

Enable debug output by setting environment variables:
```bash
export LSM_DEBUG=1
export LSM_VERBOSE=1
```

## License

This implementation is part of the IndexResearch project and follows the same licensing terms. 