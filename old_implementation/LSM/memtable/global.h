#pragma once

// Memtable implementation selection
// Uncomment one of the following to choose the implementation

// Option 1: Custom SkipList implementation (provided by user)
#define USE_CUSTOM_SKIPLIST

// Option 2: Original SkipList implementation (built-in)
// #define USE_ORIGINAL_SKIPLIST

// Option 3: Future implementations can be added here
// #define USE_BTREE_MEMTABLE
// #define USE_HASH_MEMTABLE
// #define USE_REDBLACK_MEMTABLE

// Compile-time validation
#if defined(USE_CUSTOM_SKIPLIST) && defined(USE_ORIGINAL_SKIPLIST)
#error "Multiple memtable implementations selected. Please choose only one."
#endif

#if !defined(USE_CUSTOM_SKIPLIST) && !defined(USE_ORIGINAL_SKIPLIST)
#error "No memtable implementation selected. Please choose one implementation."
#endif

#include <cstddef> // for size_t

// Common configuration
namespace LSM {
    // Memory management
    constexpr std::size_t DEFAULT_MEMTABLE_SIZE = 64 * 1024 * 1024; // 64MB
    constexpr std::size_t MAX_MEMTABLE_SIZE = 256 * 1024 * 1024;    // 256MB
    
    // Performance tuning
    constexpr int MAX_SKIPLIST_LEVELS = 32;
    constexpr float SKIPLIST_PROBABILITY = 0.25f;
    
    // Statistics
    constexpr bool ENABLE_STATISTICS = true;
    constexpr bool ENABLE_MEMORY_TRACKING = true;
    
    // Concurrency
    constexpr bool ENABLE_NUMA_AFFINITY = true;
    constexpr int DEFAULT_NUMA_NODES = 3;
    
    // Debugging
    constexpr bool ENABLE_DEBUG_LOGGING = false;
    constexpr bool ENABLE_VALIDATION = true;
}

// Implementation-specific includes
#ifdef USE_CUSTOM_SKIPLIST
#include "custom_skiplist.h"
#endif

#ifdef USE_ORIGINAL_SKIPLIST
#include "skiplist_memtable.h"
#endif

// Feature flags for conditional compilation
#define LSM_ENABLE_STATISTICS LSM::ENABLE_STATISTICS
#define LSM_ENABLE_MEMORY_TRACKING LSM::ENABLE_MEMORY_TRACKING
#define LSM_ENABLE_NUMA_AFFINITY LSM::ENABLE_NUMA_AFFINITY
#define LSM_ENABLE_DEBUG_LOGGING LSM::ENABLE_DEBUG_LOGGING
#define LSM_ENABLE_VALIDATION LSM::ENABLE_VALIDATION 