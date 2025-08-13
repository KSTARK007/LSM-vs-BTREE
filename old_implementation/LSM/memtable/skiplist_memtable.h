#pragma once

#include "memtable_interface.h"
#include <atomic>
#include <cstdint>
#include <string>
#include <memory>
#include <random>
#include <thread>
#include <vector>
#include <mutex>
#include <shared_mutex>

namespace LSM {

// Forward declarations
class SkipListMemtable;
class SkipListNode;

// Memory ordering constants for better performance
constexpr std::memory_order ACQ_REL = std::memory_order_acq_rel;
constexpr std::memory_order ACQUIRE = std::memory_order_acquire;
constexpr std::memory_order RELEASE = std::memory_order_release;
constexpr std::memory_order RELAXED = std::memory_order_relaxed;

// Key-Value pair structure (moved to memtable_interface.h)

// SkipList node structure
class SkipListNode {
public:
    static constexpr int MAX_LEVEL = 32;
    
    std::string key;
    std::string value;
    std::atomic<bool> is_deleted;
    std::atomic<SkipListNode*> next[MAX_LEVEL];
    std::atomic<int> level;
    
    SkipListNode(const std::string& k, const std::string& v, int lvl = 0) 
        : key(k), value(v), is_deleted(false), level(lvl) {
        for (int i = 0; i < MAX_LEVEL; ++i) {
            next[i].store(nullptr, RELAXED);
        }
    }
    
    ~SkipListNode() = default;
};

// Thread-local random number generator for level generation
class ThreadLocalRandom {
public:
    static int getLevel() {
        thread_local static std::mt19937_64 rng(std::random_device{}());
        thread_local static std::uniform_real_distribution<double> dist(0.0, 1.0);
        
        int level = 1;
        while (level < 32 && dist(rng) < 0.25) {
            level++;
        }
        return level;
    }
};

// High-performance concurrent skiplist memtable
class SkipListMemtable : public MemtableInterface {
private:
    SkipListNode* head_;
    SkipListNode* tail_;
    std::atomic<size_t> size_;
    std::atomic<size_t> memory_usage_;
    size_t max_size_;
    mutable std::shared_mutex rw_mutex_;
    
    // Statistics
    mutable std::atomic<uint64_t> total_inserts_;
    mutable std::atomic<uint64_t> total_lookups_;
    mutable std::atomic<uint64_t> total_deletes_;
    mutable std::atomic<uint64_t> total_updates_;
    
    // Helper methods
    SkipListNode* findNode(const std::string& key, SkipListNode** update = nullptr) const;
    int getRandomLevel() const;
    size_t calculateMemoryUsage() const;
    
public:
    // Constructor and destructor
    SkipListMemtable(size_t max_size = 64 * 1024 * 1024); // 64MB default
    ~SkipListMemtable();
    
    // Core operations
    bool put(const std::string& key, const std::string& value);
    bool get(const std::string& key, std::string& value) const;
    bool delete_key(const std::string& key);
    bool update(const std::string& key, const std::string& value);
    
    // Batch operations
    bool putBatch(const std::vector<KeyValue>& kvs);
    std::vector<KeyValue> getAll() const;
    
    // Memory management
    size_t size() const { return size_.load(RELAXED); }
    size_t memoryUsage() const { return memory_usage_.load(RELAXED); }
    size_t maxSize() const { return max_size_; }
    bool isFull() const { return memory_usage_.load(RELAXED) >= max_size_; }
    
    // Statistics
    uint64_t getTotalInserts() const { return total_inserts_.load(RELAXED); }
    uint64_t getTotalLookups() const { return total_lookups_.load(RELAXED); }
    uint64_t getTotalDeletes() const { return total_deletes_.load(RELAXED); }
    uint64_t getTotalUpdates() const { return total_updates_.load(RELAXED); }
    
    // Clear all data
    void clear();
    
    // Iterator for scanning
    class Iterator : public MemtableInterface::Iterator {
    private:
        SkipListNode* current_;
        const SkipListMemtable* memtable_;
        
    public:
        Iterator(SkipListNode* node, const SkipListMemtable* mt) 
            : current_(node), memtable_(mt) {}
        
        Iterator& operator++() override;
        bool operator!=(const MemtableInterface::Iterator& other) const override;
        KeyValue operator*() const override;
    };
    
    std::unique_ptr<MemtableInterface::Iterator> begin() const override;
    std::unique_ptr<MemtableInterface::Iterator> end() const override;
    
    // Performance tuning
    void setMaxSize(size_t max_size) { max_size_ = max_size; }
    
    // Debug and validation
    void printStats() const;
    bool validate() const;
};

} // namespace LSM 