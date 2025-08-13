#pragma once

#include "memtable_interface.h"
#include "custom_skiplist.h"
#include <memory>
#include <atomic>
#include <shared_mutex>

namespace LSM {

// Custom skiplist-based memtable implementation
class CustomSkipListMemtable : public MemtableInterface {
private:
    std::unique_ptr<SkipList<std::string, std::string>> skiplist_;
    size_t max_size_;
    size_t current_memory_usage_;
    mutable std::shared_mutex rw_mutex_;
    
    // Statistics
    mutable std::atomic<uint64_t> total_inserts_;
    mutable std::atomic<uint64_t> total_lookups_;
    mutable std::atomic<uint64_t> total_deletes_;
    mutable std::atomic<uint64_t> total_updates_;
    
    // Helper methods
    size_t calculateMemoryUsage() const;
    size_t estimateEntrySize(const std::string& key, const std::string& value) const;
    
public:
    // Constructor and destructor
    CustomSkipListMemtable(size_t max_size = DEFAULT_MEMTABLE_SIZE);
    ~CustomSkipListMemtable() override = default;
    
    // Core operations
    bool put(const std::string& key, const std::string& value) override;
    bool get(const std::string& key, std::string& value) const override;
    bool delete_key(const std::string& key) override;
    bool update(const std::string& key, const std::string& value) override;
    
    // Batch operations
    bool putBatch(const std::vector<KeyValue>& kvs) override;
    std::vector<KeyValue> getAll() const override;
    
    // Memory management
    size_t size() const override { return skiplist_->size(); }
    size_t memoryUsage() const override { return current_memory_usage_; }
    size_t maxSize() const override { return max_size_; }
    bool isFull() const override { return current_memory_usage_ >= max_size_; }
    
    // Statistics
    uint64_t getTotalInserts() const override { return total_inserts_.load(std::memory_order_relaxed); }
    uint64_t getTotalLookups() const override { return total_lookups_.load(std::memory_order_relaxed); }
    uint64_t getTotalDeletes() const override { return total_deletes_.load(std::memory_order_relaxed); }
    uint64_t getTotalUpdates() const override { return total_updates_.load(std::memory_order_relaxed); }
    
    // Clear all data
    void clear() override;
    
    // Iterator implementation
    class CustomIterator : public Iterator {
    private:
        SkipList<std::string, std::string>* skiplist_;
        std::string current_key_;
        std::string current_value_;
        bool is_end_;
        
    public:
        CustomIterator(SkipList<std::string, std::string>* sl, bool end = false);
        
        Iterator& operator++() override;
        bool operator!=(const Iterator& other) const override;
        KeyValue operator*() const override;
    };
    
    std::unique_ptr<Iterator> begin() const override;
    std::unique_ptr<Iterator> end() const override;
    
    // Performance tuning
    void setMaxSize(size_t max_size) override { max_size_ = max_size; }
    
    // Debug and validation
    void printStats() const override;
    bool validate() const override;
};

} // namespace LSM 