#pragma once

#include "global.h"
#include <string>
#include <vector>
#include <atomic>
#include <memory>

namespace LSM {

// Forward declarations
class MemtableInterface;
class KeyValue;

// Key-Value pair structure
struct KeyValue {
    std::string key;
    std::string value;
    bool is_deleted;
    
    KeyValue() : is_deleted(false) {}
    KeyValue(const std::string& k, const std::string& v, bool deleted = false) 
        : key(k), value(v), is_deleted(deleted) {}
};

// Unified memtable interface
class MemtableInterface {
public:
    virtual ~MemtableInterface() = default;
    
    // Core operations
    virtual bool put(const std::string& key, const std::string& value) = 0;
    virtual bool get(const std::string& key, std::string& value) const = 0;
    virtual bool delete_key(const std::string& key) = 0;
    virtual bool update(const std::string& key, const std::string& value) = 0;
    
    // Batch operations
    virtual bool putBatch(const std::vector<KeyValue>& kvs) = 0;
    virtual std::vector<KeyValue> getAll() const = 0;
    
    // Memory management
    virtual size_t size() const = 0;
    virtual size_t memoryUsage() const = 0;
    virtual size_t maxSize() const = 0;
    virtual bool isFull() const = 0;
    
    // Statistics
    virtual uint64_t getTotalInserts() const = 0;
    virtual uint64_t getTotalLookups() const = 0;
    virtual uint64_t getTotalDeletes() const = 0;
    virtual uint64_t getTotalUpdates() const = 0;
    
    // Clear all data
    virtual void clear() = 0;
    
    // Iterator for scanning
    class Iterator {
    public:
        virtual ~Iterator() = default;
        virtual Iterator& operator++() = 0;
        virtual bool operator!=(const Iterator& other) const = 0;
        virtual KeyValue operator*() const = 0;
    };
    
    virtual std::unique_ptr<Iterator> begin() const = 0;
    virtual std::unique_ptr<Iterator> end() const = 0;
    
    // Performance tuning
    virtual void setMaxSize(size_t max_size) = 0;
    
    // Debug and validation
    virtual void printStats() const = 0;
    virtual bool validate() const = 0;
};

// Factory function to create memtable instances
std::unique_ptr<MemtableInterface> createMemtable(size_t max_size = DEFAULT_MEMTABLE_SIZE);

} // namespace LSM 