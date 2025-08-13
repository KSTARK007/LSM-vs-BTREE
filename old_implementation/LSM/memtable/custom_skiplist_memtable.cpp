#include "custom_skiplist_memtable.h"
#include <iostream>
#include <algorithm>
#include <mutex>
#include <limits> // Required for std::numeric_limits

namespace LSM {

// Constructor
CustomSkipListMemtable::CustomSkipListMemtable(size_t max_size) 
    : max_size_(max_size), current_memory_usage_(0),
      total_inserts_(0), total_lookups_(0), total_deletes_(0), total_updates_(0) {
    skiplist_ = std::make_unique<SkipList<std::string, std::string>>();
}

// Helper methods
size_t CustomSkipListMemtable::calculateMemoryUsage() const {
    // Estimate memory usage based on skiplist size and average entry size
    size_t estimated_size = skiplist_->size() * 256; // Rough estimate per entry
    return estimated_size;
}

size_t CustomSkipListMemtable::estimateEntrySize(const std::string& key, const std::string& value) const {
    // Estimate memory usage for a single entry
    return sizeof(std::string) * 2 + key.size() + value.size() + 64; // Overhead for skiplist node
}

// Put operation
bool CustomSkipListMemtable::put(const std::string& key, const std::string& value) {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    
    // Check if key already exists (for update)
    std::string existing_value;
    bool exists = get(key, existing_value);
    
    // Calculate memory usage for new/updated entry
    size_t entry_size = estimateEntrySize(key, value);
    if (!exists) {
        // New entry
        if (current_memory_usage_ + entry_size > max_size_) {
            return false; // Memtable is full
        }
        current_memory_usage_ += entry_size;
        total_inserts_.fetch_add(1, std::memory_order_relaxed);
    } else {
        // Update existing entry
        total_updates_.fetch_add(1, std::memory_order_relaxed);
    }
    
    // Insert or update in skiplist
    skiplist_->insert(key, value);
    
    return true;
}

// Get operation
bool CustomSkipListMemtable::get(const std::string& key, std::string& value) const {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    std::string* result = skiplist_->find_wait_free(key);
    if (result) {
        value = *result;
        total_lookups_.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    
    total_lookups_.fetch_add(1, std::memory_order_relaxed);
    return false;
}

// Delete operation
bool CustomSkipListMemtable::delete_key(const std::string& key) {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    bool success = skiplist_->remove(key);
    if (success) {
        total_deletes_.fetch_add(1, std::memory_order_relaxed);
        // Update memory usage estimate
        current_memory_usage_ = calculateMemoryUsage();
    }
    
    return success;
}

// Update operation
bool CustomSkipListMemtable::update(const std::string& key, const std::string& value) {
    return put(key, value); // put handles both insert and update
}

// Batch put operation
bool CustomSkipListMemtable::putBatch(const std::vector<KeyValue>& kvs) {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    
    size_t total_new_memory = 0;
    
    // Calculate total memory needed for new entries
    for (const auto& kv : kvs) {
        std::string existing_value;
        bool exists = get(kv.key, existing_value);
        if (!exists) {
            total_new_memory += estimateEntrySize(kv.key, kv.value);
        }
    }
    
    // Check if we have enough space
    if (current_memory_usage_ + total_new_memory > max_size_) {
        return false;
    }
    
    // Insert all key-value pairs
    for (const auto& kv : kvs) {
        put(kv.key, kv.value);
    }
    
    return true;
}

// Get all key-value pairs
std::vector<KeyValue> CustomSkipListMemtable::getAll() const {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    std::vector<KeyValue> result;
    
    // Note: The custom skiplist doesn't have a built-in iterator,
    // so we'll need to implement this differently or add iterator support
    // For now, we'll return an empty vector
    // TODO: Implement proper iteration over the skiplist
    
    return result;
}

// Clear all data
void CustomSkipListMemtable::clear() {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    
    // Delete the current skiplist and create a new one
    skiplist_ = std::make_unique<SkipList<std::string, std::string>>();
    current_memory_usage_ = 0;
}

// Iterator implementation
CustomSkipListMemtable::CustomIterator::CustomIterator(SkipList<std::string, std::string>* sl, bool end)
    : skiplist_(sl), is_end_(end) {
    if (!is_end_ && skiplist_) {
        // Initialize to first element (this is a simplified implementation)
        // TODO: Implement proper iteration
    }
}

CustomSkipListMemtable::Iterator& CustomSkipListMemtable::CustomIterator::operator++() {
    // TODO: Implement proper iteration
    is_end_ = true;
    return *this;
}

bool CustomSkipListMemtable::CustomIterator::operator!=(const Iterator& other) const {
    const CustomIterator* other_iter = dynamic_cast<const CustomIterator*>(&other);
    if (!other_iter) return true;
    return is_end_ != other_iter->is_end_;
}

KeyValue CustomSkipListMemtable::CustomIterator::operator*() const {
    return KeyValue(current_key_, current_value_, false);
}

std::unique_ptr<MemtableInterface::Iterator> CustomSkipListMemtable::begin() const {
    return std::make_unique<CustomIterator>(skiplist_.get(), false);
}

std::unique_ptr<MemtableInterface::Iterator> CustomSkipListMemtable::end() const {
    return std::make_unique<CustomIterator>(skiplist_.get(), true);
}

// Print statistics
void CustomSkipListMemtable::printStats() const {
    std::cout << "=== Custom SkipList Memtable Statistics ===" << std::endl;
    std::cout << "Size: " << size() << " entries" << std::endl;
    std::cout << "Memory Usage: " << memoryUsage() << " bytes" << std::endl;
    std::cout << "Max Size: " << maxSize() << " bytes" << std::endl;
    std::cout << "Total Inserts: " << getTotalInserts() << std::endl;
    std::cout << "Total Lookups: " << getTotalLookups() << std::endl;
    std::cout << "Total Deletes: " << getTotalDeletes() << std::endl;
    std::cout << "Total Updates: " << getTotalUpdates() << std::endl;
    std::cout << "===========================================" << std::endl;
}

// Validate skiplist structure
bool CustomSkipListMemtable::validate() const {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    // Basic validation - check if skiplist is not null
    if (!skiplist_) {
        return false;
    }
    
    // TODO: Add more comprehensive validation
    return true;
}

} // namespace LSM 