#include "skiplist_memtable.h"
#include <algorithm>
#include <cassert>
#include <iostream>
#include <cstring>

namespace LSM {

// Constructor
SkipListMemtable::SkipListMemtable(size_t max_size) 
    : max_size_(max_size), size_(0), memory_usage_(0),
      total_inserts_(0), total_lookups_(0), total_deletes_(0), total_updates_(0) {
    
    // Initialize head and tail nodes
    head_ = new SkipListNode("", "", SkipListNode::MAX_LEVEL);
    tail_ = new SkipListNode("", "", SkipListNode::MAX_LEVEL);
    
    // Initialize all levels to point to tail
    for (int i = 0; i < SkipListNode::MAX_LEVEL; ++i) {
        head_->next[i].store(tail_, RELEASE);
    }
}

// Destructor
SkipListMemtable::~SkipListMemtable() {
    clear();
    delete head_;
    delete tail_;
}

// Find node helper method
SkipListNode* SkipListMemtable::findNode(const std::string& key, SkipListNode** update) const {
    SkipListNode* current = head_;
    
    // Start from the highest level and work down
    for (int i = SkipListNode::MAX_LEVEL - 1; i >= 0; --i) {
        while (true) {
            SkipListNode* next = current->next[i].load(ACQUIRE);
            if (next == tail_ || next->key >= key) {
                break;
            }
            current = next;
        }
        
        if (update) {
            update[i] = current;
        }
    }
    
    current = current->next[0].load(ACQUIRE);
    return (current != tail_ && current->key == key) ? current : nullptr;
}

// Get random level for new nodes
int SkipListMemtable::getRandomLevel() const {
    return ThreadLocalRandom::getLevel();
}

// Calculate memory usage
size_t SkipListMemtable::calculateMemoryUsage() const {
    size_t usage = 0;
    SkipListNode* current = head_->next[0].load(ACQUIRE);
    
    while (current != tail_) {
        usage += sizeof(SkipListNode);
        usage += current->key.size();
        usage += current->value.size();
        current = current->next[0].load(ACQUIRE);
    }
    
    return usage;
}

// Put operation
bool SkipListMemtable::put(const std::string& key, const std::string& value) {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    // Check if key already exists
    SkipListNode* existing = findNode(key);
    if (existing) {
        // Update existing node
        existing->value = value;
        existing->is_deleted.store(false, RELEASE);
        total_updates_.fetch_add(1, RELAXED);
        return true;
    }
    
    // Calculate memory usage for new node
    size_t new_memory = sizeof(SkipListNode) + key.size() + value.size();
    if (memory_usage_.load(RELAXED) + new_memory > max_size_) {
        return false; // Memtable is full
    }
    
    // Create new node
    int level = getRandomLevel();
    SkipListNode* new_node = new SkipListNode(key, value, level);
    
    // Insert the node
    SkipListNode* update[SkipListNode::MAX_LEVEL];
    findNode(key, update);
    
    // Update forward pointers
    for (int i = 0; i < level; ++i) {
        new_node->next[i].store(update[i]->next[i].load(ACQUIRE), RELEASE);
        update[i]->next[i].store(new_node, RELEASE);
    }
    
    // Update statistics
    size_.fetch_add(1, RELEASE);
    memory_usage_.fetch_add(new_memory, RELEASE);
    total_inserts_.fetch_add(1, RELAXED);
    
    return true;
}

// Get operation
bool SkipListMemtable::get(const std::string& key, std::string& value) const {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    SkipListNode* node = findNode(key);
    if (node && !node->is_deleted.load(ACQUIRE)) {
        value = node->value;
        total_lookups_.fetch_add(1, RELAXED);
        return true;
    }
    
    total_lookups_.fetch_add(1, RELAXED);
    return false;
}

// Delete operation
bool SkipListMemtable::delete_key(const std::string& key) {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    SkipListNode* node = findNode(key);
    if (node) {
        node->is_deleted.store(true, RELEASE);
        total_deletes_.fetch_add(1, RELAXED);
        return true;
    }
    
    return false;
}

// Update operation
bool SkipListMemtable::update(const std::string& key, const std::string& value) {
    return put(key, value); // put handles both insert and update
}

// Batch put operation
bool SkipListMemtable::putBatch(const std::vector<KeyValue>& kvs) {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    
    size_t total_new_memory = 0;
    
    // Calculate total memory needed
    for (const auto& kv : kvs) {
        SkipListNode* existing = findNode(kv.key);
        if (!existing) {
            total_new_memory += sizeof(SkipListNode) + kv.key.size() + kv.value.size();
        }
    }
    
    // Check if we have enough space
    if (memory_usage_.load(RELAXED) + total_new_memory > max_size_) {
        return false;
    }
    
    // Insert all key-value pairs
    for (const auto& kv : kvs) {
        put(kv.key, kv.value);
    }
    
    return true;
}

// Get all key-value pairs
std::vector<KeyValue> SkipListMemtable::getAll() const {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    std::vector<KeyValue> result;
    SkipListNode* current = head_->next[0].load(ACQUIRE);
    
    while (current != tail_) {
        if (!current->is_deleted.load(ACQUIRE)) {
            result.emplace_back(current->key, current->value, false);
        }
        current = current->next[0].load(ACQUIRE);
    }
    
    return result;
}

// Clear all data
void SkipListMemtable::clear() {
    std::unique_lock<std::shared_mutex> write_lock(rw_mutex_);
    
    SkipListNode* current = head_->next[0].load(ACQUIRE);
    while (current != tail_) {
        SkipListNode* next = current->next[0].load(ACQUIRE);
        delete current;
        current = next;
    }
    
    // Reset head pointers to tail
    for (int i = 0; i < SkipListNode::MAX_LEVEL; ++i) {
        head_->next[i].store(tail_, RELEASE);
    }
    
    size_.store(0, RELEASE);
    memory_usage_.store(0, RELEASE);
}

// Iterator implementation
SkipListMemtable::Iterator& SkipListMemtable::Iterator::operator++() {
    if (current_) {
        current_ = current_->next[0].load(ACQUIRE);
        // Skip deleted nodes
        while (current_ && current_->is_deleted.load(ACQUIRE)) {
            current_ = current_->next[0].load(ACQUIRE);
        }
    }
    return *this;
}

bool SkipListMemtable::Iterator::operator!=(const MemtableInterface::Iterator& other) const {
    const Iterator* other_iter = dynamic_cast<const Iterator*>(&other);
    if (!other_iter) return true;
    return current_ != other_iter->current_;
}

KeyValue SkipListMemtable::Iterator::operator*() const {
    if (current_) {
        return KeyValue(current_->key, current_->value, current_->is_deleted.load(ACQUIRE));
    }
    return KeyValue();
}

std::unique_ptr<MemtableInterface::Iterator> SkipListMemtable::begin() const {
    SkipListNode* first = head_->next[0].load(ACQUIRE);
    // Skip deleted nodes
    while (first != tail_ && first->is_deleted.load(ACQUIRE)) {
        first = first->next[0].load(ACQUIRE);
    }
    return std::make_unique<Iterator>(first, this);
}

std::unique_ptr<MemtableInterface::Iterator> SkipListMemtable::end() const {
    return std::make_unique<Iterator>(tail_, this);
}

// Print statistics
void SkipListMemtable::printStats() const {
    std::cout << "=== SkipList Memtable Statistics ===" << std::endl;
    std::cout << "Size: " << size() << " entries" << std::endl;
    std::cout << "Memory Usage: " << memoryUsage() << " bytes" << std::endl;
    std::cout << "Max Size: " << maxSize() << " bytes" << std::endl;
    std::cout << "Total Inserts: " << getTotalInserts() << std::endl;
    std::cout << "Total Lookups: " << getTotalLookups() << std::endl;
    std::cout << "Total Deletes: " << getTotalDeletes() << std::endl;
    std::cout << "Total Updates: " << getTotalUpdates() << std::endl;
    std::cout << "===================================" << std::endl;
}

// Validate skiplist structure
bool SkipListMemtable::validate() const {
    std::shared_lock<std::shared_mutex> read_lock(rw_mutex_);
    
    // Check that head points to tail at all levels
    for (int i = 0; i < SkipListNode::MAX_LEVEL; ++i) {
        SkipListNode* next = head_->next[i].load(ACQUIRE);
        if (next == nullptr) {
            return false;
        }
    }
    
    // Check that tail is reachable from head
    SkipListNode* current = head_;
    for (int i = SkipListNode::MAX_LEVEL - 1; i >= 0; --i) {
        while (current->next[i].load(ACQUIRE) != tail_) {
            current = current->next[i].load(ACQUIRE);
            if (current == nullptr) {
                return false;
            }
        }
    }
    
    return true;
}

} // namespace LSM 