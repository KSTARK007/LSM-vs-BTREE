#include "global.h"
#include <tbb/concurrent_hash_map.h>


struct SSTable {
    uint64_t id; // Unique ID for ordering/debugging
    KeyType min_key;
    KeyType max_key;
    tbb::concurrent_hash_map<KeyType, ValueType> data; // SSTable data stored in memory
    size_t entry_count;

    SSTable(uint64_t i, KeyType min_k, KeyType max_k, tbb::concurrent_hash_map<KeyType, ValueType> d)
        : id(i), min_key(min_k), max_key(max_k), data(std::move(d)) {
        entry_count = data.size();
    }

    // No complex destructor needed for file operations
    ~SSTable() = default;
    
    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;
    SSTable(SSTable&&) = default;
    SSTable& operator=(SSTable&&) = default;

    bool find_key(KeyType key, ValueType& value) const {
        tbb::concurrent_hash_map<KeyType, ValueType>::const_accessor acc;
        if (data.find(acc, key)) {
            if (acc->second == TOMBSTONE_VALUE) {
                return false; // Key found, but it's a tombstone
            }
            value = acc->second;
            return true;
        }
        return false;
    }

    // Creates an in-memory SSTable from a MemTable's contents.
    template <typename MapType>
    static std::shared_ptr<SSTable> create_from_memtable(
        const MapType& memtable_data_to_copy, // Pass by const ref
        uint64_t sstable_id) {
        if (memtable_data_to_copy.empty()) return nullptr;

        tbb::concurrent_hash_map<KeyType, ValueType> tbbmap;
        for (const auto& kv : memtable_data_to_copy) {
            typename tbb::concurrent_hash_map<KeyType, ValueType>::accessor acc;
            tbbmap.insert(acc, kv.first);
            acc->second = kv.second;
        }
        // Find min and max key
        KeyType min_k = std::numeric_limits<KeyType>::max();
        KeyType max_k = std::numeric_limits<KeyType>::min();
        for (auto it = tbbmap.begin(); it != tbbmap.end(); ++it) {
            if (it->first < min_k) min_k = it->first;
            if (it->first > max_k) max_k = it->first;
        }
        return std::make_shared<SSTable>(sstable_id, min_k, max_k, std::move(tbbmap));
    }
};