#ifndef SSTABLES_H
#define SSTABLES_H

// Assuming global.h defines KeyType, ValueType, TOMBSTONE_VALUE and ENABLE_LEARNED_INDEX
#include "global.h" 
#include <tbb/concurrent_hash_map.h>
#include "RegisterBlockedBloomFilter.h"

// If ENABLE_LEARNED_INDEX is defined and is 1, include the learned index.
// Otherwise, LearnedIndex type might not be defined.
#if defined(ENABLE_LEARNED_INDEX) && ENABLE_LEARNED_INDEX == 1
#include "learned_index.h" // Our new learned index header
#endif

#include <vector>
#include <algorithm>
#include <cmath>
#include <limits>


struct SSTable {
    uint64_t id;
    KeyType min_key;
    KeyType max_key;
    tbb::concurrent_hash_map<KeyType, ValueType> data;
    size_t entry_count;
    RegisterBlockedBloomFilter bloom;

    #if defined(ENABLE_LEARNED_INDEX) && ENABLE_LEARNED_INDEX == 1
    LearnedIndex learned_idx; // Use the new LearnedIndex class
    #endif

    SSTable(uint64_t i, KeyType min_k, KeyType max_k, tbb::concurrent_hash_map<KeyType, ValueType> d)
        : id(i), min_key(min_k), max_key(max_k), data(std::move(d)), entry_count(data.size()),
          bloom(512, 7)
    {
        for (auto it = data.begin(); it != data.end(); ++it) {
            bloom.Insert(it->first);
        }

        #if defined(ENABLE_LEARNED_INDEX) && ENABLE_LEARNED_INDEX == 1
        if (entry_count > 0) {
            std::vector<KeyType> sorted_keys;
            sorted_keys.reserve(entry_count);
            for (const auto& pair : data) {
                sorted_keys.push_back(pair.first);
            }
            std::sort(sorted_keys.begin(), sorted_keys.end());
            learned_idx.train(sorted_keys);
        }
        #endif
    }

    ~SSTable() = default;
    
    SSTable(const SSTable&) = delete;
    SSTable& operator=(const SSTable&) = delete;
    SSTable(SSTable&&) = default;
    SSTable& operator=(SSTable&&) = default;

    bool find_key(KeyType key, ValueType& value) const {
        if (key < min_key || key > max_key) return false;

        #if defined(ENABLE_LEARNED_INDEX) && ENABLE_LEARNED_INDEX == 1
        if (learned_idx.is_trained() && entry_count > 0) {
            // Only use LI if key is within the range of keys the model was trained on.
            if (key >= learned_idx.get_min_training_key() && key <= learned_idx.get_max_training_key()) {
                int estimated_min_idx, estimated_max_idx;
                if (learned_idx.predict_index_range(key, estimated_min_idx, estimated_max_idx)) {
                    if (estimated_min_idx > estimated_max_idx) { // Predicted range is empty
                        #ifdef LEARNED_INDEX_AGGRESSIVE_FILTERING
                        return false; 
                        #endif
                    }
                }
            }
            // If key is outside learned_idx training range (but within SSTable min/max),
            // or if prediction fails, we just proceed without LI filtering.
        }
        #endif

        if (!bloom.Query(key)) return false;

        tbb::concurrent_hash_map<KeyType, ValueType>::const_accessor acc;
        if (data.find(acc, key)) {
            if (acc->second == TOMBSTONE_VALUE) {
                return false;
            }
            value = acc->second;
            return true;
        }
        return false;
    }

    template <typename MapType>
    static std::shared_ptr<SSTable> create_from_memtable(
        const MapType& memtable_data_to_copy,
        uint64_t sstable_id) {
        if (memtable_data_to_copy.empty()) return nullptr;

        tbb::concurrent_hash_map<KeyType, ValueType> tbbmap;
        KeyType min_k = std::numeric_limits<KeyType>::max(); // Initialize properly
        KeyType max_k = std::numeric_limits<KeyType>::min(); // Initialize properly
        bool first_key = true;

        for (const auto& kv : memtable_data_to_copy) {
            typename tbb::concurrent_hash_map<KeyType, ValueType>::accessor acc_tbb;
            tbbmap.insert(acc_tbb, kv.first);
            acc_tbb->second = kv.second;
            
            if (first_key) {
                min_k = kv.first;
                max_k = kv.first;
                first_key = false;
            } else {
                min_k = std::min(min_k, kv.first);
                max_k = std::max(max_k, kv.first);
            }
        }
        
        if (tbbmap.empty()) return nullptr;

        return std::make_shared<SSTable>(sstable_id, min_k, max_k, std::move(tbbmap));
    }
};

#endif // SSTABLES_H