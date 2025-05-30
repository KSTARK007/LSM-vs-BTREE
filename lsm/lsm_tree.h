#ifndef LSM_TREE_H
#define LSM_TREE_H

#include "global.h"
#include "SSTables.h"
#include <tbb/concurrent_hash_map.h>
#include <condition_variable>

class LSMTree {
public:
    LSMTree(size_t memtable_max_entries = 1000,
            size_t l0_max_sstables = 4,
            int num_levels = 4,
            double level_size_ratio = 10.0,     // Max entries in L_i+1 = ratio * Max entries in L_i
            size_t sstable_target_entries = 256) // Target entries per SSTable during compaction
        : active_memtable_(std::make_unique<MemTable>()),
          next_sstable_id_(0),
          shutdown_requested_(false),
          memtable_max_size_entries_(memtable_max_entries),
          max_level0_sstables_(l0_max_sstables),
          max_levels_(num_levels),
          level_entry_multiplier_(level_size_ratio),
          sstable_target_entry_count_(sstable_target_entries) {

        levels_.resize(max_levels_);
        // No disk loading or directory creation needed

        shutdown_requested_ = false;
        flush_worker_thread_ = std::thread(&LSMTree::flush_worker_loop, this);
        compaction_worker_thread_ = std::thread(&LSMTree::compaction_worker_loop, this);
    }

    ~LSMTree() {
        shutdown_requested_ = true;
        immutable_memtables_cv_.notify_all();
        compaction_cv_.notify_all();

        if (flush_worker_thread_.joinable()) {
            flush_worker_thread_.join();
        }
        if (compaction_worker_thread_.joinable()) {
            compaction_worker_thread_.join();
        }
        
        // Final flush of active and immutable memtables (to L0 in-memory SSTables)
        if (active_memtable_ && !active_memtable_->empty()) {
            std::lock_guard<std::mutex> lock(immutable_memtables_mutex_);
            immutable_memtables_.push_back(std::move(active_memtable_));
        }
        active_memtable_ = nullptr;

        while (true) {
            std::unique_ptr<MemTable> memtable_to_flush;
            {
                std::lock_guard<std::mutex> lock(immutable_memtables_mutex_);
                if (immutable_memtables_.empty()) break;
                memtable_to_flush = std::move(immutable_memtables_.front());
                immutable_memtables_.erase(immutable_memtables_.begin());
            }
            if (memtable_to_flush) {
                flush_memtable_to_l0(std::move(memtable_to_flush));
            }
        }
    }

    bool get(KeyType key, ValueType& value) {
        // 1. Check active memtable
        if (active_memtable_) {
            MemTable::const_accessor acc;
            if (active_memtable_->find(acc, key)) {
                if (acc->second == TOMBSTONE_VALUE) return false;
                value = acc->second;
                return true;
            }
        }
        // 2. Check immutable memtables (newest to oldest)
        {
            std::lock_guard<std::mutex> lock(immutable_memtables_mutex_);
            for (auto it = immutable_memtables_.rbegin(); it != immutable_memtables_.rend(); ++it) {
                auto const& mt = *it;
                MemTable::const_accessor acc;
                if (mt->find(acc, key)) {
                    if (acc->second == TOMBSTONE_VALUE) return false;
                    value = acc->second;
                    return true;
                }
            }
        }

        // 3. Check SSTables (L0 newest first, then L1 to Ln)
        std::shared_lock<std::shared_mutex> levels_lock(levels_metadata_mutex_);
        if (!levels_.empty()) { // L0
            const auto& level0_sstables = levels_[0];
            for (auto sst_it = level0_sstables.rbegin(); sst_it != level0_sstables.rend(); ++sst_it) {
                std::shared_ptr<SSTable> sstable = *sst_it; // Ensure local shared_ptr copy
                if (key >= sstable->min_key && key <= sstable->max_key) {
                    if (sstable->find_key(key, value)) return true;
                }
            }
        }

        for (size_t i = 1; i < levels_.size(); ++i) { // L1+
            const auto& current_level_sstables = levels_[i];
            for (const auto& sstable_ptr : current_level_sstables) { // L1+ SSTables are non-overlapping by min_key
                std::shared_ptr<SSTable> sstable = sstable_ptr; // Ensure local shared_ptr copy
                if (key >= sstable->min_key && key <= sstable->max_key) { // Range check first
                    if (sstable->find_key(key, value)) return true;
                    // If non-overlapping and sorted by min_key, can break early if sstable->min_key > key
                } else if (sstable->min_key > key && !current_level_sstables.empty() && sstable == current_level_sstables.front()){
                    // Optimization for sorted, non-overlapping levels: if key is smaller than the first sstable's min_key
                } else if (sstable->min_key > key) { // If level sorted by min_key & non-overlapping
                     break; // Key cannot be in subsequent SSTables of this level
                }
            }
        }
        return false;
    }

    void put(KeyType key, const ValueType& value) {
        std::unique_lock<std::shared_mutex> lock(active_memtable_mutex_);
        if (!active_memtable_) {
            active_memtable_ = std::make_unique<MemTable>();
        }
        {
            MemTable::accessor acc;
            active_memtable_->insert(acc, key);
            acc->second = value;
        }
        if (active_memtable_->size() >= memtable_max_size_entries_) {
            lock.unlock(); // Unlock before calling schedule_flush_active_memtable to avoid deadlock
            schedule_flush_active_memtable();
        }
    }

    void del(KeyType key) {
        put(key, TOMBSTONE_VALUE);
    }
    
    void print_tree_stats() {
        std::cout << "--- LSM Tree In-Memory Stats ---" << std::endl;
        {
            std::shared_lock<std::shared_mutex> lock(active_memtable_mutex_);
            std::cout << "Active MemTable Entries: " << (active_memtable_ ? active_memtable_->size() : 0) << "/" << memtable_max_size_entries_ << std::endl;
        }
        {
            std::lock_guard<std::mutex> lock(immutable_memtables_mutex_);
            std::cout << "Immutable MemTables Count: " << immutable_memtables_.size() << std::endl;
        }
        {
            std::shared_lock<std::shared_mutex> lock(levels_metadata_mutex_);
            std::cout << "SSTable Levels: " << levels_.size() << " (Max Configured: " << max_levels_ << ")" << std::endl;
            for (size_t i = 0; i < levels_.size(); ++i) {
                size_t total_entries = 0;
                for(const auto& sst : levels_[i]) {
                    total_entries += sst->entry_count;
                }
                std::cout << "  Level " << i << ": " << levels_[i].size() << " SSTables, Total Entries: " << total_entries << std::endl;
                 if (i == 0 && levels_[i].size() > max_level0_sstables_) {
                    std::cout << "    (Needs L0 compaction, max SSTables is " << max_level0_sstables_ << ")" << std::endl;
                 } else if (i > 0 && total_entries > get_max_entries_for_level(i)) { // Check L1+
                    std::cout << "    (Needs L" << i << " compaction, max entries is ~" << get_max_entries_for_level(i) << ")" << std::endl;
                 }
            }
        }
         std::cout << "Next SSTable ID: " << next_sstable_id_.load() << std::endl;
        std::cout << "--------------------------------" << std::endl;
    }

private:
    using MemTable = tbb::concurrent_hash_map<KeyType, ValueType>;
    using SSTablePtr = std::shared_ptr<SSTable>;

    std::unique_ptr<MemTable> active_memtable_;
    std::shared_mutex active_memtable_mutex_;

    std::vector<std::unique_ptr<MemTable>> immutable_memtables_;
    std::mutex immutable_memtables_mutex_;
    std::condition_variable immutable_memtables_cv_;

    std::vector<std::vector<SSTablePtr>> levels_;
    std::shared_mutex levels_metadata_mutex_;

    std::atomic<uint64_t> next_sstable_id_;

    size_t memtable_max_size_entries_;
    size_t max_level0_sstables_;
    int max_levels_;
    double level_entry_multiplier_;
    size_t sstable_target_entry_count_;

    std::thread flush_worker_thread_;
    std::thread compaction_worker_thread_;
    std::atomic<bool> shutdown_requested_;
    std::condition_variable compaction_cv_;
    std::mutex compaction_mutex_;


    void schedule_flush_active_memtable() {
        std::unique_ptr<MemTable> new_active = std::make_unique<MemTable>();
        std::unique_ptr<MemTable> old_active_to_flush;
        {
            std::unique_lock<std::shared_mutex> lock(active_memtable_mutex_);
            if (active_memtable_ && active_memtable_->size() >= memtable_max_size_entries_) {
                old_active_to_flush = std::move(active_memtable_);
                active_memtable_ = std::move(new_active);
            } else {
                return;
            }
        }
        if (old_active_to_flush) {
            std::lock_guard<std::mutex> imm_lock(immutable_memtables_mutex_);
            immutable_memtables_.push_back(std::move(old_active_to_flush));
            immutable_memtables_cv_.notify_one();
        }
    }

    void flush_worker_loop() {
        while (!shutdown_requested_) {
            std::unique_ptr<MemTable> memtable_to_flush;
            {
                std::unique_lock<std::mutex> lock(immutable_memtables_mutex_);
                immutable_memtables_cv_.wait(lock, [this] {
                    return shutdown_requested_ || !immutable_memtables_.empty();
                });

                if (shutdown_requested_ && immutable_memtables_.empty()) break;
                if (immutable_memtables_.empty()) continue;

                memtable_to_flush = std::move(immutable_memtables_.front());
                immutable_memtables_.erase(immutable_memtables_.begin());
            }

            if (memtable_to_flush) {
                flush_memtable_to_l0(std::move(memtable_to_flush));
                compaction_cv_.notify_one(); // Signal for potential L0 compaction
            }
        }
    }
    
    void flush_memtable_to_l0(std::unique_ptr<MemTable> memtable_data_ptr) {
        if (!memtable_data_ptr || memtable_data_ptr->empty()) return;

        uint64_t current_sstable_id = next_sstable_id_++;
        // Create an in-memory SSTable. Pass the map data directly.
        SSTablePtr new_sstable = SSTable::create_from_memtable(
            *memtable_data_ptr, current_sstable_id);

        if (new_sstable) {
            std::unique_lock<std::shared_mutex> lock(levels_metadata_mutex_);
            levels_[0].push_back(new_sstable);
            // L0 SSTables are sorted by creation time (ID) to search newest first
            std::sort(levels_[0].begin(), levels_[0].end(), [](const SSTablePtr& a, const SSTablePtr& b) {
                return a->id < b->id; // Older IDs (smaller) first for consistent iteration order
            });
        }
    }

    size_t get_level_total_entries(int level_idx) const {
        // Caller must hold levels_metadata_mutex_ (shared is fine)
        size_t total_entries = 0;
        if (level_idx < 0 || level_idx >= static_cast<int>(levels_.size())) return 0;
        for (const auto& sst : levels_[level_idx]) {
            total_entries += sst->entry_count;
        }
        return total_entries;
    }
    
    size_t get_max_entries_for_level(int level_idx) const {
        if (level_idx < 0) return 0;
        // L0 limit is by number of SSTables (max_level0_sstables_)
        // Max entries for L0 = max_level0_sstables_ * sstable_target_entry_count_ (approx)
        if (level_idx == 0) { 
            return max_level0_sstables_ * sstable_target_entry_count_; 
        }
        // Max entries L_i+1 = level_entry_multiplier_ * Max entries L_i
        size_t max_l0_entries = max_level0_sstables_ * sstable_target_entry_count_; 
        size_t max_entries = max_l0_entries;
        for (int i = 0; i < level_idx; ++i) { // level_idx is 1-based for this loop conceptually
            max_entries = static_cast<size_t>(max_entries * level_entry_multiplier_);
        }
        return max_entries;
    }

    void compaction_worker_loop() {
        while (!shutdown_requested_) {
            std::unique_lock<std::mutex> lock(compaction_mutex_);
            compaction_cv_.wait(lock, [this] {
                if(shutdown_requested_) return true;
                std::shared_lock<std::shared_mutex> levels_lock(levels_metadata_mutex_);
                if (levels_[0].size() > max_level0_sstables_) return true; // L0 compaction based on file count
                for (int i = 0; i < max_levels_ - 1; ++i) { // Check L0 to L_N-2 for size-based compaction into next level
                    if (get_level_total_entries(i) > get_max_entries_for_level(i)) return true;
                }
                return false;
            });

            if (shutdown_requested_) break;
            lock.unlock();
            perform_compaction_check();
        }
    }
    
    void perform_compaction_check() {
        std::unique_lock<std::shared_mutex> levels_lock(levels_metadata_mutex_); 
        if (levels_[0].size() > max_level0_sstables_) {
            std::vector<SSTablePtr> l0_ssts = levels_[0]; // Copy shared_ptrs
            std::vector<SSTablePtr> l1_overlap_ssts;
            if (max_levels_ > 1) {
                l1_overlap_ssts = find_overlapping_sstables_nolock(l0_ssts, 1);
            }
            levels_lock.unlock(); 

            compact_sstables(0, l0_ssts, l1_overlap_ssts);
            return; 
        }
        
        for (int i = 0; i < max_levels_ - 1; ++i) { // Check L_i -> L_{i+1}
            size_t current_level_entries = get_level_total_entries(i); // Needs lock if not already held
            size_t max_level_entries_threshold = get_max_entries_for_level(i);

            if (current_level_entries > max_level_entries_threshold) {
                // Simplistic: compact whole level 'i' if too big. Better: pick specific SSTables.
                std::vector<SSTablePtr> li_ssts = levels_[i]; // Copy shared_ptrs
                std::vector<SSTablePtr> li_plus_1_overlap_ssts = find_overlapping_sstables_nolock(li_ssts, i + 1);
                
                levels_lock.unlock(); // Unlock before heavy operation
                compact_sstables(i, li_ssts, li_plus_1_overlap_ssts);
                return; 
            }
        }
        // If lock was taken and no compaction happened, it will be released when current_level_lock goes out of scope
        // or explicitly released if it was the top-level `levels_lock`.
        // Ensure `levels_lock` is released if no compaction is triggered from the loop.
        // The above structure unlocks before calling compact_sstables, or the loop finishes and lock is released.
    }

    // Assumes levels_metadata_mutex_ is already held (shared or exclusive) by caller.
    std::vector<SSTablePtr> find_overlapping_sstables_nolock(const std::vector<SSTablePtr>& source_ssts, int target_level_idx) {
        std::vector<SSTablePtr> overlapping;
        if (source_ssts.empty() || target_level_idx < 0 || target_level_idx >= max_levels_) {
            return overlapping;
        }

        KeyType overall_min_key = source_ssts.front()->min_key;
        KeyType overall_max_key = source_ssts.front()->max_key; // This should be max_key of the last SSTable if sorted
        for(const auto& sst : source_ssts) { // Recalculate true overall range
            if (sst->min_key < overall_min_key) overall_min_key = sst->min_key;
            if (sst->max_key > overall_max_key) overall_max_key = sst->max_key;
        }
        
        const auto& target_level_files = levels_[target_level_idx];
        for (const auto& sst : target_level_files) {
            if (sst->min_key <= overall_max_key && sst->max_key >= overall_min_key) { // Standard overlap condition
                overlapping.push_back(sst);
            }
        }
        return overlapping;
    }

    void compact_sstables(int source_level_idx,
                          const std::vector<SSTablePtr>& ssts_from_source, // Passed by value (vector of shared_ptr)
                          const std::vector<SSTablePtr>& ssts_from_target_overlap) { // Passed by value
        
        if (ssts_from_source.empty()) return;
        int target_level_idx = source_level_idx + 1;

        if (target_level_idx >= max_levels_) { // Cannot compact from last level to a new one
             // Compaction within the last level could be implemented if needed.
            return; 
        }

        MemTable merged_data_map; // K-V pairs after merging, tombstones not yet removed

        auto load_map_from_sst_list = [&](const std::vector<SSTablePtr>& sst_list_to_load) {
            for (const auto& sst_ptr : sst_list_to_load) {
                for (const auto& pair : sst_ptr->data) {
                    MemTable::accessor acc;
                    merged_data_map.insert(acc, pair.first);
                    acc->second = pair.second;
                }
            }
        };
        
        // Process older data first (target level), then newer (source level)
        load_map_from_sst_list(ssts_from_target_overlap);
        load_map_from_sst_list(ssts_from_source);
        
        // Remove tombstones from the final merged data
        std::vector<KeyType> keys_to_erase;
        for (auto it = merged_data_map.begin(); it != merged_data_map.end(); ++it) {
            if (it->second == TOMBSTONE_VALUE) {
                keys_to_erase.push_back(it->first);
            }
        }
        for (const auto& k : keys_to_erase) {
            merged_data_map.erase(k);
        }

        std::vector<SSTablePtr> new_ssts_for_target;
        if (!merged_data_map.empty()) {
            MemTable current_sstable_build_data;
            for (const auto& pair : merged_data_map) {
                { // Explicit scope for accessor
                    MemTable::accessor acc;
                    current_sstable_build_data.insert(acc, pair.first);
                    acc->second = pair.second;
                } // acc is destroyed here
                if (current_sstable_build_data.size() >= sstable_target_entry_count_) {
                    SSTablePtr new_sst = SSTable::create_from_memtable(
                        current_sstable_build_data, next_sstable_id_++);
                    if (new_sst) new_ssts_for_target.push_back(new_sst);
                    current_sstable_build_data.clear(); // Now safe
                }
            }
            if (!current_sstable_build_data.empty()) { // Remainder
                SSTablePtr new_sst = SSTable::create_from_memtable(
                    current_sstable_build_data, next_sstable_id_++);
                if (new_sst) new_ssts_for_target.push_back(new_sst);
            }
        }

        // Atomically update levels_ metadata
        {
            std::unique_lock<std::shared_mutex> lock(levels_metadata_mutex_);
            
            auto remove_compacted_ssts = [&](std::vector<SSTablePtr>& level_vec, const std::vector<SSTablePtr>& compacted_ssts) {
                level_vec.erase(std::remove_if(level_vec.begin(), level_vec.end(),
                    [&](const SSTablePtr& sst_in_level) {
                        return std::find_if(compacted_ssts.begin(), compacted_ssts.end(),
                                       [&](const SSTablePtr& sst_to_remove){ return sst_to_remove->id == sst_in_level->id; }) 
                                       != compacted_ssts.end();
                    }), level_vec.end());
            };

            remove_compacted_ssts(levels_[source_level_idx], ssts_from_source);
            if (target_level_idx < max_levels_) {
                remove_compacted_ssts(levels_[target_level_idx], ssts_from_target_overlap);
                levels_[target_level_idx].insert(levels_[target_level_idx].end(),
                                                 new_ssts_for_target.begin(),
                                                 new_ssts_for_target.end());
                // Sort target level SSTables by min_key (crucial for L1+ non-overlapping property)
                 std::sort(levels_[target_level_idx].begin(), levels_[target_level_idx].end(), 
                           [](const SSTablePtr& a, const SSTablePtr& b) {
                    if (a->min_key != b->min_key) return a->min_key < b->min_key;
                    return a->id < b->id; 
                });
            }
        }
        // Old SSTable objects (now in-memory) will be destructed automatically when shared_ptr ref counts drop to zero.
    }
};

#endif // LSM_TREE_H