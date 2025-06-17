#pragma once
#include <vector>
#include <cstdint>
#include <functional>
#include <algorithm>

class RegisterBlockedBloomFilter {
public:
    RegisterBlockedBloomFilter(size_t num_blocks = 512, size_t num_hashes = 7)
        : num_blocks_(num_blocks), num_hashes_(num_hashes), blocks_(num_blocks, 0) {}

    void Insert(uint64_t key) {
        uint64_t hash = std::hash<uint64_t>{}(key);
        uint64_t* block = GetBlock(hash);
        *block |= ConstructMask(hash);
    }

    bool Query(uint64_t key) const {
        uint64_t hash = std::hash<uint64_t>{}(key);
        const uint64_t* block = GetBlock(hash);
        uint64_t mask = ConstructMask(hash);
        return ((*block) & mask) == mask;
    }

private:
    size_t num_blocks_;
    size_t num_hashes_;
    std::vector<uint64_t> blocks_;

    uint64_t* GetBlock(uint64_t hash) {
        size_t block_idx = ComputeHash(hash, 0) % num_blocks_;
        return &blocks_[block_idx];
    }
    const uint64_t* GetBlock(uint64_t hash) const {
        size_t block_idx = ComputeHash(hash, 0) % num_blocks_;
        return &blocks_[block_idx];
    }

    uint64_t ConstructMask(uint64_t hash) const {
        uint64_t mask = 0;
        for (size_t i = 1; i < num_hashes_; ++i) {
            uint64_t bit_pos = ComputeHash(hash, i) % 64;
            mask |= (1ull << bit_pos);
        }
        return mask;
    }

    uint64_t ComputeHash(uint64_t key, size_t i) const {
        // Use a simple hash combiner
        return std::hash<uint64_t>{}(key ^ (0x9e3779b9 * i));
    }
}; 