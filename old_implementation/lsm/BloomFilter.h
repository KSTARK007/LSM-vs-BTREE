#pragma once
#include <vector>
#include <functional>
#include <cstdint>

// Simple Bloom filter for integer keys
class BloomFilter {
public:
    BloomFilter(size_t size, size_t numHashes)
        : bits_(size), numHashes_(numHashes), size_(size) {}

    void add(uint64_t key) {
        for (size_t n = 0; n < numHashes_; ++n) {
            bits_[hash(key, n) % size_] = true;
        }
    }

    bool possiblyContains(uint64_t key) const {
        for (size_t n = 0; n < numHashes_; ++n) {
            if (!bits_[hash(key, n) % size_]) return false;
        }
        return true;
    }

private:
    std::vector<bool> bits_;
    size_t numHashes_;
    size_t size_;

    // Combine std::hash with a salt for multiple hash functions
    size_t hash(uint64_t key, size_t n) const {
        return std::hash<uint64_t>{}(key ^ (0x9e3779b9 * n));
    }
}; 