#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>

#define ENABLE_LEARNED_INDEX 0
constexpr size_t LEARNED_INDEX_TARGET_KEYS_PER_SEGMENT = 256;
constexpr size_t LEARNED_INDEX_MIN_KEYS_FOR_MULTISEGMENT = LEARNED_INDEX_TARGET_KEYS_PER_SEGMENT * 2;
constexpr size_t LEARNED_INDEX_MIN_KEYS_PER_SEGMENT_TRAINING = 5; // Increased for more stability



using KeyType = uint64_t;
using ValueType = std::string;

const ValueType TOMBSTONE_VALUE = std::string("%%__TOMBSTONE__%%");

#include <tbb/concurrent_hash_map.h>

class LSMTree;
