#pragma once

#include <iostream>
#include <vector>
#include <string>
#include <cstdint>

using KeyType = uint64_t;
using ValueType = std::string;

const ValueType TOMBSTONE_VALUE = std::string("%%__TOMBSTONE__%%");

#include <tbb/concurrent_hash_map.h>

class LSMTree;
