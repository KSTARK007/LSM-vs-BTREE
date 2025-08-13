#include "memtable_interface.h"
#include "global.h"

#ifdef USE_CUSTOM_SKIPLIST
#include "custom_skiplist_memtable.h"
#endif

#ifdef USE_ORIGINAL_SKIPLIST
#include "skiplist_memtable.h"
#endif

namespace LSM {

std::unique_ptr<MemtableInterface> createMemtable(size_t max_size) {
#ifdef USE_CUSTOM_SKIPLIST
    return std::make_unique<CustomSkipListMemtable>(max_size);
#endif

#ifdef USE_ORIGINAL_SKIPLIST
    return std::make_unique<SkipListMemtable>(max_size);
#endif

    // Fallback - should not reach here due to compile-time checks in global.h
    throw std::runtime_error("No memtable implementation selected");
}

} // namespace LSM 