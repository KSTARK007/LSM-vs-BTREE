# FBTree Read Path Walkthrough

## Overview

FBTree (Feature B-Tree) is a concurrent B-tree implementation that uses optimistic concurrency control with versioning. The read path is designed to be lock-free for most operations, using version numbers to detect conflicts and retry when necessary.

## Key Components

### 1. Control Structure
Each node has a `Control` object that manages:
- **Version number**: Monotonic increasing counter for detecting modifications
- **Lock bit**: For exclusive access during modifications
- **Leaf bit**: Indicates if the node is a leaf
- **Sibling bit**: Indicates if the node has a right sibling
- **Order bit**: Indicates if key-value pairs in leaf are ordered
- **Delete bit**: Indicates if the node has been deleted
- **Split bits**: Used during node splitting operations

### 2. Node Types
- **InnerNode**: Internal nodes that route to child nodes
- **LeafNode**: Leaf nodes that store actual key-value pairs

## Read Path Execution

### Step 1: Entry Point - `lookup(K key)`

```cpp
KVPair* lookup(K key) {
    assert(epoch_->guarded());
    K cvt_key = encode_convert(key);
    void* node = root_;
    while(!is_leaf(node)) {
        inner(node)->to_next(cvt_key, node);
        node_prefetch(node);
    }
    // ... leaf node lookup
}
```

**What happens:**
1. **Epoch Guard**: Ensures the thread is protected by epoch-based memory reclamation
2. **Key Conversion**: Converts the key to a suitable encoding form for efficient comparison
3. **Tree Traversal**: Descends from root to leaf using inner node routing

### Step 2: Inner Node Traversal - `to_next(K key, void*& next)`

```cpp
bool to_next(K key, void*& next) {
    bool to_sibling;
    uint64_t init_version;
    
    while(true) { // Retry loop for atomicity
        to_sibling = false;
        init_version = control_.begin_read();
        
        if(control_.deleted()) {
            to_sibling = true, next = next_;
            break;
        }
        
        int pcmp = to_next_phase1(key, next, to_sibling);
        if(!pcmp) { // Key equals prefix
            // Use SIMD operations to find the correct child
            // ... feature comparison logic
        }
        
        if(control_.end_read(init_version)) break;
    }
    return to_sibling;
}
```

**What happens:**
1. **Optimistic Read**: Begins with `begin_read()` to get current version
2. **Deleted Check**: If node is deleted, jump to left sibling
3. **Prefix Comparison**: Compare key with node's prefix
4. **Feature Matching**: Use SIMD operations to find matching child
5. **Version Validation**: End read and retry if version changed

### Step 3: Leaf Node Lookup - `lookup(K key)`

```cpp
KVPair* lookup(K key) {
    char tag = hash(key); // Generate fingerprint
    uint64_t mask = bitmap_ & compare_equal(tags_, tag); // Find candidates
    
    while(mask) {
        int idx = index_least1(mask);
        KVPair* kv = kvs_[idx].load(load_order);
        if(kv != nullptr && key == kv->key) {
            return kv;
        }
        mask &= ~(0x01ul << idx);
    }
    return nullptr;
}
```

**What happens:**
1. **Hash Generation**: Create a fingerprint (tag) for the key
2. **Tag Matching**: Use SIMD to find slots with matching tags
3. **Key Verification**: Load actual key-value pairs and verify exact match
4. **Return Result**: Return the found KVPair or nullptr

### Step 4: Concurrent Access Control

#### Version-Based Optimistic Control

```cpp
uint64_t begin_read() {
    int spin = 0, limit = Config::kSpinInit;
    while(true) {
        uint64_t control = control_.load(load_order);
        if((control & kLockBit) == 0) {
            return control & kVersionMask;
        }
        // Spin or yield if locked
        if(spin++ >= limit) {
            std::this_thread::yield();
            spin = 0, limit += Config::kSpinInc;
        }
    }
}

bool end_read(uint64_t version) {
    uint64_t control = control_.load(load_order);
    while((control & kLockBit) != 0) {
        // Wait for lock to be released
        control = control_.load(load_order);
    }
    // Check if version changed during read
    return (control & kVersionMask) == version;
}
```

**What happens:**
1. **Begin Read**: Get current version, spin if node is locked
2. **Execute Read**: Perform the actual lookup operation
3. **End Read**: Validate that version hasn't changed
4. **Retry if Needed**: If version changed, retry the entire operation

## Key Optimizations

### 1. SIMD Operations
- **Tag-based Filtering**: Use SIMD to quickly filter candidate slots
- **Feature Comparison**: Parallel comparison of key features in inner nodes

### 2. Prefetching
```cpp
void node_prefetch(void* node) {
    if(Config::kNodePrefetch) {
        for(int i = 0; i < kPrefetchSize; i++)
            prefetcht0((char*) node + i * 64);
    }
}
```

### 3. Sibling Traversal
For range queries and when keys might be in sibling nodes:
```cpp
while(leaf(node)->to_sibling(key, node)) {
    version = control(node)->begin_read();
}
```

## Error Handling and Retry Logic

### 1. Version Conflicts
- If `end_read()` returns false, the entire lookup is retried
- This handles concurrent modifications gracefully

### 2. Node Deletion
- If a node is deleted during traversal, jump to the appropriate sibling
- Maintains correctness even during structural changes

### 3. Split Operations
- During splits, nodes may be in inconsistent state
- High key checks help determine if traversal should continue to sibling

## Performance Characteristics

### Lock-Free Reads
- Most reads proceed without acquiring locks
- Only retry when concurrent modifications are detected

### Cache-Friendly
- Prefetching of next nodes during traversal
- Compact node layouts with good cache locality

### Scalable
- Version-based conflict detection scales well with read-heavy workloads
- Minimal contention between readers

## Summary

The FBTree read path is designed for high concurrency and performance:

1. **Optimistic**: Uses version numbers to detect conflicts rather than locks
2. **Efficient**: Leverages SIMD operations and prefetching
3. **Correct**: Handles all concurrent scenarios including splits, merges, and deletions
4. **Scalable**: Lock-free reads allow excellent read scalability

The key insight is that most reads can proceed without synchronization, and the version mechanism efficiently detects when retries are necessary due to concurrent modifications. 