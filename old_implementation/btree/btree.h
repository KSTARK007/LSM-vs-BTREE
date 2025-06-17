#include <fcntl.h>
#include <numa.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>
#include <utility>
#include <vector>
#include <memory>
#include <shared_mutex>

#define MAX_RANGE_RESULTS 1000

int64_t cycles_to_nanoseconds(uint64_t cycles, double cpu_frequency_ghz) {
    return static_cast<int64_t>(cycles / cpu_frequency_ghz);
}

// -----------------------------------------------------------------------------
// B+ TREE CONSTANTS
// -----------------------------------------------------------------------------
static const size_t NODE_SIZE = 4096;  // 4 KB node size
static const uint8_t NODE_TYPE_INTERNAL = 0;
static const uint8_t NODE_TYPE_LEAF = 1;

static const int MAX_KEYS_INTERNAL = 120;
static const int MAX_KEYS_LEAF = 30;

// -----------------------------------------------------------------------------
// On-disk structures (packed)
// -----------------------------------------------------------------------------
#pragma pack(push, 1)
struct InternalNodeDisk {
    uint8_t nodeType;
    uint32_t numKeys;
    uint64_t childPtrs[MAX_KEYS_INTERNAL + 1];
    uint64_t keys[MAX_KEYS_INTERNAL];
    uint64_t padding[MAX_KEYS_INTERNAL * 2];  // padding to align to 16 bytes(Jiyu's Idea)
};

struct LeafRecord {
    uint64_t key_padding[2];  // padding to align to 16 bytes
    uint64_t key;
    char value[8];  // fixed 8-byte value
};

struct LeafNodeDisk {
    uint8_t nodeType;
    uint32_t numKeys;
    uint64_t nextLeaf;
    LeafRecord records[MAX_KEYS_LEAF];
};

struct Superblock {
    uint32_t magic;
    uint64_t rootNodeOffset;
    uint64_t currentEndOffset;
    char padding[4096 - sizeof(uint32_t) - 2 * sizeof(uint64_t)];  // Pad to 4KB
};
static_assert(sizeof(LeafRecord) <= NODE_SIZE, "LeafRecord size must be 16 bytes");
static_assert(sizeof(InternalNodeDisk) <= NODE_SIZE, "InternalNodeDisk size must be 16 bytes");
static_assert(sizeof(LeafNodeDisk) <= NODE_SIZE, "LeafNodeDisk size must be 16 bytes");
static_assert(sizeof(Superblock) <= NODE_SIZE, "Superblock size must be 16 bytes");
#pragma pack(pop)

// -----------------------------------------------------------------------------
// InsertResult struct for handling node splits
// -----------------------------------------------------------------------------
struct InsertResult {
    bool splitted;            // true if the child node was split
    uint64_t newChildOffset;  // offset of new sibling node
    uint64_t promotedKey;     // key to be promoted up to the parent
};

// -----------------------------------------------------------------------------
// In-memory Node Structures
// -----------------------------------------------------------------------------
struct InternalNode {
    uint32_t numKeys;
    std::vector<uint64_t> keys;
    std::vector<size_t> childIndices; // indices into the node vector
    InternalNode() : numKeys(0), keys(MAX_KEYS_INTERNAL), childIndices(MAX_KEYS_INTERNAL + 1, SIZE_MAX) {}
};

struct LeafNode {
    uint32_t numKeys;
    std::vector<uint64_t> keys;
    std::vector<std::string> values;
    size_t nextLeaf; // index of next leaf, or SIZE_MAX for null
    LeafNode() : numKeys(0), keys(MAX_KEYS_LEAF), values(MAX_KEYS_LEAF), nextLeaf(SIZE_MAX) {}
};

enum class NodeType { Internal, Leaf };

struct Node {
    NodeType type;
    std::unique_ptr<InternalNode> internal;
    std::unique_ptr<LeafNode> leaf;
    mutable std::shared_mutex node_mutex; // Fine-grained lock for this node
    Node(NodeType t) : type(t), internal(nullptr), leaf(nullptr) {
        if (t == NodeType::Internal) internal = std::make_unique<InternalNode>();
        else leaf = std::make_unique<LeafNode>();
    }
};

// -----------------------------------------------------------------------------
// BPlusTree Class (in-memory, index-based)
// -----------------------------------------------------------------------------
class BPlusTree {
public:
    BPlusTree() {
        // Create root as a leaf node
        rootIndex_ = allocateLeaf();
    }

    // Insert or update
    void put(uint64_t key, const std::string &value) {
        if (rootIndex_ == SIZE_MAX) {
            std::unique_lock<std::shared_mutex> treeLock(tree_mutex);
            rootIndex_ = allocateLeaf();
            std::unique_lock<std::shared_mutex> rootLock(nodes_[rootIndex_]->node_mutex);
            nodes_[rootIndex_]->leaf->keys[0] = key;
            nodes_[rootIndex_]->leaf->values[0] = value;
            nodes_[rootIndex_]->leaf->numKeys = 1;
            return;
        }
        InsertResult result = insertInternal(rootIndex_, key, value);
        if (result.splitted) {
            std::unique_lock<std::shared_mutex> treeLock(tree_mutex);
            uint64_t newRoot = allocateNode();
            std::unique_lock<std::shared_mutex> newRootLock(nodes_[newRoot]->node_mutex);
            nodes_[newRoot]->internal->keys[0] = result.promotedKey;
            nodes_[newRoot]->internal->childIndices[0] = rootIndex_;
            nodes_[newRoot]->internal->childIndices[1] = result.newChildOffset;
            nodes_[newRoot]->internal->numKeys = 1;
            rootIndex_ = newRoot;
        }
    }

    // Get
    bool get(uint64_t key, std::string &outValue) {
        if (rootIndex_ == SIZE_MAX) return false;
        return searchKey(rootIndex_, key, outValue);
    }

    std::vector<std::pair<uint64_t, std::string>> rangeQuery(uint64_t low, uint64_t high,
                                                             size_t max_results = MAX_RANGE_RESULTS) {
        std::vector<std::pair<uint64_t, std::string>> out;
        if (rootIndex_ == SIZE_MAX || low > high) return out;
        uint64_t leafOff = findLeafForKey(rootIndex_, low);
        while (leafOff != SIZE_MAX && out.size() < max_results) {
            std::shared_lock<std::shared_mutex> leafLock(nodes_[leafOff]->node_mutex);
            LeafNode *leaf = nodes_[leafOff]->leaf.get();
            for (uint32_t i = 0; i < leaf->numKeys && out.size() < max_results; ++i) {
                uint64_t k = leaf->keys[i];
                if (k < low) continue;
                if (k > high) return out;  // done
                out.emplace_back(k, leaf->values[i]);
            }
            leafOff = leaf->nextLeaf;
        }
        return out;
    }

    void print_btree(uint64_t nodeOffset, int level) {
        if (nodeOffset == SIZE_MAX) return;  // skip null nodes

        uint8_t nodeType = nodes_[nodeOffset]->type == NodeType::Internal ? NODE_TYPE_INTERNAL : NODE_TYPE_LEAF;
        if (nodeType == NODE_TYPE_LEAF) {
            return;  // skip leaf nodes
        } else {
            InternalNode *internal = nodes_[nodeOffset]->internal.get();
            std::cout << std::string(level * 2, ' ') << "Internal Node (offset: " << nodeOffset << "):\n";
            for (uint32_t i = 0; i < internal->numKeys; i++) {
                std::cout << std::string((level + 1) * 2, ' ') << "Key: " << internal->keys[i] << "\n";
            }
            for (uint32_t i = 0; i <= internal->numKeys; i++) {
                print_btree(internal->childIndices[i], level + 1);
            }
        }
    }

    void print_tree_stats() {
        std::cout << "B+ Tree Stats:\n";
        std::cout << "  Root Offset: " << rootIndex_ << "\n";
        std::cout << "  Node Size: " << NODE_SIZE << "\n";
        std::cout << "  Tree Depth: " << get_tree_depth(rootIndex_) << "\n";
        std::cout << "  Total Nodes: " << get_total_nodes(rootIndex_) << "\n";
        std::cout << "  Total internal nodes: " << get_total_internal_nodes(rootIndex_) << "\n";
        std::cout << "  Total leaf nodes: " << get_total_leaf_nodes(rootIndex_) << "\n";
        std::cout << "  Total Size (in MB): " << (get_total_nodes(rootIndex_) * NODE_SIZE) / (1024.0 * 1024.0) << "\n";
    }

    size_t rootIndex_ = SIZE_MAX;
    std::vector<std::unique_ptr<Node>> nodes_;
    mutable std::shared_mutex tree_mutex; // Optional global lock for the whole tree

    // Atomic counters for reads/writes
    std::atomic<int> total_reads{0};
    std::atomic<int> total_writes{0};
    std::vector<uint64_t> accessed_page_numbers;

private:
    // ------------------------------------------------
    // Stats helpers
    // ------------------------------------------------
    int get_tree_depth(uint64_t nodeOffset) {
        if (nodeOffset == SIZE_MAX) return 0;
        uint8_t nodeType = nodes_[nodeOffset]->type == NodeType::Internal ? NODE_TYPE_INTERNAL : NODE_TYPE_LEAF;
        if (nodeType == NODE_TYPE_LEAF) {
            return 1;
        } else {
            InternalNode *internal = nodes_[nodeOffset]->internal.get();
            int maxDepth = 0;
            for (uint32_t i = 0; i <= internal->numKeys; i++) {
                int depth = get_tree_depth(internal->childIndices[i]);
                if (depth > maxDepth) {
                    maxDepth = depth;
                }
            }
            return maxDepth + 1;
        }
    }
    int get_total_nodes(uint64_t nodeOffset) {
        if (nodeOffset == SIZE_MAX) return 0;
        uint8_t nodeType = nodes_[nodeOffset]->type == NodeType::Internal ? NODE_TYPE_INTERNAL : NODE_TYPE_LEAF;
        if (nodeType == NODE_TYPE_LEAF) {
            return 1;
        } else {
            InternalNode *internal = nodes_[nodeOffset]->internal.get();
            int total = 1;  // count this node
            for (uint32_t i = 0; i <= internal->numKeys; i++) {
                total += get_total_nodes(internal->childIndices[i]);
            }
            return total;
        }
    }
    int get_total_internal_nodes(uint64_t nodeOffset) {
        if (nodeOffset == SIZE_MAX) return 0;
        uint8_t nodeType = nodes_[nodeOffset]->type == NodeType::Internal ? NODE_TYPE_INTERNAL : NODE_TYPE_LEAF;
        if (nodeType == NODE_TYPE_LEAF) {
            return 0;
        } else {
            InternalNode *internal = nodes_[nodeOffset]->internal.get();
            int total = 1;  // count this node
            for (uint32_t i = 0; i <= internal->numKeys; i++) {
                total += get_total_internal_nodes(internal->childIndices[i]);
            }
            return total;
        }
    }
    int get_total_leaf_nodes(uint64_t nodeOffset) {
        if (nodeOffset == SIZE_MAX) return 0;
        uint8_t nodeType = nodes_[nodeOffset]->type == NodeType::Internal ? NODE_TYPE_INTERNAL : NODE_TYPE_LEAF;
        if (nodeType == NODE_TYPE_LEAF) {
            return 1;
        } else {
            InternalNode *internal = nodes_[nodeOffset]->internal.get();
            int total = 0;
            for (uint32_t i = 0; i <= internal->numKeys; i++) {
                total += get_total_leaf_nodes(internal->childIndices[i]);
            }
            return total;
        }
    }
    // ------------------------------------------------
    // Low-level I/O
    // ------------------------------------------------
    uint64_t allocateNode() {
        uint64_t newOffset = nodes_.size();
        nodes_.emplace_back(std::make_unique<Node>(NodeType::Internal));
        return newOffset;
    }

    uint64_t allocateLeaf() {
        uint64_t newOffset = nodes_.size();
        nodes_.emplace_back(std::make_unique<Node>(NodeType::Leaf));
        return newOffset;
    }

    // ------------------------------------------------
    // Search
    // ------------------------------------------------
    bool searchKey(uint64_t nodeOffset, uint64_t key, std::string &outValue) {
        if (nodeOffset == SIZE_MAX) return false;
        std::shared_lock<std::shared_mutex> nodeLock(nodes_[nodeOffset]->node_mutex);
        uint8_t nodeType = nodes_[nodeOffset]->type == NodeType::Internal ? NODE_TYPE_INTERNAL : NODE_TYPE_LEAF;
        if (nodeType == NODE_TYPE_LEAF) {
            LeafNode *leaf = nodes_[nodeOffset]->leaf.get();
            for (uint32_t i = 0; i < leaf->numKeys; i++) {
                if (leaf->keys[i] == key) {
                    outValue = leaf->values[i];
                    return true;
                }
            }
            return false;
        } else {
            InternalNode *internal = nodes_[nodeOffset]->internal.get();
            int i = 0;
            while (i < (int)internal->numKeys && key >= internal->keys[i]) {
                i++;
            }
            return searchKey(internal->childIndices[i], key, outValue);
        }
    }
    uint64_t findLeafForKey(uint64_t nodeOffset, uint64_t key) {
        if (nodeOffset == SIZE_MAX) return SIZE_MAX;
        std::shared_lock<std::shared_mutex> nodeLock(nodes_[nodeOffset]->node_mutex);
        uint8_t nodeType = nodes_[nodeOffset]->type == NodeType::Internal ? NODE_TYPE_INTERNAL : NODE_TYPE_LEAF;
        if (nodeType == NODE_TYPE_LEAF) {
            return nodeOffset;
        }
        InternalNode *internal = nodes_[nodeOffset]->internal.get();
        int i = 0;
        while (i < static_cast<int>(internal->numKeys) && key >= internal->keys[i]) ++i;
        return findLeafForKey(internal->childIndices[i], key);
    }

    // ------------------------------------------------
    // Insert Internal
    // ------------------------------------------------
    InsertResult insertInternal(uint64_t nodeOffset, uint64_t key, const std::string &val) {
        if (nodeOffset == SIZE_MAX) {
            std::unique_lock<std::shared_mutex> treeLock(tree_mutex);
            rootIndex_ = allocateLeaf();
            std::unique_lock<std::shared_mutex> rootLock(nodes_[rootIndex_]->node_mutex);
            nodes_[rootIndex_]->leaf->keys[0] = key;
            nodes_[rootIndex_]->leaf->values[0] = val;
            nodes_[rootIndex_]->leaf->numKeys = 1;
            return {false, 0, 0};
        }
        std::unique_lock<std::shared_mutex> nodeLock(nodes_[nodeOffset]->node_mutex);
        uint8_t nodeType = nodes_[nodeOffset]->type == NodeType::Internal ? NODE_TYPE_INTERNAL : NODE_TYPE_LEAF;
        if (nodeType == NODE_TYPE_LEAF) {
            return insertLeaf(nodeOffset, nodes_[nodeOffset]->leaf.get(), key, val);
        } else {
            return insertIntoInternal(nodeOffset, nodes_[nodeOffset]->internal.get(), key, val);
        }
    }

    // ------------------------------------------------
    // Insert into a Leaf
    // ------------------------------------------------
    InsertResult insertLeaf(uint64_t leafOffset, LeafNode *leaf, uint64_t key, const std::string &val) {
        InsertResult res{};
        res.splitted = false;
        for (uint32_t i = 0; i < leaf->numKeys; i++) {
            if (leaf->keys[i] == key) {
                leaf->values[i] = val;
                return res;  // no split
            }
        }
        if (leaf->numKeys < MAX_KEYS_LEAF) {
            int pos = leaf->numKeys;
            while (pos > 0 && leaf->keys[pos - 1] > key) {
                leaf->keys[pos] = leaf->keys[pos - 1];
                leaf->values[pos] = leaf->values[pos - 1];
                pos--;
            }
            leaf->keys[pos] = key;
            leaf->values[pos] = val;
            leaf->numKeys++;
            return res;
        } else {
            return splitLeaf(leafOffset, leaf, key, val);
        }
    }

    InsertResult splitLeaf(uint64_t leafOffset, LeafNode *leaf, uint64_t key, const std::string &val) {
        // Gather all keys/values including the new one
        std::vector<uint64_t> tmpKeys(leaf->numKeys);
        std::vector<std::string> tmpValues(leaf->numKeys);
        for (uint32_t i = 0; i < leaf->numKeys; i++) {
            tmpKeys[i] = leaf->keys[i];
            tmpValues[i] = leaf->values[i];
        }
        // Insert the new key/value in sorted order
        size_t pos = tmpKeys.size();
        for (size_t i = 0; i < tmpKeys.size(); ++i) {
            if (key < tmpKeys[i]) {
                pos = i;
                break;
            }
        }
        tmpKeys.insert(tmpKeys.begin() + pos, key);
        tmpValues.insert(tmpValues.begin() + pos, val);
        // Now tmpKeys.size() == MAX_KEYS_LEAF + 1
        uint64_t newLeafOffset = allocateLeaf();
        std::unique_lock<std::shared_mutex> newLeafLock(nodes_[newLeafOffset]->node_mutex);
        LeafNode* newLeaf = nodes_[newLeafOffset]->leaf.get();
        size_t split = tmpKeys.size() / 2;
        // Assign first half to original leaf
        leaf->numKeys = split;
        for (size_t i = 0; i < split; i++) {
            leaf->keys[i] = tmpKeys[i];
            leaf->values[i] = tmpValues[i];
        }
        // Assign second half to new leaf
        newLeaf->numKeys = tmpKeys.size() - split;
        for (size_t i = 0; i < newLeaf->numKeys; i++) {
            newLeaf->keys[i] = tmpKeys[split + i];
            newLeaf->values[i] = tmpValues[split + i];
        }
        newLeaf->nextLeaf = leaf->nextLeaf;
        leaf->nextLeaf = newLeafOffset;
        InsertResult res;
        res.splitted = true;
        res.newChildOffset = newLeafOffset;
        res.promotedKey = newLeaf->keys[0]; // promote the first key of the new leaf
        return res;
    }

    // ------------------------------------------------
    // Insert into an Internal node
    // ------------------------------------------------
    InsertResult insertIntoInternal(uint64_t nodeOffset, InternalNode *internal, uint64_t key,
                                    const std::string &val) {
        InsertResult res{};
        res.splitted = false;
        int i = 0;
        while (i < (int)internal->numKeys && key >= internal->keys[i]) {
            i++;
        }
        InsertResult cRes = insertInternal(internal->childIndices[i], key, val);
        if (!cRes.splitted) {
            return res;
        }
        if ((int)internal->numKeys < MAX_KEYS_INTERNAL) {
            for (int j = internal->numKeys; j > i; j--) {
                internal->keys[j] = internal->keys[j - 1];
                internal->childIndices[j + 1] = internal->childIndices[j];
            }
            internal->keys[i] = cRes.promotedKey;
            internal->childIndices[i + 1] = cRes.newChildOffset;
            internal->numKeys++;
            return res;
        } else {
            return splitInternal(nodeOffset, internal, i, cRes);
        }
    }

    InsertResult splitInternal(uint64_t nodeOffset, InternalNode *node, int childIndex, const InsertResult &cRes) {
        // Gather all keys/children including the new one
        std::vector<uint64_t> tmpKeys(node->numKeys);
        std::vector<size_t> tmpChildIndices(node->numKeys + 1);
        for (uint32_t i = 0; i < node->numKeys; i++) {
            tmpKeys[i] = node->keys[i];
        }
        for (uint32_t i = 0; i <= node->numKeys; i++) {
            tmpChildIndices[i] = node->childIndices[i];
        }
        tmpKeys.insert(tmpKeys.begin() + childIndex, cRes.promotedKey);
        tmpChildIndices.insert(tmpChildIndices.begin() + (childIndex + 1), cRes.newChildOffset);
        // Now tmpKeys.size() == MAX_KEYS_INTERNAL + 1
        size_t totalKeys = tmpKeys.size();
        size_t midIndex = totalKeys / 2;
        uint64_t promotedKey = tmpKeys[midIndex];
        size_t leftCount = midIndex;
        size_t rightCount = totalKeys - (leftCount + 1);
        // Assign left half to original node
        node->numKeys = (uint32_t)leftCount;
        for (size_t i = 0; i < leftCount; i++) {
            node->keys[i] = tmpKeys[i];
            node->childIndices[i] = tmpChildIndices[i];
        }
        node->childIndices[leftCount] = tmpChildIndices[leftCount];
        // Assign right half to new node
        uint64_t newOffset = allocateNode();
        std::unique_lock<std::shared_mutex> newNodeLock(nodes_[newOffset]->node_mutex);
        InternalNode* newNode = nodes_[newOffset]->internal.get();
        newNode->numKeys = (uint32_t)rightCount;
        for (size_t i = 0; i < rightCount; i++) {
            newNode->keys[i] = tmpKeys[midIndex + 1 + i];
            newNode->childIndices[i] = tmpChildIndices[midIndex + 1 + i];
        }
        newNode->childIndices[rightCount] = tmpChildIndices[midIndex + 1 + rightCount];
        InsertResult res;
        res.splitted = true;
        res.promotedKey = promotedKey;
        res.newChildOffset = newOffset;
        return res;
    }
};