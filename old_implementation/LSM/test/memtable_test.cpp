#include <iostream>
#include <cassert>
#include <string>
#include <vector>
#include <random>
#include <thread>
#include <chrono>
#include "../memtable/skiplist_memtable.h"

using namespace LSM;

// Test basic operations
void test_basic_operations() {
    std::cout << "Testing basic operations..." << std::endl;
    
    SkipListMemtable memtable(1024 * 1024); // 1MB limit
    
    // Test put and get
    assert(memtable.put("key1", "value1"));
    assert(memtable.put("key2", "value2"));
    assert(memtable.put("key3", "value3"));
    
    std::string value;
    assert(memtable.get("key1", value));
    assert(value == "value1");
    
    assert(memtable.get("key2", value));
    assert(value == "value2");
    
    assert(memtable.get("key3", value));
    assert(value == "value3");
    
    // Test non-existent key
    assert(!memtable.get("nonexistent", value));
    
    // Test update
    assert(memtable.put("key1", "new_value1"));
    assert(memtable.get("key1", value));
    assert(value == "new_value1");
    
    // Test delete
    assert(memtable.delete_key("key2"));
    assert(!memtable.get("key2", value)); // Should not find deleted key
    
    std::cout << "Basic operations test passed!" << std::endl;
}

// Test memory limits
void test_memory_limits() {
    std::cout << "Testing memory limits..." << std::endl;
    
    SkipListMemtable memtable(1000); // Very small limit
    
    std::string large_value(500, 'x'); // 500 bytes
    
    // Should fail to insert due to memory limit
    assert(!memtable.put("key1", large_value));
    assert(!memtable.put("key2", large_value));
    
    std::cout << "Memory limits test passed!" << std::endl;
}

// Test batch operations
void test_batch_operations() {
    std::cout << "Testing batch operations..." << std::endl;
    
    SkipListMemtable memtable(1024 * 1024);
    
    std::vector<KeyValue> batch;
    for (int i = 0; i < 100; ++i) {
        batch.emplace_back("batch_key_" + std::to_string(i), 
                          "batch_value_" + std::to_string(i));
    }
    
    assert(memtable.putBatch(batch));
    
    // Verify all keys were inserted
    std::string value;
    for (int i = 0; i < 100; ++i) {
        assert(memtable.get("batch_key_" + std::to_string(i), value));
        assert(value == "batch_value_" + std::to_string(i));
    }
    
    std::cout << "Batch operations test passed!" << std::endl;
}

// Test iterator
void test_iterator() {
    std::cout << "Testing iterator..." << std::endl;
    
    SkipListMemtable memtable(1024 * 1024);
    
    // Insert some data
    for (int i = 0; i < 10; ++i) {
        memtable.put("iter_key_" + std::to_string(i), 
                    "iter_value_" + std::to_string(i));
    }
    
    // Test iteration
    int count = 0;
    auto it = memtable.begin();
    auto end = memtable.end();
    for (; it->operator!=(*end); it->operator++()) {
        KeyValue kv = it->operator*();
        assert(kv.key.find("iter_key_") == 0);
        assert(kv.value.find("iter_value_") == 0);
        count++;
    }
    
    assert(count == 10);
    
    std::cout << "Iterator test passed!" << std::endl;
}

// Test concurrent operations
void test_concurrent_operations() {
    std::cout << "Testing concurrent operations..." << std::endl;
    
    SkipListMemtable memtable(1024 * 1024);
    std::atomic<int> success_count(0);
    std::atomic<int> failure_count(0);
    
    // Worker function
    auto worker = [&](int thread_id) {
        for (int i = 0; i < 1000; ++i) {
            std::string key = "thread_" + std::to_string(thread_id) + "_key_" + std::to_string(i);
            std::string value = "thread_" + std::to_string(thread_id) + "_value_" + std::to_string(i);
            
            if (memtable.put(key, value)) {
                success_count.fetch_add(1);
            } else {
                failure_count.fetch_add(1);
            }
        }
    };
    
    // Start multiple threads
    std::vector<std::thread> threads;
    for (int i = 0; i < 4; ++i) {
        threads.emplace_back(worker, i);
    }
    
    // Wait for all threads to complete
    for (auto& t : threads) {
        t.join();
    }
    
    std::cout << "Concurrent operations completed. Success: " << success_count.load() 
              << ", Failures: " << failure_count.load() << std::endl;
    
    // Verify some of the inserted data
    std::string value;
    int verified = 0;
    for (int thread_id = 0; thread_id < 4; ++thread_id) {
        for (int i = 0; i < 100; ++i) {
            std::string key = "thread_" + std::to_string(thread_id) + "_key_" + std::to_string(i);
            if (memtable.get(key, value)) {
                verified++;
            }
        }
    }
    
    std::cout << "Verified " << verified << " entries" << std::endl;
    std::cout << "Concurrent operations test passed!" << std::endl;
}

// Test statistics
void test_statistics() {
    std::cout << "Testing statistics..." << std::endl;
    
    SkipListMemtable memtable(1024 * 1024);
    
    // Insert some data
    for (int i = 0; i < 100; ++i) {
        memtable.put("stat_key_" + std::to_string(i), "stat_value_" + std::to_string(i));
    }
    
    // Perform some operations
    std::string value;
    for (int i = 0; i < 50; ++i) {
        memtable.get("stat_key_" + std::to_string(i), value);
    }
    
    for (int i = 0; i < 20; ++i) {
        memtable.delete_key("stat_key_" + std::to_string(i));
    }
    
    // Check statistics
    assert(memtable.getTotalInserts() >= 100);
    assert(memtable.getTotalLookups() >= 50);
    assert(memtable.getTotalDeletes() >= 20);
    assert(memtable.size() >= 80); // At least 80 entries remaining
    
    std::cout << "Statistics test passed!" << std::endl;
}

// Test validation
void test_validation() {
    std::cout << "Testing validation..." << std::endl;
    
    SkipListMemtable memtable(1024 * 1024);
    
    // Empty memtable should be valid
    assert(memtable.validate());
    
    // Insert some data
    for (int i = 0; i < 100; ++i) {
        memtable.put("valid_key_" + std::to_string(i), "valid_value_" + std::to_string(i));
    }
    
    // Should still be valid
    assert(memtable.validate());
    
    std::cout << "Validation test passed!" << std::endl;
}

// Performance test
void test_performance() {
    std::cout << "Testing performance..." << std::endl;
    
    SkipListMemtable memtable(64 * 1024 * 1024); // 64MB
    
    // Measure insert performance
    auto start = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < 100000; ++i) {
        memtable.put("perf_key_" + std::to_string(i), "perf_value_" + std::to_string(i));
    }
    
    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Inserted 100,000 keys in " << duration.count() << " ms" << std::endl;
    std::cout << "Insert rate: " << (100000.0 / duration.count()) * 1000 << " ops/sec" << std::endl;
    
    // Measure lookup performance
    start = std::chrono::high_resolution_clock::now();
    
    std::string value;
    for (int i = 0; i < 100000; ++i) {
        memtable.get("perf_key_" + std::to_string(i), value);
    }
    
    end = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    
    std::cout << "Looked up 100,000 keys in " << duration.count() << " ms" << std::endl;
    std::cout << "Lookup rate: " << (100000.0 / duration.count()) * 1000 << " ops/sec" << std::endl;
    
    std::cout << "Performance test completed!" << std::endl;
}

int main() {
    std::cout << "Starting memtable tests..." << std::endl;
    
    try {
        test_basic_operations();
        test_memory_limits();
        test_batch_operations();
        test_iterator();
        test_concurrent_operations();
        test_statistics();
        test_validation();
        test_performance();
        
        std::cout << "\nAll tests passed!" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Test failed with unknown exception" << std::endl;
        return 1;
    }
} 