#include <x86intrin.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <random>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <string>
#include <numa.h>
#include <pthread.h>
#include <sched.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <iomanip>
#include <unordered_map>

#include "../memtable/memtable_interface.h"
#include "../../util/benchmark_helper.h"
#include "/mydata/LSM-vs-BTREE/zipf_implementation.h"
#include <cassert>
#include "../../util/numa_alloc.h"

#define ZIPF_CONST 0.99

#ifndef CPU_FREQ_GHZ
#define CPU_FREQ_GHZ 2.1  // hardcode cpu frequency for precise timing
#endif

// Benchmark configuration
constexpr int TOTAL_KEYS = 500000;   // 500K keys to leave room for benchmark operations
constexpr int TOTAL_OPS = 50000;
constexpr int NUM_EXEC_NODES = 3;
constexpr int NUM_THREADS = 4;
constexpr int total_runtime = 1;  // seconds

// Global variables
std::atomic<int> total_reads(0);
std::atomic<int> total_writes(0);
std::atomic<int> total_failures(0);
std::string results_FILE = "memtable_benchmark.csv";

// Global storage for YCSB string keys
std::vector<std::string> ycsb_key_storage;

// Worker thread for operations
void worker_get(const std::vector<std::pair<uint64_t, char>>& ops, int start, int end, int thread_id, 
                LSM::MemtableInterface* memtable, std::vector<double>& local_read_latencies, 
                std::vector<double>& local_write_latencies) {
    {
        struct bitmask* cpus = numa_allocate_cpumask();
        numa_node_to_cpus((thread_id % NUM_EXEC_NODES) + 1, cpus);

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        for (int i = 0; i < cpus->size; ++i) {
            if (numa_bitmask_isbitset(cpus, i)) {
                CPU_SET(i, &cpuset);
            }
        }

        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);

        uint32_t numa_node = UINT32_MAX;
        int rc_getcpu = getcpu(nullptr, &numa_node);
        assert(rc_getcpu != -1 && "getcpu() failed");
        assert(numa_node == (thread_id % NUM_EXEC_NODES) + 1 && "not running on target node");
    }
    
    local_read_latencies.clear();
    local_write_latencies.clear();
    uint64_t t1, t2;
    std::string val_to_insert = generate_random_value();
    auto start_thread_time = std::chrono::high_resolution_clock::now();
    auto end_thread_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_thread_time - start_thread_time;
    uint32_t tsc_aux;
    double write_ratio = 0.0;
    
    // Set write ratio based on workload type
    if (results_FILE == "a.csv") {
        write_ratio = 0.5;
    } else if (results_FILE == "b.csv") {
        write_ratio = 0.05;
    } else {
        write_ratio = 0.0; // 10% updates, 90% reads for memtable testing
    }
    
    ScrambledZipfianGenerator zipf(TOTAL_KEYS, ZIPF_CONST, write_ratio);
    std::string key = "";
    char op = 'R';
    std::string result_value;

    uint64_t local_failures = 0;
    while (elapsed_seconds.count() < total_runtime) {
        for (int i = start; i < end; ++i) {
            if (elapsed_seconds.count() > total_runtime) {
                break;
            }
            op = zipf.get_op();
            // For reads, use keys that are more likely to exist in the memtable
            if (op == 'R') {
                // Use keys from the existing data (0 to memtable size)
                uint64_t key_index = zipf.Next() % memtable->size();
                key = generate_ycsb_like_key(key_index);
                
                t1 = __rdtscp(&tsc_aux);
                bool found = memtable->get(key, result_value);
                t2 = __rdtscp(&tsc_aux);
                if (!found) {
                    local_failures++;
                }
                auto duration = cycles_to_nanoseconds(t2 - t1, CPU_FREQ_GHZ);
                local_read_latencies.push_back(duration);
            } else if (op == 'U' || op == 'I') {
                // For updates, use existing keys to avoid memtable overflow
                uint64_t key_index = zipf.Next() % memtable->size();
                key = generate_ycsb_like_key(key_index);
                
                t1 = __rdtscp(&tsc_aux);
                bool success = memtable->put(key, val_to_insert); // This will be an update
                t2 = __rdtscp(&tsc_aux);
                if (!success) {
                    local_failures++;
                }
                auto duration = cycles_to_nanoseconds(t2 - t1, CPU_FREQ_GHZ);
                local_write_latencies.push_back(duration);
            }
        }
        end_thread_time = std::chrono::high_resolution_clock::now();
        elapsed_seconds = end_thread_time - start_thread_time;
    }

    total_failures.fetch_add(local_failures, std::memory_order_relaxed);
    std::cout << "Thread " << thread_id << " failures: " << local_failures << "\n";
}

// The benchmark
void benchmark(int num_threads, const std::vector<std::pair<uint64_t, std::string>>& data,
               const std::vector<std::pair<uint64_t, char>>& ops, CSVLogger& logger, 
               CSVLogger& pagenumbers, LSM::MemtableInterface* memtable) {
    std::vector<std::thread> threads;
    threads.reserve(num_threads);
    std::vector<std::vector<double>> thread_read_latencies(num_threads);
    std::vector<std::vector<double>> thread_write_latencies(num_threads);
    int chunk = TOTAL_OPS / num_threads;
    auto startTime = std::chrono::high_resolution_clock::now();
    
    for (int i = 0; i < num_threads; ++i) {
        int beg = i * chunk;
        int end = (i == num_threads - 1) ? TOTAL_OPS : (i + 1) * chunk;
        threads.emplace_back([&, i, beg, end]() {
            std::vector<double> local_read_latencies;
            std::vector<double> local_write_latencies;
            worker_get(ops, beg, end, i, memtable, local_read_latencies, local_write_latencies);
            thread_read_latencies[i] = local_read_latencies;
            thread_write_latencies[i] = local_write_latencies;
        });
    }
    
    for (auto& t : threads) {
        t.join();
    }
    auto endTime = std::chrono::high_resolution_clock::now();

    // Merge thread-local stats
    std::vector<double> all_read_latencies, all_write_latencies;
    for (int i = 0; i < num_threads; ++i) {
        all_read_latencies.insert(all_read_latencies.end(), thread_read_latencies[i].begin(), thread_read_latencies[i].end());
        all_write_latencies.insert(all_write_latencies.end(), thread_write_latencies[i].begin(), thread_write_latencies[i].end());
    }
    
    double elapsedSec = std::chrono::duration<double>(endTime - startTime).count();
    double sumReadLat = 0.0;
    for (double lat : all_read_latencies) sumReadLat += lat;
    double avgReadLat = all_read_latencies.empty() ? 0.0 : sumReadLat / all_read_latencies.size();
    double Readthroughput = 0.0;
    if (avgReadLat > 0) {
        Readthroughput = (double)all_read_latencies.size() / ((sumReadLat / num_threads) * 1e-9);  // ops/sec
    }
    
    double sumWriteLat = 0.0;
    for (double lat : all_write_latencies) sumWriteLat += lat;
    double avgWriteLat = all_write_latencies.empty() ? 0.0 : sumWriteLat / all_write_latencies.size();
    double Writethroughput;
    if (avgWriteLat > 0)
        Writethroughput = (double)all_write_latencies.size() / ((sumWriteLat / num_threads) * 1e-9);  // ops/sec
    else
        Writethroughput = 0.0;
    
    double total_sum = 0.0;
    for (double lat : all_write_latencies) total_sum += lat;
    for (double lat : all_read_latencies) total_sum += lat;
    
    if (all_read_latencies.empty() && all_write_latencies.empty()) {
        std::cout << "No read or write operations performed.\n";
        return;
    }
    
    double avgLat = total_sum / (all_write_latencies.size() + all_read_latencies.size());
    double throughput = (double)(all_write_latencies.size() + all_read_latencies.size()) /
                        ((total_sum / num_threads) * 1e-9);  // ops/sec
    
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "GET | Threads: " << num_threads << " | Avg Latency: " << avgReadLat << " ns/op\n";
    std::cout << "PUT | Threads: " << num_threads << " | Avg Latency: " << avgWriteLat << " ns/op\n";
    std::cout << "Total | Threads: " << num_threads << " | Throughput: " << throughput << " ops/s"
              << " | Avg Latency: " << avgLat << " ns/op\n";
    std::cout << "Total Failures: " << total_failures.load() << "\n";

    logger.writeRow({std::to_string(num_threads), std::to_string(throughput), std::to_string(avgLat),
                     std::to_string(avgReadLat), std::to_string(avgWriteLat)});
}

// main
int main(int argc, char* argv[]) {
    if (argc > 2 && argv[2] != nullptr) {
        results_FILE = argv[2];
    }
    
    // numa_alloc_init();
    std::cout << "Results file: " << results_FILE << "\n";
    std::string log_path = "/mydata/LSM-vs-BTREE/lsm_results/" + results_FILE;
    
    // Create results directory if it doesn't exist
    system("mkdir -p /mydata/LSM-vs-BTREE/lsm_results");
    
    CSVLogger logger(log_path, {"Thread Count", "Throughput (ops/s)", "Avg Latency (ns/op)", "Avg Read Latency (ops/s)",
                                "Avg Write Latency (ns/op)"});
    CSVLogger pagenumbers("/mydata/pages.csv", {"page numbers"});
    if (!pagenumbers.file_.is_open()) {
        std::cerr << "Failed to open CSV file: /mydata/pages.csv. Please check that the directory exists and is writable.\n";
    }

    numa_set_strict(1);
    struct bitmask* cpus = numa_allocate_cpumask();
    numa_node_to_cpus((1 % NUM_EXEC_NODES) + 1, cpus);
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    for (int i = 0; i < cpus->size; ++i) {
        if (numa_bitmask_isbitset(cpus, i)) {
            CPU_SET(i, &cpuset);
        }
    }

    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    
    try {
        // Create memtable with 64MB size limit using factory
        auto memtable = LSM::createMemtable(64 * 1024 * 1024);
        
        std::cout << "Creating SkipList memtable and inserting YCSB-style data...\n";
        
        // Populate the global key storage
        ycsb_key_storage.clear();
        ycsb_key_storage.reserve(TOTAL_KEYS);
        
        // Generate random value for all keys
        std::string random_value(100, 'a'); // Simple fixed value for now
        
        // Insert data into memtable
        for (uint64_t i = 0; i < TOTAL_KEYS; i++) {
            // Generate YCSB-style key string exactly like YCSB BuildKeyName
            std::string ycsb_key = generate_ycsb_like_key(i);
            
            // Store the YCSB key in global storage
            ycsb_key_storage.push_back(ycsb_key);
            
            // Insert into memtable
            bool success = memtable->put(ycsb_key, random_value);
            if (!success) {
                std::cout << "Memtable full after " << i << " inserts\n";
                break;
            }
            
            if (i % 100000 == 0) {
                std::cout << "Inserted " << i << " keys..." << std::endl;
            }
        }
        
        std::cout << "Inserted " << memtable->size() << " YCSB-style key/value pairs.\n";
        std::cout << "Memory usage: " << memtable->memoryUsage() << " bytes\n";

        // Create dummy data structure for compatibility
        std::vector<std::pair<uint64_t, std::string>> dummy_data(TOTAL_KEYS);
        for (uint64_t i = 0; i < TOTAL_KEYS; i++) {
            dummy_data[i] = {i, random_value};
        }
        
        auto ops = generate_random_ops_just_one(dummy_data);

        if (argc > 1) {
            int Number_of_threads = std::stoi(argv[1]);
            benchmark(Number_of_threads, dummy_data, ops, logger, pagenumbers, memtable.get());
        } else {
            std::cout << "No thread number provided. Running with 1 thread.\n";
            benchmark(1, dummy_data, ops, logger, pagenumbers, memtable.get());
        }
        
        // Print memtable statistics at the end of benchmark
        memtable->printStats();
        
        std::cout << "Done.\n";
    } catch (const std::exception& ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
    }
    return 0;
} 