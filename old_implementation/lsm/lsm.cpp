#include <x86intrin.h> // For __rdtscp
#include <numa.h>      // For NUMA operations
#include <sched.h>     // For cpu_set_t, CPU_ZERO, CPU_SET
#include <pthread.h>   // For pthread_setaffinity_np

#include <fstream>
#include <iostream>
#include <vector>
#include <string>
#include <sstream>
#include <random>
#include <chrono>
#include <thread>
#include <atomic>
#include <mutex>
#include <iomanip>      // For std::fixed, std::setprecision
#include <numeric>      // For std::accumulate
#include <algorithm>    // For std::sort, std::shuffle
#include <cassert>      // For assert

#include "lsm_tree.h"             // Include the In-Memory LSM Tree
#include "../zipf_implementation.h"  // Assumed to be provided by user

// Helper for benchmark if common.h is not available or doesn't have it
#ifndef CYCLES_TO_NANOSECONDS_H
#define CYCLES_TO_NANOSECONDS_H
#include <cstdint>
static inline double cycles_to_nanoseconds(uint64_t cycles, double cpu_freq_ghz) {
    if (cpu_freq_ghz == 0.0) return 0.0; 
    return static_cast<double>(cycles) / cpu_freq_ghz;
}
#endif


#define ZIPF_CONST 1.1

#ifndef CPU_FREQ_GHZ
#define CPU_FREQ_GHZ 2.1  // hardcode cpu frquency for precise timing
#endif
// ----------------------------------------------------------------
// Benchmark Code (adapted from user's prompt)
// ----------------------------------------------------------------

constexpr int TOTAL_KEYS = 20000000; // User's original value
constexpr int TOTAL_OPS = 5000000;   // User's original value (though benchmark now runs for fixed time)
constexpr int NUM_EXEC_NODES = 3;    // User's original value
constexpr int NUM_THREADS = 4;       // User's original value
constexpr int VALUE_SIZE = 8;
constexpr int total_runtime = 10;  // seconds

const std::string YCSB_FILE = "/mydata/ycsb/c"; // User's original path
std::string results_FILE = "c.csv";             // Default, can be changed by arg

std::atomic<long long> total_global_reads(0); 
std::atomic<long long> total_global_writes(0);
std::mutex latency_mutex; // Global mutex for latency vectors if needed, but local is better
// std::vector<uint64_t> total_accessed_page_numbers; // Not applicable to this LSM

class CSVLogger {
public:
    std::mutex csv_mutex; // Renamed to avoid conflict
    std::ofstream file_stream; // Renamed

    CSVLogger(const std::string& filename, const std::vector<std::string>& header) {
        bool file_exists = false;
        {
            std::ifstream f(filename.c_str());
            file_exists = f.good();
        }
        
        file_stream.open(filename, std::ios::out | std::ios::app);
        if (!file_stream.is_open()) {
            std::cerr << "Failed to open CSV file: " << filename << std::endl;
        }
        if (!file_exists && file_stream.is_open()) {
            writeRow(header);
        }
    }

    ~CSVLogger() {
        if (file_stream.is_open()) file_stream.close();
    }

    void writeRow(const std::vector<std::string>& row) {
        if (!file_stream.is_open()) return;
        std::lock_guard<std::mutex> lock(csv_mutex);
        for (size_t i = 0; i < row.size(); ++i) {
            file_stream << row[i];
            if (i != row.size() - 1) file_stream << ",";
        }
        file_stream << "\n";
        file_stream.flush();
    }
};

std::string generate_random_value() {
    std::string value(VALUE_SIZE, ' ');
    // Using thread_local for RNG to avoid contention and ensure different sequences per thread
    static thread_local std::mt19937 rng(std::hash<std::thread::id>{}(std::this_thread::get_id()) + std::chrono::high_resolution_clock::now().time_since_epoch().count());
    std::uniform_int_distribution<char> dist('a', 'z');
    for (char& c : value) c = dist(rng);
    return value;
}

std::vector<std::pair<uint64_t, std::string>> generate_initial_data(int count) {
    std::vector<std::pair<uint64_t, std::string>> data(count);
    std::mt19937 key_rng(1337); 
    std::vector<uint64_t> keys(count);
    std::iota(keys.begin(), keys.end(), 0); 
    std::shuffle(keys.begin(), keys.end(), key_rng);

    for (int i = 0; i < count; i++) {
        data[i] = {keys[i], generate_random_value()}; 
    }
    return data;
}

// Worker thread for GET/PUT
void worker_main(int thread_id, LSMTree* tree, 
                 std::vector<double>& local_read_latencies, 
                 std::vector<double>& local_write_latencies,
                 long long& num_local_reads, long long& num_local_writes) {
    // NUMA pinning from user's code
    {
        struct bitmask* cpus = numa_allocate_cpumask();
        numa_node_to_cpus((thread_id % NUM_EXEC_NODES) + 1, cpus); // Nodes 1, 2, 3...

        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        for (unsigned int i = 0; i < cpus->size; ++i) { // cpus->size can be large
            if (numa_bitmask_isbitset(cpus, i)) {
                CPU_SET(i, &cpuset);
            }
        }
        pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        numa_free_cpumask(cpus); // Free the mask

        unsigned int current_cpu_id = 0; // For getcpu
        unsigned int current_node_id = 0; // For getcpu
        int rc_getcpu = getcpu(&current_cpu_id, &current_node_id); // Check which CPU/Node we are on
        assert(rc_getcpu != -1 && "getcpu() failed");
        // std::cout << "Thread " << thread_id << " on CPU " << current_cpu_id << " Node " << current_node_id
        //           << " (Target Node " << (thread_id % NUM_EXEC_NODES) + 1 << ")" << std::endl;
        assert(current_node_id == static_cast<unsigned int>((thread_id % NUM_EXEC_NODES) + 1) && "not running on target node");
    }
    
    local_read_latencies.clear(); // Ensure vectors are empty
    local_write_latencies.clear();
    num_local_reads = 0;
    num_local_writes = 0;

    uint64_t t1, t2;
    std::string val_buffer; 
    std::string val_to_insert_template = generate_random_value(); 
    uint32_t tsc_aux;

    double zipf_write_ratio = 0.0; // Default for YCSB C (read-only)
    if (results_FILE == "a.csv") { // YCSB A (50/50 Read/Update)
        zipf_write_ratio = 0.5;
    } else if (results_FILE == "b.csv") { // YCSB B (95/5 Read/Update)
        zipf_write_ratio = 0.05;
    } else if (results_FILE == "c.csv") { // YCSB C (100% Read)
        zipf_write_ratio = 0.0;
    }
    // Add more YCSB profiles if needed e.g. D (read latest), F (read-modify-write)

    ScrambledZipfianGenerator zipf(TOTAL_KEYS, ZIPF_CONST, zipf_write_ratio);
    
    auto thread_start_time = std::chrono::high_resolution_clock::now();
    while (true) {
        auto current_loop_time = std::chrono::high_resolution_clock::now();
        if (std::chrono::duration<double>(current_loop_time - thread_start_time).count() >= total_runtime) {
            break;
        }

        KeyType key = zipf.Next();
        char op = zipf.get_op(); // 'R' for Read, 'U' for Update/Insert

        if (op == 'R') {
            t1 = __rdtscp(&tsc_aux);
            /*bool found =*/ tree->get(key, val_buffer); // found status can be logged if needed
            t2 = __rdtscp(&tsc_aux);
            local_read_latencies.push_back(cycles_to_nanoseconds(t2 - t1, CPU_FREQ_GHZ));
            num_local_reads++;
        } else { // 'U' or 'I'
            t1 = __rdtscp(&tsc_aux);
            tree->put(key, val_to_insert_template); // Use pre-generated value for puts
            t2 = __rdtscp(&tsc_aux);
            local_write_latencies.push_back(cycles_to_nanoseconds(t2 - t1, CPU_FREQ_GHZ));
            num_local_writes++;
        }
    }
    total_global_reads += num_local_reads;
    total_global_writes += num_local_writes;
}

// The benchmark
void benchmark_LSM(int num_threads_to_run, LSMTree* tree, CSVLogger& logger) {
    std::vector<std::thread> threads;
    threads.reserve(num_threads_to_run);

    // Per-thread statistics storage
    std::vector<std::vector<double>> per_thread_read_latencies(num_threads_to_run);
    std::vector<std::vector<double>> per_thread_write_latencies(num_threads_to_run);
    std::vector<long long> per_thread_reads_count(num_threads_to_run);
    std::vector<long long> per_thread_writes_count(num_threads_to_run);

    total_global_reads = 0; // Reset global counters
    total_global_writes = 0;

    auto overall_start_time = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_threads_to_run; ++i) {
        threads.emplace_back(worker_main, i, tree, 
                             std::ref(per_thread_read_latencies[i]), 
                             std::ref(per_thread_write_latencies[i]),
                             std::ref(per_thread_reads_count[i]),
                             std::ref(per_thread_writes_count[i]));
    }

    for (auto& t : threads) {
        t.join();
    }
    auto overall_end_time = std::chrono::high_resolution_clock::now();
    double actual_duration_sec = std::chrono::duration<double>(overall_end_time - overall_start_time).count();

    // Merge all latencies
    std::vector<double> all_reads_merged_latencies;
    std::vector<double> all_writes_merged_latencies;
    for (int i = 0; i < num_threads_to_run; ++i) {
        all_reads_merged_latencies.insert(all_reads_merged_latencies.end(), 
                                          per_thread_read_latencies[i].begin(), per_thread_read_latencies[i].end());
        all_writes_merged_latencies.insert(all_writes_merged_latencies.end(), 
                                           per_thread_write_latencies[i].begin(), per_thread_write_latencies[i].end());
    }
    
    long long total_ops_performed = total_global_reads.load() + total_global_writes.load();
    double throughput_ops_sec = (actual_duration_sec > 0) ? total_ops_performed / actual_duration_sec : 0.0;

    double sum_total_read_lat = std::accumulate(all_reads_merged_latencies.begin(), all_reads_merged_latencies.end(), 0.0);
    double avg_read_lat_ns = total_global_reads > 0 ? sum_total_read_lat / total_global_reads.load() : 0.0;

    double sum_total_write_lat = std::accumulate(all_writes_merged_latencies.begin(), all_writes_merged_latencies.end(), 0.0);
    double avg_write_lat_ns = total_global_writes > 0 ? sum_total_write_lat / total_global_writes.load() : 0.0;
    
    double avg_overall_lat_ns = total_ops_performed > 0 ? (sum_total_read_lat + sum_total_write_lat) / total_ops_performed : 0.0;

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "--- LSM In-Memory Benchmark Results ---" << std::endl;
    std::cout << "Threads: " << num_threads_to_run << " | Duration: " << actual_duration_sec << "s" << std::endl;
    std::cout << "Total Ops: " << total_ops_performed << " ("
              << "R: " << total_global_reads.load() << ", "
              << "W: " << total_global_writes.load() << ")" << std::endl;
    std::cout << "Throughput: " << throughput_ops_sec << " ops/s" << std::endl;
    std::cout << "Avg Latency (Overall): " << avg_overall_lat_ns << " ns/op" << std::endl;
    std::cout << "Avg Read Latency: " << avg_read_lat_ns << " ns/op" << std::endl;
    std::cout << "Avg Write Latency: " << avg_write_lat_ns << " ns/op" << std::endl;
    std::cout << "---------------------------------------" << std::endl;

    logger.writeRow({
        std::to_string(num_threads_to_run), 
        std::to_string(throughput_ops_sec), 
        std::to_string(avg_overall_lat_ns),
        std::to_string(avg_read_lat_ns), 
        std::to_string(avg_write_lat_ns)
    });
}


int main(int argc, char* argv[]) {
    int num_threads_for_benchmark = NUM_THREADS; // Default
    if (argc > 1) {
        try {
            num_threads_for_benchmark = std::stoi(argv[1]);
        } catch (const std::exception& e) {
            std::cerr << "Warning: Invalid thread count '" << argv[1] << "'. Using default: " << NUM_THREADS << std::endl;
        }
    }
    if (num_threads_for_benchmark <= 0) num_threads_for_benchmark = NUM_THREADS;


    if (argc > 2) {
        results_FILE = argv[2]; // e.g., "a.csv", "b.csv", "c.csv"
    }
    std::cout << "Results file: " << results_FILE << std::endl;
    // User's original path structure
    std::string log_path = "/mydata/LSM-vs-BTREE/lsm_results/" + results_FILE; 
    // For local testing, you might use: std::string log_path = "./" + results_FILE;


    CSVLogger logger(log_path, {"Thread Count", "Throughput (ops/s)", "Avg Latency (ns/op)", 
                                "Avg Read Latency (ns/op)", "Avg Write Latency (ns/op)"});
    if (!logger.file_stream.is_open()) { // Check if CSVLogger successfully opened the file
         std::cerr << "FATAL: Could not open results CSV file at " << log_path 
                   << ". Please check directory existence and permissions." << std::endl;
         // return 1; // Exit if logging is critical and failed.
    }

    // CSVLogger for page numbers (not used by this LSM-Tree)
    // CSVLogger pagenumbers("/mydata/pages.csv", {"page numbers"});

    numa_set_strict(1); // As per user's original code
    // Set affinity for the main thread (optional, but consistent with worker pinning)
    {
        struct bitmask* cpus = numa_allocate_cpumask();
        // Pin main thread to the first CPU of the first execution NUMA node (e.g. node 1)
        numa_node_to_cpus(1 % (NUM_EXEC_NODES +1), cpus); // Assuming nodes 1,2,3 from NUM_EXEC_NODES=3
        
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        bool affinity_set = false;
        for (unsigned int i = 0; i < cpus->size; ++i) {
            if (numa_bitmask_isbitset(cpus, i)) {
                CPU_SET(i, &cpuset);
                affinity_set = true; // Mark that we found at least one CPU
            }
        }
        if (affinity_set) { // Only set if cpuset is not empty
           pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        }
        numa_free_cpumask(cpus);
    }

    try {
        // Configure In-Memory LSM Tree: memtable_entries, L0_max_SSTs, num_levels, level_ratio, sst_target_entries
        LSMTree tree(256 * 1024, 8, 5, 10.0, 1024 * 16); 
        
        std::cout << "Generating and inserting " << TOTAL_KEYS << " initial key/value pairs..." << std::endl;
        auto initial_fill_data = generate_initial_data(TOTAL_KEYS);
        auto load_start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < initial_fill_data.size(); ++i) {
            tree.put(initial_fill_data[i].first, initial_fill_data[i].second);
            if ((i+1) % (TOTAL_KEYS/100) == 0 && TOTAL_KEYS >=100) {
                 std::cout << "\rLoading progress: " << (i+1)*100 / TOTAL_KEYS << "%" << std::flush;
            }
        }
        std::cout << "\rLoading progress: 100%." << std::endl;
        auto load_end = std::chrono::high_resolution_clock::now();
        std::cout << "Initial data loading complete in " 
                  << std::chrono::duration<double>(load_end - load_start).count() << " seconds." << std::endl;
        
        tree.print_tree_stats();

        std::cout << "\nStarting benchmark with " << num_threads_for_benchmark << " threads for " << total_runtime << " seconds..." << std::endl;
        benchmark_LSM(num_threads_for_benchmark, &tree, logger);

        std::cout << "\nBenchmark finished." << std::endl;
        tree.print_tree_stats(); // Final state

    } catch (const std::exception& ex) {
        std::cerr << "Exception: " << ex.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "Unknown exception occurred." << std::endl;
        return 1;
    }

    std::cout << "Done." << std::endl;
    return 0;
}