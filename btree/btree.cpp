#include <x86intrin.h>

#include <fstream>
#include <iostream>

#include "btree.h"
#include "../zipf_implementation.h"
// #include "common.h"
#include <cassert>

#define ZIPF_CONST 1.1

#ifndef CPU_FREQ_GHZ
#define CPU_FREQ_GHZ 2.1  // hardcode cpu frquency for precise timing
#endif
// ----------------------------------------------------------------
// Benchmark Code
// ----------------------------------------------------------------

// We do 1,000,000 inserts and then 10 get operations

constexpr int TOTAL_KEYS = 20000000;
constexpr int TOTAL_OPS = 5000000;
constexpr int NUM_EXEC_NODES = 3;
constexpr int NUM_THREADS = 4;
constexpr int VALUE_SIZE = 8;
constexpr int total_runtime = 10;  // seconds

const std::string YCSB_FILE = "/mydata/ycsb/c";
std::string results_FILE = "c.csv";

std::atomic<int> total_reads(0);
std::atomic<int> total_writes(0);
std::mutex latency_mutex;
std::vector<uint64_t> total_accessed_page_numbers;

class CSVLogger {
public:
    std::mutex mutex;
    CSVLogger(const std::string& filename, const std::vector<std::string>& header) {
        bool file_exists = std::ifstream(filename).good();
        file_.open(filename, std::ios::out | std::ios::app);
        if (!file_.is_open()) {
            std::cerr << "Failed to open CSV file: " << filename << std::endl;
        }
        if (!file_exists) {
            writeRow(header);
        }
    }

    ~CSVLogger() {
        if (file_.is_open()) file_.close();
    }

    void writeRow(const std::vector<std::string>& row) {
        std::lock_guard<std::mutex> lock(mutex);
        for (size_t i = 0; i < row.size(); ++i) {
            file_ << row[i];
            if (i != row.size() - 1) file_ << ",";
        }
        file_ << "\n";
        file_.flush();  // very important
        std::cout << "Writing row with " << row.size() << " columns\n";
    }

    std::ofstream file_;
};

// Generate random VALUE_SIZE-byte values
std::string generate_random_value() {
    std::string value(VALUE_SIZE, ' ');
    std::uniform_int_distribution<char> dist('a', 'z');
    std::mt19937 rng(1337);
    for (char& c : value) c = dist(rng);
    return value;
}

std::vector<std::pair<uint64_t, char>> read_ops_from_file() {
    std::ifstream file(YCSB_FILE);
    std::vector<std::pair<uint64_t, char>> ops;
    if (file.is_open()) {
        std::string line;
        while (std::getline(file, line)) {
            std::istringstream iss(line);
            uint64_t key;
            int node;
            char op;
            if (iss >> key >> node >> op) {
                ops.emplace_back(key, op);
            }
            // std::cout << "Key: " << key << ", Node: " << node << ", Op: " << op << "\n";
        }
        file.close();
    } else {
        std::cerr << "Unable to open file: " << YCSB_FILE << "\n";
    }
    return ops;
}

// Generate random (key, value) pairs
std::vector<std::pair<uint64_t, std::string>> generate_data() {
    std::vector<std::pair<uint64_t, std::string>> data(TOTAL_KEYS);
    std::string v = generate_random_value();
    for (int i = 0; i < TOTAL_KEYS; i++) {
        data[i] = {i, v};
    }
    return data;
}

// Generate random read ops
std::vector<std::pair<uint64_t, char>> generate_random_ops(const std::vector<std::pair<uint64_t, std::string>>& data) {
    std::vector<std::pair<uint64_t, char>> ops(TOTAL_OPS);
    std::mt19937 rng(1337);
    std::uniform_int_distribution<int> dist(0, TOTAL_KEYS - 1);
    for (int i = 0; i < TOTAL_OPS; ++i) {
        ops[i].first = data[dist(rng)].first;  // pick random key from 'data'
        ops[i].second = 'R';                   // 'G' for GET operation
    }
    return ops;
}

std::vector<std::pair<uint64_t, char>> generate_random_ops_just_one(
    const std::vector<std::pair<uint64_t, std::string>>& data) {
    std::vector<std::pair<uint64_t, char>> ops(TOTAL_OPS);
    std::mt19937 rng(1337);
    std::uniform_int_distribution<int> dist(0, TOTAL_KEYS - 1);
    for (int i = 0; i < 2; ++i) {
        ops[i].first = data[dist(rng)].first;  // pick random key from 'data'
        ops[i].second = 'R';                   // 'G' for GET operation
    }
    return ops;
}

// Worker thread for GET
void worker_get(const std::vector<std::pair<uint64_t, char>>& ops, int start, int end, int thread_id, BPlusTree* tree, std::vector<double>& local_read_latencies, std::vector<double>& local_write_latencies) {
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
    // std::cout << "Thread " << thread_id << " started.\n";
    local_read_latencies.clear();
    local_write_latencies.clear();
    uint64_t t1, t2;
    bool found = false;
    std::string val_to_insert = generate_random_value();
    auto start_thread_time = std::chrono::high_resolution_clock::now();
    auto end_thread_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed_seconds = end_thread_time - start_thread_time;
    uint32_t tsc_aux;
    double write_ratio = 0.0;
    if (results_FILE == "a.csv") {
        write_ratio = 0.5;
    } else if (results_FILE == "b.csv") {
        write_ratio = 0.05;
    }
    ScrambledZipfianGenerator zipf(TOTAL_KEYS, ZIPF_CONST, write_ratio);
    off_t key = 1;
    char op = 'R';

    while (elapsed_seconds.count() < total_runtime) {
        for (int i = start; i < end; ++i) {
            if (elapsed_seconds.count() > total_runtime) {
                break;
            }
            std::string val;
            key = zipf.Next();
            op = zipf.get_op();
            if (op == 'R') {
                t1 = __rdtscp(&tsc_aux);
                found = tree->get(key, val);
                t2 = __rdtscp(&tsc_aux);
                if (!found) {
                    std::cerr << "Key not found: " << key << "\n";
                }
                auto duration = cycles_to_nanoseconds(t2 - t1, CPU_FREQ_GHZ);
                local_read_latencies.push_back(duration);
                found = false;
            } else if (op == 'U' || op == 'I') {
                t1 = __rdtscp(&tsc_aux);
                tree->put(key, val_to_insert);
                t2 = __rdtscp(&tsc_aux);
                auto duration = cycles_to_nanoseconds(t2 - t1, CPU_FREQ_GHZ);
                local_write_latencies.push_back(duration);
                found = false;
            }
        }
        end_thread_time = std::chrono::high_resolution_clock::now();
        elapsed_seconds = end_thread_time - start_thread_time;
    }

    std::lock_guard<std::mutex> lock(latency_mutex);
    // Now handled in benchmark
}

// The benchmark
void benchmark(int num_threads, const std::vector<std::pair<uint64_t, std::string>>& data,
               const std::vector<std::pair<uint64_t, char>>& ops, CSVLogger& logger, CSVLogger& pagenumbers, BPlusTree* tree) {
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
            worker_get(ops, beg, end, i, tree, local_read_latencies, local_write_latencies);
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
    std::cout << "Lat size" << all_read_latencies.size() << "\n";
    std::cout << "Write Lat size" << all_write_latencies.size() << "\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << "GET | Threads: " << num_threads << " | Avg Latency: " << avgReadLat << " ns/op\n";
    std::cout << "PUT | Threads: " << num_threads << " | Avg Latency: " << avgWriteLat << " ns/op\n";
    std::cout << "Total | Threads: " << num_threads << " | Throughput: " << throughput << " ops/s"
              << " | Avg Latency: " << avgLat << " ns/op\n";

    logger.writeRow({std::to_string(num_threads), std::to_string(throughput), std::to_string(avgLat),
                     std::to_string(avgReadLat), std::to_string(avgWriteLat)});
    if (total_accessed_page_numbers.size() > 0) {
        std::cout << "Total accessed page numbers: " << total_accessed_page_numbers.size() << "\n";
        for (auto page_number : total_accessed_page_numbers) {
            pagenumbers.writeRow({std::to_string(page_number)});
        }
    }
}

// main
int main(int argc, char* argv[]) {
    if (argv[2] != nullptr) {
        results_FILE = argv[2];
    }
    std::cout << "Results file: " << results_FILE << "\n";
    std::string log_path = "/mydata/LSM-vs-BTREE/btree_results/" + results_FILE;
    CSVLogger logger(log_path, {"Thread Count", "Throughput (ops/s)", "Avg Latency (ns/op)", "Avg Read Latency (ns/op)",
                                "Avg Write Latency (ns/op)"});
    CSVLogger pagenumbers("/mydata/pages.csv", {"page numbers"});
    if (!pagenumbers.file_.is_open()) {
        std::cerr << "Failed to open CSV file: /mydata/pages.csv. Please check that the directory exists and is writable.\n";
    }
    // CSVLogger logger("/mydata/results_local.csv", {"Thread Count", "Throughput (ops/s)", "Avg Latency (ns/op)"});

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
        BPlusTree tree;
        auto data = generate_data();
        std::cout << "Creating B+ Tree and inserting data...\n";
        for (auto& kv : data) {
            tree.put(kv.first, kv.second);
        }
        std::cout << "Inserted " << data.size() << " random key/value pairs.\n";
        // auto tmp = tree.rangeQuery(1, 50);
        // std::cout << "Range query result size: " << tmp.size() << "\n";
        // for (auto& kv : tmp) {
        //     std::cout << "Key: " << kv.first << ", Value: " << kv.second << "\n";
        // }

        // auto ops = generate_random_ops(data);
        auto ops = generate_random_ops_just_one(data);
        // auto ops = read_ops_from_file();

        if (argc > 1) {
            int Number_of_threads = std::stoi(argv[1]);
            benchmark(Number_of_threads, data, ops, logger, pagenumbers, &tree);
        } else {
            std::cout << "No batch number provided. Running with 1 thread.\n";
            benchmark(1, data, ops, logger, pagenumbers, &tree);
        }

        std::cout << "Done.\n";
        {
            std::cout << "B+ Tree Structure:\n";
            // tree.print_btree(tree.rootOffset_, 0);
            tree.print_tree_stats();
        }
    } catch (const std::exception& ex) {
        std::cerr << "Exception: " << ex.what() << "\n";
    }
    return 0;
}
