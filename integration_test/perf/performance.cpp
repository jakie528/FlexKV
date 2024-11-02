#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cassert>
#include <chrono>
#include <thread>
#include <atomic>
#include <algorithm>
#include "kv739_client.hpp"

// Function to convert string to lowercase for case-insensitive comparison
std::string to_lowercase(const std::string& str) {
    std::string lower_str = str;
    std::transform(lower_str.begin(), lower_str.end(), lower_str.begin(), ::tolower);
    return lower_str;
}

// Shared atomic variables for statistics
std::atomic<long long> total_latency{0};
std::atomic<long long> total_read_latency{0};
std::atomic<long long> total_write_latency{0};
std::atomic<int> total_operations{0};
std::atomic<int> total_read_operations{0};
std::atomic<int> total_write_operations{0};
static auto universe_start = std::chrono::high_resolution_clock::now(); 

void perform_operation(const std::vector<std::string>& operations, const char* config_file, int thread_id) {
    char value[2049];
    
    if (kv739_init(const_cast<char*>(config_file)) != 0) {
        std::cerr << "Initialization failed in thread" << std::endl;
        return;
    }
        auto overall_start = std::chrono::high_resolution_clock::now();
        if (overall_start > universe_start) {
            universe_start = overall_start;
        } 


    for (const auto& line : operations) {
        std::istringstream iss(line);
        std::string operation, key, val;
        iss >> operation;

        // Convert operation to lowercase for case-insensitive comparison
        operation = to_lowercase(operation);

        if (operation == "done") {
            std::cout << "Finish loading data, start benchmark" << std::endl;
            continue;
        }

        // Handle SHUTDOWN operation
        if (operation == "shutdown") {
                kv739_shutdown();
                std::cout << "Shutdown completed" << std::endl;
            break;
        }

        auto start = std::chrono::high_resolution_clock::now();
        if (operation == "put") {
            iss >> key >> val;
            int result = kv739_put(const_cast<char*>(key.c_str()), const_cast<char*>(val.c_str()), value);
            assert(result == 0 || result == 1);
        } else if (operation == "get") {
            iss >> key;
            int result = kv739_get(const_cast<char*>(key.c_str()), value);
            assert(result == 0 || result == 1);
        } else {
            std::cerr << "Unknown operation: " << operation << std::endl;
            continue;
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();

        // Update atomic statistics
        total_latency.fetch_add(latency);
        auto total_ops = total_operations.fetch_add(1);
       if (total_ops % 20 == 0) {
          std::cout << "completed " << total_ops << " ops" << std::endl;
       }
        if (operation == "put") {
            total_write_operations.fetch_add(1);
            total_write_latency.fetch_add(latency);
        } else if (operation == "get") {
            total_read_operations.fetch_add(1);
            total_read_latency.fetch_add(latency);
        }
    }

}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <config_file> <workload_file> <thread_num>" << std::endl;
        return 1;
    }

    const char* config_file = argv[1];
    const char* workload_file = argv[2];
    int thread_number = std::atoi(argv[3]);


    std::cout << "Running Multithreaded Performance Tests with Workload File..." << std::endl;

    std::ifstream infile(workload_file);
    if (!infile.is_open()) {
        std::cerr << "Error: Could not open workload file: " << workload_file << std::endl;
        return 1;
    }

    std::string line;
    std::vector<std::string> operations;
    std::atomic<bool> initialized(false);

    while (std::getline(infile, line)) {
        operations.push_back(line);
    }
    infile.close();

    int num_threads =thread_number;
    std::vector<std::thread> threads;
    int chunk_size = operations.size() / num_threads;
    

    universe_start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < num_threads; ++i) {
        int start_idx = i * chunk_size;
        int end_idx = (i == num_threads - 1) ? operations.size() : start_idx + chunk_size;
        std::vector<std::string> operation_chunk(operations.begin() + start_idx, operations.begin() + end_idx);
        threads.emplace_back(perform_operation, operation_chunk, config_file, i);
    }

    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }

    auto overall_end = std::chrono::high_resolution_clock::now();
    auto overall_duration = std::chrono::duration_cast<std::chrono::microseconds>(overall_end - universe_start).count();

    // Print results
    std::cout << "Workload Results:" << std::endl;
    // std::cout << "Total Throughput: " << total_operations.load() / ((total_latency.load() / 1000000)) << std::endl;
    std::cout << "Total Throughput: " << (total_operations.load() * 1e6) / overall_duration << " ops/sec" << std::endl;

    std::cout << "Total Operations: " << total_operations.load() << std::endl;
    std::cout << "Average Latency: " << (total_operations.load() > 0 ? total_latency.load() / total_operations.load() : 0) << " microseconds" << std::endl;
    std::cout << "Total Read Operations: " << total_read_operations.load() << std::endl;
    std::cout << "Average Read Latency: " << (total_read_operations.load() > 0 ? total_read_latency.load() / total_read_operations.load() : 0) << " microseconds" << std::endl;
    std::cout << "Total Write Operations: " << total_write_operations.load() << std::endl;
    std::cout << "Average Write Latency: " << (total_write_operations.load() > 0 ? total_write_latency.load() / total_write_operations.load() : 0) << " microseconds" << std::endl;

    return 0;
}
