#include "kv739_client.hpp"
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <random>
#include <cassert>
#include <atomic>

class KV739Tester {
private:
    KV739ClientCpp client;
    std::atomic<bool> test_failed{false};
    
    // Helper function to generate random strings
    std::string random_string(size_t length) {
        static const char alphanum[] =
            "0123456789"
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
            "abcdefghijklmnopqrstuvwxyz";
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> dis(0, sizeof(alphanum) - 2);
        
        std::string str;
        str.reserve(length);
        for (size_t i = 0; i < length; ++i) {
            str += alphanum[dis(gen)];
        }
        return str;
    }

    void assert_with_message(bool condition, const std::string& message) {
        if (!condition) {
            std::cerr << "Assertion failed: " << message << std::endl;
            test_failed = true;
        }
    }

public:
    KV739Tester(uint64_t leader_id, const std::string& leader_addr) 
        : client(leader_id, leader_addr) {
        client.init();
    }

    // Basic functionality tests
    void test_basic_operations() {
        std::cout << "Running basic operations test..." << std::endl;
        
        try {
            // Simple put and get
            client.put("test_key", "test_value");
            std::string result = client.get("test_key");
            assert_with_message(result == "test_value", "Basic put/get failed");

            // Update existing key
            client.put("test_key", "updated_value");
            result = client.get("test_key");
            assert_with_message(result == "updated_value", "Update value failed");

            // Non-existent key
            try {
                client.get("nonexistent_key");
                assert_with_message(false, "Should throw exception for non-existent key");
            } catch (const KV739Exception& e) {
                // Expected behavior
            }
        } catch (const KV739Exception& e) {
            std::cerr << "Basic operations test failed: " << e.what() << std::endl;
            test_failed = true;
        }
    }

    // Concurrent operations test
    void test_concurrent_operations() {
        std::cout << "Running concurrent operations test..." << std::endl;
        
        const int num_threads = 10;
        const int ops_per_thread = 100;
        std::vector<std::thread> threads;

        for (int i = 0; i < num_threads; ++i) {
            threads.emplace_back([this, i, ops_per_thread]() {
                try {
                    for (int j = 0; j < ops_per_thread; ++j) {
                        std::string key = "key_" + std::to_string(i) + "_" + std::to_string(j);
                        std::string value = random_string(10);
                        
                        client.put(key, value);
                        std::string retrieved = client.get(key);
                        assert_with_message(retrieved == value, 
                            "Concurrent operation failed: value mismatch");
                    }
                } catch (const KV739Exception& e) {
                    std::cerr << "Thread " << i << " failed: " << e.what() << std::endl;
                    test_failed = true;
                }
            });
        }

        for (auto& thread : threads) {
            thread.join();
        }
    }

    // Node failure and recovery test
    void test_node_failure_recovery() {
        std::cout << "Running node failure and recovery test..." << std::endl;
        
        try {
            // Start additional nodes
            uint64_t node_id_1 = 2;
            uint64_t node_id_2 = 3;
            client.start(node_id_1, "localhost:8082");
            client.start(node_id_2, "localhost:8083");

            // Write some data
            client.put("failure_test_key", "initial_value");

            // Simulate node failure
            client.leave(node_id_1);

            // Try operations with one node down
            std::string result = client.get("failure_test_key");
            assert_with_message(result == "initial_value", 
                "Data should be available after single node failure");

            // Update data with one node down
            client.put("failure_test_key", "updated_value");

            // Recover failed node
            client.start(node_id_1, "localhost:8082");
            
            // Verify data consistency after recovery
            std::this_thread::sleep_for(std::chrono::seconds(2)); // Allow time for sync
            result = client.get("failure_test_key");
            assert_with_message(result == "updated_value", 
                "Data should be consistent after node recovery");

        } catch (const KV739Exception& e) {
            std::cerr << "Node failure/recovery test failed: " << e.what() << std::endl;
            test_failed = true;
        }
    }

    // Performance test
    void test_performance() {
        std::cout << "Running performance test..." << std::endl;
        
        const int num_operations = 1000;
        std::vector<std::chrono::microseconds> latencies;
        latencies.reserve(num_operations);

        try {
            auto start_time = std::chrono::high_resolution_clock::now();
            
            for (int i = 0; i < num_operations; ++i) {
                std::string key = "perf_key_" + std::to_string(i);
                std::string value = random_string(100);

                auto op_start = std::chrono::high_resolution_clock::now();
                client.put(key, value);
                client.get(key);
                auto op_end = std::chrono::high_resolution_clock::now();

                latencies.push_back(std::chrono::duration_cast<std::chrono::microseconds>(
                    op_end - op_start));
            }

            auto end_time = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::seconds>(
                end_time - start_time);

            // Calculate statistics
            double total_latency = 0;
            for (const auto& latency : latencies) {
                total_latency += latency.count();
            }
            double avg_latency = total_latency / latencies.size();
            double throughput = num_operations / duration.count();

            std::cout << "Performance results:" << std::endl
                      << "Average latency: " << avg_latency << " microseconds" << std::endl
                      << "Throughput: " << throughput << " operations/second" << std::endl;

        } catch (const KV739Exception& e) {
            std::cerr << "Performance test failed: " << e.what() << std::endl;
            test_failed = true;
        }
    }

    bool run_all_tests() {
        test_basic_operations();
        test_concurrent_operations();
        test_node_failure_recovery();
        test_performance();
        return !test_failed;
    }
};

int main() {
    try {
        KV739Tester tester(1, "localhost:8081");
        bool success = tester.run_all_tests();
        
        if (success) {
            std::cout << "All tests passed successfully!" << std::endl;
            return 0;
        } else {
            std::cerr << "Some tests failed!" << std::endl;
            return 1;
        }
    } catch (const KV739Exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
}
