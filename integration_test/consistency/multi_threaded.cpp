#include "kv739_client.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <chrono>
#include <string.h>

using namespace std;

class MultiThreadTester {
private:
    atomic<bool> test_failed{false};

    void assert_condition(bool condition, const string& message) {
        if (!condition) {
            cout << "Test failed: " << message << endl;
            test_failed = true;
        }
    }

    bool verify_value(char* key, const char* expected) {
        char value[2049];
        int result = kv739_get(key, value);
        if (result == 0) {
            return strcmp(value, expected) == 0;
        }
        return false;
    }

public:
    MultiThreadTester() {
        kv739_launch_server(const_cast<char*>(""));
        if (kv739_init(const_cast<char*>("config.txt")) != 0) {
            throw runtime_error("Failed to initialize KV store");
        }
        cout << "KV store initialized successfully" << endl;
    }

    ~MultiThreadTester() {
        kv739_shutdown();
    }

    void test_concurrent_writes() {
        cout << "\nTesting concurrent writes..." << endl;
        
        const int num_threads = 10;
        vector<thread> threads;
        atomic<int> successful_writes{0};

        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([this, i, &successful_writes]() {
                char old_value[2049];
                string key = "concurrent_key_" + to_string(i);
                string value = "value_" + to_string(i);
                
                int result = kv739_put(
                    const_cast<char*>(key.c_str()),
                    const_cast<char*>(value.c_str()),
                    old_value
                );
                
                if (result >= 0) {
                    successful_writes++;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        cout << "Completed " << successful_writes << " successful concurrent writes" << endl;
        assert_condition(successful_writes > 0, "No successful writes completed");
    }

    void test_concurrent_reads() {
        cout << "\nTesting concurrent reads..." << endl;
        
        // Write initial value
        char old_value[2049];
        kv739_put(
            const_cast<char*>("read_test_key"),
            const_cast<char*>("read_test_value"),
            old_value
        );

        const int num_threads = 10;
        vector<thread> threads;
        atomic<int> successful_reads{0};

        for (int i = 0; i < num_threads; i++) {
            threads.emplace_back([this, &successful_reads]() {
                char value[2049];
                int result = kv739_get(
                    const_cast<char*>("read_test_key"),
                    value
                );
                if (result == 0) {
                    successful_reads++;
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        cout << "Completed " << successful_reads << " successful concurrent reads" << endl;
        assert_condition(successful_reads > 0, "No successful reads completed");
    }

    void test_mixed_operations() {
        cout << "\nTesting mixed read/write operations..." << endl;
        
        const int num_threads = 10;
        vector<thread> threads;
        atomic<int> successful_ops{0};
        atomic<bool> stop_flag{false};

        // Start reader threads
        for (int i = 0; i < num_threads/2; i++) {
            threads.emplace_back([this, &stop_flag, &successful_ops]() {
                while (!stop_flag) {
                    char value[2049];
                    int result = kv739_get(
                        const_cast<char*>("mixed_test_key"),
                        value
                    );
                    if (result == 0) {
                        successful_ops++;
                    }
                    this_thread::sleep_for(chrono::milliseconds(10));
                }
            });
        }

        // Start writer threads
        for (int i = 0; i < num_threads/2; i++) {
            threads.emplace_back([this, i, &stop_flag, &successful_ops]() {
                while (!stop_flag) {
                    char old_value[2049];
                    string value = "value_" + to_string(i);
                    int result = kv739_put(
                        const_cast<char*>("mixed_test_key"),
                        const_cast<char*>(value.c_str()),
                        old_value
                    );
                    if (result >= 0) {
                        successful_ops++;
                    }
                    this_thread::sleep_for(chrono::milliseconds(20));
                }
            });
        }

        // Let the test run for a few seconds
        this_thread::sleep_for(chrono::seconds(3));
        stop_flag = true;

        for (auto& t : threads) {
            t.join();
        }

        cout << "Completed " << successful_ops << " successful mixed operations" << endl;
        assert_condition(successful_ops > 0, "No successful operations completed");
    }

    void run_all_tests() {
        test_concurrent_writes();
        test_concurrent_reads();
        test_mixed_operations();
        cout << "\nAll multi-threaded tests completed!" << endl;
        
        if (test_failed) {
            cout << "Some tests failed!" << endl;
        } else {
            cout << "All tests passed successfully!" << endl;
        }
    }
};

int main() {
    try {
        MultiThreadTester tester;
        tester.run_all_tests();
        return 0;
    } catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << endl;
        return 1;
    }
}