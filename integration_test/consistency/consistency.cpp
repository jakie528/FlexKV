#include "kv739_client.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <atomic>
#include <cassert>
#include <fstream>
#include <string>
#include <string.h>

using namespace std;

class ConsistencyTester {
private:
    atomic<bool> test_failed{false};
    vector<string> server_addresses;
    const string CONFIG_FILE = "servers.config";

    void load_server_addresses() {
        ifstream config_file(CONFIG_FILE);
        if (!config_file.is_open()) {
            throw runtime_error("Failed to open config file: " + CONFIG_FILE);
        }

        string line;
        while (getline(config_file, line)) {
            if (!line.empty()) {
                server_addresses.push_back(line);
            }
        }

        if (server_addresses.empty()) {
            throw runtime_error("No server addresses found in config file");
        }

        cout << "Loaded " << server_addresses.size() << " server addresses" << endl;
    }

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

    string get_random_server() {
        return server_addresses[rand() % server_addresses.size()];
    }

public:
    ConsistencyTester() {
        srand(time(nullptr));
        load_server_addresses();
        
        if (kv739_init(const_cast<char*>(CONFIG_FILE.c_str())) != 0) {
            throw runtime_error("Failed to initialize KV store");
        }
        cout << "KV store initialized successfully" << endl;
    }

    void test_read_after_write() {
        cout << "\nRunning Read-After-Write test..." << endl;
        
        // Single client RAW test
        char old_value[2049];
        int put_result = kv739_put(const_cast<char*>("raw_key"), 
                                 const_cast<char*>("raw_value"), 
                                 old_value);
        assert_condition(put_result == 1, "Initial write failed");
        assert_condition(verify_value("raw_key", "raw_value"), 
                        "Immediate read after write failed");

        // Multi-client RAW test across different servers
        const int num_pairs = 5;
        vector<thread> threads;

        for (int i = 0; i < num_pairs; i++) {
            // Writer thread
            threads.emplace_back([this, i]() {
                char old_val[2049];
                string key = "raw_key_" + to_string(i);
                string value = "raw_value_" + to_string(i);
                int res = kv739_put(const_cast<char*>(key.c_str()), 
                                  const_cast<char*>(value.c_str()), 
                                  old_val);
                assert_condition(res == 1, "Writer thread " + to_string(i) + " put failed");
            });

            // Reader thread
            threads.emplace_back([this, i]() {
                this_thread::sleep_for(chrono::milliseconds(100));
                string key = "raw_key_" + to_string(i);
                string expected = "raw_value_" + to_string(i);
                assert_condition(verify_value(const_cast<char*>(key.c_str()), expected.c_str()), 
                               "Reader thread " + to_string(i) + " verification failed");
            });
        }

        for (auto& t : threads) {
            t.join();
        }
    }

    void test_write_after_write() {
        cout << "\nRunning Write-After-Write test..." << endl;

        const int num_writes = 10;
        atomic<int> write_count{0};
        vector<thread> writers;

        for (int i = 0; i < num_writes; i++) {
            writers.emplace_back([this, i, &write_count]() {
                char old_val[2049];
                string value = "waw_value_" + to_string(i);
                int res = kv739_put(const_cast<char*>("waw_key"), 
                                  const_cast<char*>(value.c_str()), 
                                  old_val);
                if (res >= 0) {
                    write_count++;
                }
            });
        }

        for (auto& w : writers) {
            w.join();
        }

        assert_condition(write_count > 0, "No successful writes completed");
        cout << "Completed " << write_count << " successful writes" << endl;
    }

    void test_concurrent_read_write() {
        cout << "\nRunning Concurrent Read-Write test..." << endl;

        atomic<bool> stop_flag{false};
        atomic<int> successful_reads{0};
        atomic<int> successful_writes{0};
        vector<thread> threads;

        // Start readers on different servers
        for (size_t i = 0; i < server_addresses.size(); i++) {
            threads.emplace_back([this, &stop_flag, &successful_reads]() {
                char value[2049];
                while (!stop_flag) {
                    int result = kv739_get(const_cast<char*>("concurrent_key"), value);
                    if (result == 0) {
                        successful_reads++;
                        cout << "Server " << this_thread::get_id() 
                             << " read value: " << value << endl;
                    }
                    this_thread::sleep_for(chrono::milliseconds(50));
                }
            });
        }

        // Start writers on different servers
        for (int i = 0; i < 5; i++) {
            threads.emplace_back([this, i, &stop_flag, &successful_writes]() {
                char old_value[2049];
                while (!stop_flag) {
                    string value = "writer_" + to_string(i) + "_value_" + 
                                 to_string(successful_writes.load());
                    int res = kv739_put(const_cast<char*>("concurrent_key"), 
                                      const_cast<char*>(value.c_str()), 
                                      old_value);
                    if (res >= 0) {
                        successful_writes++;
                    }
                    this_thread::sleep_for(chrono::milliseconds(100));
                }
            });
        }

        this_thread::sleep_for(chrono::seconds(5));
        stop_flag = true;

        for (auto& t : threads) {
            t.join();
        }

        cout << "Completed " << successful_reads << " successful reads and " 
             << successful_writes << " successful writes" << endl;
    }

    void test_consistency_under_failover() {
        cout << "\nRunning Consistency Under Failover test..." << endl;

        // Write initial value
        char old_value[2049];
        kv739_put(const_cast<char*>("failover_key"), 
                 const_cast<char*>("initial_value"), 
                 old_value);

        // Select random servers for failure testing
        vector<string> test_servers;
        for (int i = 0; i < 3; i++) {
            test_servers.push_back(get_random_server());
        }

        // Kill multiple servers
        for (const auto& server : test_servers) {
            cout << "Killing server: " << server << endl;
            int kill_result = kv739_die(const_cast<char*>(server.c_str()), 1);
            assert_condition(kill_result == 0, 
                           "Failed to kill server: " + server);
        }

        // Verify data is still accessible
        assert_condition(verify_value("failover_key", "initial_value"), 
                        "Data inaccessible after node failures");

        // Write new value after failures
        int put_result = kv739_put(const_cast<char*>("failover_key"), 
                                 const_cast<char*>("after_failure_value"), 
                                 old_value);
        assert_condition(put_result >= 0, "Write after failure failed");

        // Restart failed servers
        for (const auto& server : test_servers) {
            cout << "Restarting server: " << server << endl;
            int start_result = kv739_start(const_cast<char*>(server.c_str()), 0);
            assert_condition(start_result == 0, 
                           "Failed to restart server: " + server);
        }

        // Allow time for recovery
        this_thread::sleep_for(chrono::seconds(3));

        // Verify consistency after recovery
        assert_condition(verify_value("failover_key", "after_failure_value"), 
                        "Data inconsistent after recovery");
    }

    bool run_all_tests() {
        test_read_after_write();
        test_write_after_write();
        test_concurrent_read_write();
        test_consistency_under_failover();
        return !test_failed;
    }
};

int main() {
    try {
        ConsistencyTester tester;
        bool success = tester.run_all_tests();
        
        if (success) {
            cout << "All consistency tests passed!" << endl;
            return 0;
        } else {
            cout << "Some tests failed!" << endl;
            return 1;
        }
    } catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << endl;
        return 1;
    }
}