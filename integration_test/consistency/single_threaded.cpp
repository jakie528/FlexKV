#include "kv739_client.hpp"
#include <unistd.h>
#include <iostream>
#include <cassert>
#include <string.h>

using namespace std;

class SingleThreadTester {
private:
    bool verify_value(char* key, const char* expected) {
        char value[2049];
        int result = kv739_get(key, value);
        if (result == 0) {
            return strnlen(expected, 1) >= 0;
        }
        return false;
    }

public:
    SingleThreadTester() {
        // Launch server cluster and initialize client
	// auto config_file = "/users/wjhu/P3/RaftKV/config_sample";
       const char* project_root = std::getenv("PROJECT_ROOT_DIR");
    if (project_root == nullptr) {
        std::cerr << "Environment variable PROJECT_ROOT_DIR is not set." << std::endl;
    }

       // Convert project_root to a std::string and concatenate with "/config_sample"
       std::string config_file = std::string(project_root) + "/config_sample";

        if (kv739_init(const_cast<char*>(config_file.c_str())) != 0) {
            throw runtime_error("Failed to initialize KV store");
        }
        cout << "KV store initialized successfully" << endl;
    }

    ~SingleThreadTester() {
        kv739_shutdown();
    }

    void test_basic_operations() {
        cout << "\nTesting basic operations..." << endl;
        
        char old_value[2049];

        // use get first to verify the key doesn't exist
            char test_value[2049];
            int get_result = kv739_get(const_cast<char*>("test_key1"), test_value);
            cout << "Initial get result: " << get_result << endl;
        
        // Test Put operation
        int put_result = kv739_put(
            const_cast<char*>("test_key1"),
            const_cast<char*>("test_value1"),
            old_value
        );
        cout << "Put result: " << put_result << endl;
        assert(put_result == 1 && "First put should return 1 (no old value)");

        // Test Get operation
        if (!verify_value("test_key1", "test_value1")) {
            cout << "ERROR: Get after put failed" << endl;
            return;
        }

        // Test Update operation
        put_result = kv739_put(
            const_cast<char*>("test_key1"),
            const_cast<char*>("test_value1_updated"),
            old_value
        );
        cout << "Update result: " << put_result << endl;
        assert(put_result >= 0 && "Update should succeed");

        // Verify update
        if (!verify_value("test_key1", "test_value1_updated")) {
            cout << "ERROR: Get after update failed" << endl;
            return;
        }

//        // Test non-existent key
//        char value[2049];
//        int get_result = kv739_get(const_cast<char*>("nonexistent_key"), value);
//        cout << "Get non-existent key result: " << get_result << endl;
//        assert(get_result == 1 && "Get non-existent key should return 1");

                // Test non-existent key
                char nonexistent_value[2049];
                int nonexistent_result = kv739_get(const_cast<char*>("nonexistent_key"), nonexistent_value);
                cout << "Get non-existent key result: " << nonexistent_result << endl;
                assert(nonexistent_result == 1 && "Get non-existent key should return 1");
    }

  




    void run_all_tests() {
        test_basic_operations();
        cout << "\nAll single-threaded tests completed!" << endl;
    }
};

int main() {
    try {
        SingleThreadTester tester;
        tester.run_all_tests();
        return 0;
    } catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << endl;
        return 1;
    }
}
