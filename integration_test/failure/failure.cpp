#include "kv739_client.hpp"
#include <iostream>
#include <cassert>
#include <string.h>

using namespace std;

class FailureTester {
private:
    bool verify_value(char* key, const char* expected) {
        char value[2049];
        int result = kv739_get(key, value);
        if (result == 0) {
            cout << "Got value: " << value << " for key: " << key << endl;
            return strcmp(value, expected) == 0;
        }
        return false;
    }

public:
    FailureTester() {
        // Launch server cluster and initialize client
    // string file_name = "/config_sample";
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

    ~FailureTester() {
        kv739_shutdown();
    }

  void test_server_failure() {
        cout << "\nTesting server failure handling..." << endl;

        // Store a value before failure
        char old_value[2049];
        kv739_put(
            const_cast<char*>("failure_test_key"),
            const_cast<char*>("before_failure"),
            old_value
        );

        // Kill one server (clean shutdown)
        cout << "Killing server instance..." << endl;
        int kill_result = kv739_die(const_cast<char*>("127.0.0.1:21001"), 1);
        cout << "Kill result: " << kill_result << endl;
	    sleep(5);
        // Try to access data
        if (!verify_value("failure_test_key", "before_failure")) {
            cout << "ERROR: Data inaccessible after server failure" << endl;
        }

    }


    void run_all_tests() {
        test_server_failure();
        cout << "\nAll single-threaded tests completed!" << endl;
    }
};

int main() {
    try {
        FailureTester tester;
        tester.run_all_tests();
        return 0;
    } catch (const exception& e) {
        cerr << "Fatal error: " << e.what() << endl;
        return 1;
    }
}
