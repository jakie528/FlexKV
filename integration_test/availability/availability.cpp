#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <thread>
#include <fstream>
#include <cassert>
#include <cstring>

extern "C" {
    int kv739_init(char* config_file);
    int kv739_shutdown(void);
    int kv739_get(char* key, char* value);
    int kv739_put(char* key, char* value, char* old_value);
    int kv739_die(char* instance_name, int clean);
    int kv739_start(char* instance_name, int clean);
    int kv739_leave(char* instance_name, int clean);
}

class AvailabilityTester {
private:
    std::vector<std::string> instances;
    const size_t MAX_VALUE_SIZE = 2049;
    
    bool loadInstances(const std::string& config_file) {
        std::ifstream file(config_file);
        if (!file.is_open()) return false;
        
        std::string instance;
        while (std::getline(file, instance)) {
            if (!instance.empty()) instances.push_back(instance);
        }
        std::cout << "Loaded " << instances.size() << " instances" << std::endl;
        return true;
    }

    bool isServiceResponsive() {
        char value[MAX_VALUE_SIZE];
        char old_value[MAX_VALUE_SIZE];
        const char* test_key = "availability_test_key";
        const char* test_value = "test_value";
        
        int put_result = kv739_put(const_cast<char*>(test_key), 
                                 const_cast<char*>(test_value), 
                                 old_value);
        return put_result >= 0;
    }

    std::vector<std::string> killInstances(const std::vector<size_t>& indices, bool clean) {
        std::vector<std::string> killed;
        for (size_t idx : indices) {
            if (idx < instances.size()) {
                if (kv739_die(const_cast<char*>(instances[idx].c_str()), clean) == 0) {
                    killed.push_back(instances[idx]);
                    std::cout << "Killed instance: " << instances[idx] << std::endl;
                }
            }
        }
        return killed;
    }

    void restartInstances(const std::vector<std::string>& instances, bool clean) {
        for (const auto& instance : instances) {
            std::cout << "Restarting instance: " << instance << std::endl;
            kv739_start(const_cast<char*>(instance.c_str()), clean);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

public:
    bool initialize(const std::string& config_file) {
        if (!loadInstances(config_file)) return false;
        return kv739_init(const_cast<char*>(config_file.c_str())) == 0;
    }

    void cleanup() {
        kv739_shutdown();
    }

    // Test 1: Determine minimum instances needed for availability
    void testMinimumAvailability() {
        std::cout << "\n=== Testing Minimum Availability Requirements ===" << std::endl;
        
        // Start with all instances and gradually reduce
        for (size_t available = instances.size(); available > 0; available--) {
            std::cout << "\nTesting with " << available << " instances..." << std::endl;
            
            // Kill instances.size() - available instances
            std::vector<size_t> to_kill;
            for (size_t i = available; i < instances.size(); i++) {
                to_kill.push_back(i);
            }
            
            auto killed = killInstances(to_kill, true);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            
            bool is_available = isServiceResponsive();
            std::cout << "Service available with " << available << " instances: " 
                     << (is_available ? "Yes" : "No") << std::endl;
            
            if (!is_available) {
                std::cout << "Minimum required instances for availability: " 
                         << (available + 1) << "/" << instances.size() << std::endl;
                restartInstances(killed, true);
                break;
            }
            
            restartInstances(killed, true);
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }
    }

    // Test 2: Test different failure patterns
    void testFailurePatterns() {
        std::cout << "\n=== Testing Different Failure Patterns ===" << std::endl;

        // Test sudden failures vs gradual failures
        testSuddenFailures();
        testGradualFailures();
        
        // Test clean vs unclean failures
        testCleanFailures();
        testUncleanFailures();
    }

private:
    void testSuddenFailures() {
        std::cout << "\nTesting Sudden Multiple Failures" << std::endl;
        
        // Kill half the instances simultaneously
        size_t half = instances.size() / 2;
        std::vector<size_t> to_kill;
        for (size_t i = 0; i < half; i++) {
            to_kill.push_back(i);
        }
        
        auto killed = killInstances(to_kill, true);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
        bool available = isServiceResponsive();
        std::cout << "Service available after sudden failure of " << half 
                  << " instances: " << (available ? "Yes" : "No") << std::endl;
        
        restartInstances(killed, true);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    void testGradualFailures() {
        std::cout << "\nTesting Gradual Failures" << std::endl;
        
        std::vector<std::string> killed_instances;
        size_t half = instances.size() / 2;
        
        // Kill instances one by one
        for (size_t i = 0; i < half; i++) {
            auto newly_killed = killInstances({i}, true);
            killed_instances.insert(killed_instances.end(), newly_killed.begin(), newly_killed.end());
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            
            bool available = isServiceResponsive();
            std::cout << "Service available after " << (i + 1) 
                      << " gradual failures: " << (available ? "Yes" : "No") << std::endl;
        }
        
        restartInstances(killed_instances, true);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    void testCleanFailures() {
        std::cout << "\nTesting Clean Failures" << std::endl;
        
        size_t num_to_kill = std::min(size_t(3), instances.size());
        std::vector<size_t> to_kill;
        for (size_t i = 0; i < num_to_kill; i++) {
            to_kill.push_back(i);
        }
        
        auto killed = killInstances(to_kill, true);  // Clean shutdown
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
        bool available = isServiceResponsive();
        std::cout << "Service available after clean shutdown of " << num_to_kill 
                  << " instances: " << (available ? "Yes" : "No") << std::endl;
        
        restartInstances(killed, true);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }

    void testUncleanFailures() {
        std::cout << "\nTesting Unclean Failures" << std::endl;
        
        size_t num_to_kill = std::min(size_t(3), instances.size());
        std::vector<size_t> to_kill;
        for (size_t i = 0; i < num_to_kill; i++) {
            to_kill.push_back(i);
        }
        
        auto killed = killInstances(to_kill, false);  // Unclean shutdown
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
        bool available = isServiceResponsive();
        std::cout << "Service available after unclean shutdown of " << num_to_kill 
                  << " instances: " << (available ? "Yes" : "No") << std::endl;
        
        restartInstances(killed, true);
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    AvailabilityTester tester;
    if (!tester.initialize(argv[1])) {
        std::cerr << "Failed to initialize tester" << std::endl;
        return 1;
    }

    try {
        tester.testMinimumAvailability();
        tester.testFailurePatterns();
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
    }

    tester.cleanup();
    return 0;
}