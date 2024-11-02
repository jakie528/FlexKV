#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <chrono>
#include <thread>
#include <fstream>
#include <cassert>
#include <cstring>
#include <algorithm>

extern "C" {
    int kv739_init(char* config_file);
    int kv739_shutdown(void);
    int kv739_get(char* key, char* value);
    int kv739_put(char* key, char* value, char* old_value);
    int kv739_die(char* instance_name, int clean);
    int kv739_start(char* instance_name, int clean);
    int kv739_leave(char* instance_name, int clean);
}

class RaftQuorumTester {
private:
    std::vector<std::string> instances;
    const size_t MAX_VALUE_SIZE = 2049;
    
    // Calculate minimum nodes needed for quorum (f + 1)
    size_t getQuorumSize() {
        return (instances.size() / 2) + 1;
    }
    
    // Calculate maximum failures tolerable (f)
    size_t getMaxFailures() {
        return (instances.size() - 1) / 2;
    }

    bool loadInstances(const std::string& config_file) {
        std::ifstream file(config_file);
        if (!file.is_open()) {
            std::cerr << "Failed to open config file: " << config_file << std::endl;
            return false;
        }
        
        std::string instance;
        while (std::getline(file, instance)) {
            if (!instance.empty()) {
                instances.push_back(instance);
            }
        }
        
        std::cout << "Loaded " << instances.size() << " instances" << std::endl;
        std::cout << "Quorum size (f + 1): " << getQuorumSize() << std::endl;
        std::cout << "Max failures (f): " << getMaxFailures() << std::endl;
        return true;
    }

    // Test write operation
    bool testWrite(const std::string& key, const std::string& value) {
        char old_value[MAX_VALUE_SIZE];
        int result = kv739_put(
            const_cast<char*>(key.c_str()),
            const_cast<char*>(value.c_str()),
            old_value
        );
        return result >= 0;  // 0 or 1 indicates success
    }

    // Test read operation
    bool testRead(const std::string& key, std::string& value) {
        char value_buffer[MAX_VALUE_SIZE];
        int result = kv739_get(
            const_cast<char*>(key.c_str()),
            value_buffer
        );
        if (result == 0) {
            value = std::string(value_buffer);
            return true;
        }
        return false;
    }

    // Kill specific instances
    std::vector<std::string> killInstances(const std::vector<size_t>& indices, bool clean) {
        std::vector<std::string> killed;
        for (size_t idx : indices) {
            if (idx < instances.size()) {
                std::string instance = instances[idx];
                if (kv739_die(const_cast<char*>(instance.c_str()), clean) == 0) {
                    killed.push_back(instance);
                    std::cout << "Killed instance: " << instance << std::endl;
                }
            }
        }
        return killed;
    }

    // Restart instances
    void restartInstances(const std::vector<std::string>& killed_instances, bool clean) {
        for (const auto& instance : killed_instances) {
            std::cout << "Restarting instance: " << instance << std::endl;
            kv739_start(const_cast<char*>(instance.c_str()), clean);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

public:
    bool initialize(const std::string& config_file) {
        if (!loadInstances(config_file)) {
            return false;
        }
        return kv739_init(const_cast<char*>(config_file.c_str())) == 0;
    }
    
    void cleanup() {
        kv739_shutdown();
    }

    // Test 1: Quorum Maintenance Test
    void testQuorumMaintenance() {
        std::cout << "\n=== Running Quorum Maintenance Test ===" << std::endl;
        std::cout << "Testing system behavior up to maximum tolerable failures (f)" << std::endl;
        
        size_t max_failures = getMaxFailures();
        std::vector<std::string> killed_instances;
        
        // Initial write to verify system is working
        std::string test_key = "quorum_test_key";
        std::string test_value = "initial_value";
        bool initial_write = testWrite(test_key, test_value);
        assert(initial_write && "Initial write should succeed");
        
        // Kill nodes one by one up to f failures
        for (size_t i = 0; i < max_failures; i++) {
            std::cout << "\nKilling instance " << (i + 1) << " of " << max_failures << std::endl;
            
            auto newly_killed = killInstances({i}, true);
            killed_instances.insert(killed_instances.end(), newly_killed.begin(), newly_killed.end());
            
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
            
            // Test both read and write operations
            std::string new_value = "value_after_" + std::to_string(i + 1) + "_failures";
            bool write_success = testWrite(test_key, new_value);
            
            std::string read_value;
            bool read_success = testRead(test_key, read_value);
            
            std::cout << "Write operation with " << (i + 1) << " failures: " 
                      << (write_success ? "Succeeded" : "Failed") << std::endl;
            std::cout << "Read operation with " << (i + 1) << " failures: " 
                      << (read_success ? "Succeeded" : "Failed") << std::endl;
            
            assert(write_success && "Write should succeed with f or fewer failures");
            assert(read_success && "Read should succeed with f or fewer failures");
        }
        
        // Recovery
        restartInstances(killed_instances, true);
    }

    // Test 2: Quorum Loss Test
    void testQuorumLoss() {
        std::cout << "\n=== Running Quorum Loss Test ===" << std::endl;
        std::cout << "Testing system behavior beyond maximum tolerable failures (f)" << std::endl;
        
        size_t max_failures = getMaxFailures();
        size_t quorum_size = getQuorumSize();
        
        // Initial write to verify system is working
        std::string test_key = "quorum_loss_test_key";
        std::string test_value = "initial_value";
        bool initial_write = testWrite(test_key, test_value);
        assert(initial_write && "Initial write should succeed");
        
        // Kill f+1 nodes (more than maximum tolerable failures)
        std::vector<size_t> indices;
        for (size_t i = 0; i <= max_failures; i++) {
            indices.push_back(i);
        }
        
        std::cout << "\nKilling " << (max_failures + 1) << " instances (exceeding f)" << std::endl;
        auto killed_instances = killInstances(indices, true);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
        // Test operations after quorum loss
        std::string new_value = "value_after_quorum_loss";
        bool write_success = testWrite(test_key, new_value);
        
        std::string read_value;
        bool read_success = testRead(test_key, read_value);
        
        std::cout << "Write operation after quorum loss: " 
                  << (write_success ? "Succeeded" : "Failed") << std::endl;
        std::cout << "Read operation after quorum loss: " 
                  << (read_success ? "Succeeded" : "Failed") << std::endl;
        
        assert(!write_success && "Write should fail after quorum loss");
        assert(!read_success && "Read should fail after quorum loss");
        
        // Recovery
        restartInstances(killed_instances, true);
    }

    // Test 3: Quorum Recovery Test
    void testQuorumRecovery() {
        std::cout << "\n=== Running Quorum Recovery Test ===" << std::endl;
        
        size_t max_failures = getMaxFailures();
        std::vector<std::string> killed_instances;
        
        // Kill f+1 nodes to lose quorum
        std::vector<size_t> indices;
        for (size_t i = 0; i <= max_failures; i++) {
            indices.push_back(i);
        }
        
        std::cout << "Killing " << (max_failures + 1) << " instances to lose quorum" << std::endl;
        killed_instances = killInstances(indices, true);
        
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        
        // Verify quorum is lost
        std::string test_key = "recovery_test_key";
        std::string test_value = "recovery_test_value";
        bool write_during_loss = testWrite(test_key, test_value);
        assert(!write_during_loss && "Write should fail during quorum loss");
        
        // Recover nodes one by one until quorum is restored
        size_t nodes_to_restore = 1;  // We only need to restore one node to regain quorum
        std::cout << "\nRestoring " << nodes_to_restore << " instance(s) to regain quorum" << std::endl;
        
        for (size_t i = 0; i < nodes_to_restore; i++) {
            if (i < killed_instances.size()) {
                std::string instance = killed_instances[i];
                std::cout << "Restoring instance: " << instance << std::endl;
                kv739_start(const_cast<char*>(instance.c_str()), true);
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }
        }
        
        // Verify quorum is restored
        bool write_after_recovery = testWrite(test_key, test_value);
        std::cout << "Write operation after partial recovery: " 
                  << (write_after_recovery ? "Succeeded" : "Failed") << std::endl;
        assert(write_after_recovery && "Write should succeed after quorum is restored");
        
        // Restore remaining nodes
        for (size_t i = nodes_to_restore; i < killed_instances.size(); i++) {
            kv739_start(const_cast<char*>(killed_instances[i].c_str()), true);
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }
};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config_file>" << std::endl;
        return 1;
    }

    RaftQuorumTester tester;
    if (!tester.initialize(argv[1])) {
        std::cerr << "Failed to initialize tester" << std::endl;
        return 1;
    }

    try {
        // Run all quorum-based tests
        tester.testQuorumMaintenance();
        tester.testQuorumLoss();
        tester.testQuorumRecovery();
    } catch (const std::exception& e) {
        std::cerr << "Test failed with exception: " << e.what() << std::endl;
    }

    tester.cleanup();
    return 0;
}