// #pragma once

// #include <memory>
// #include <string>
// #include <stdexcept>


// // Forward declaration of Rust types
// struct KV739Client;
// struct FFIResult;
// // Extern declarations for FFI functions
// extern "C" {
//     struct FFIResult {
//         bool success;
//         char* error_msg;
//         char* data;
//     };

//     KV739Client* kv739_client_new(uint64_t leader_id, const char* leader_addr);
//     void kv739_client_free(KV739Client* client);
//     FFIResult kv739_client_init(KV739Client* client);
//     FFIResult kv739_client_get(KV739Client* client, const char* key);
//     FFIResult kv739_client_put(KV739Client* client, const char* key, const char* value);
//     FFIResult kv739_client_start(KV739Client* client, uint64_t node_id, const char* addr);
//     FFIResult kv739_client_leave(KV739Client* client, uint64_t node_id);
//     void kv739_free_result(FFIResult result);
// }





// class KV739Exception : public std::runtime_error {
// public:
//     explicit KV739Exception(const std::string& msg) : std::runtime_error(msg) {}
// };

// class KV739ClientCpp {
// private:
//     struct Deleter {
//         void operator()(KV739Client* client) {
//             if (client) {
//                 kv739_client_free(client);
//             }
//         }
//     };

//     std::unique_ptr<KV739Client, Deleter> client;

//     // Helper to process FFI results
//     static std::string process_result(FFIResult result) {
//         std::string data;
//         if (result.success) {
//             if (result.data) {
//                 data = std::string(result.data);
//             }
//         } else {
//             std::string error_msg(result.error_msg);
//             kv739_free_result(result);
//             throw KV739Exception(error_msg);
//         }
//         kv739_free_result(result);
//         return data;
//     }

// public:
//     KV739ClientCpp(uint64_t leader_id, const std::string& leader_addr) {
//         client.reset(kv739_client_new(leader_id, leader_addr.c_str()));
//         if (!client) {
//             throw KV739Exception("Failed to create client");
//         }
//     }

//     void init() {
//         auto result = kv739_client_init(client.get());
//         process_result(result);
//     }

//     std::string get(const std::string& key) {
//         auto result = kv739_client_get(client.get(), key.c_str());
//         return process_result(result);
//     }

//     void put(const std::string& key, const std::string& value) {
//         auto result = kv739_client_put(client.get(), key.c_str(), value.c_str());
//         process_result(result);
//     }

//     void start(uint64_t node_id, const std::string& addr) {
//         auto result = kv739_client_start(client.get(), node_id, addr.c_str());
//         process_result(result);
//     }

//     void leave(uint64_t node_id) {
//         auto result = kv739_client_leave(client.get(), node_id);
//         process_result(result);
//     }
// };

