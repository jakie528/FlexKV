#include "kv739_client.hpp"
#include <iostream>

using namespace std;

// int kv739_get(char * key, char * value); 
// int kv739_put(char * key, char * value, char * old_value); 
// int kv739_die(char * instance_name, int clean);  
// int kv739_start(char * instance_name, int clean); 
// int kv739_leave(char * instance_name, int clean); 

int main() {
    // launch server cluster 
    // kv739_launch_server( const_cast<char*>(""));

    kv739_init( const_cast<char*>("/users/wjhu/P3/RaftKV/flexkv-rocks/flexkv-rocksdb/cpp/example/config_sample"));
    char old_value[2049];
    int put_res = kv739_put( const_cast<char*>("key1"),  const_cast<char*>("value1"), old_value);
    if (put_res == 1) {
        cout << "write success!" << endl;
    } else {
      cout << "ERROR: failed to write value for key1!" << endl;
    }
    char value[2049];
    int get_res = kv739_get( const_cast<char*>("key1"), value);
    if (get_res == 0) {
        cout << "get value for key1 as: " << value << endl;
    } else {
      cout << "ERROR: failed to get value for key1!" << endl;
    }
   
  //  kv739_die(const_cast<char*>("127.0.0.1:21001"), 0);
  //   cout << "test get after die: " << endl;
 
  //  get_res = kv739_get( const_cast<char*>("key1"), value);
  //  if (get_res == 0) {
  //       cout << "get value for key1 as: " << value << endl;
  //  } else {
  //     cout << "ERROR: failed to get value for key1!" << endl;
  //  }

}

