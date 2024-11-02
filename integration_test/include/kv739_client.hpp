#pragma once

#include <memory>
#include <string>
#include <stdexcept>

// Extern declarations for FFI functions
extern "C" {
// system interface
int kv739_init(char *config_file);
int kv739_get(char * key, char * value); 
int kv739_put(char * key, char * value, char * old_value); 
int kv739_die(char * instance_name, int clean);  
int kv739_start(char * instance_name, int clean); 
int kv739_leave(char * instance_name, int clean); 
int kv739_shutdown(); 


// for tests only
int kv739_launch_server(char *config_file);
}
