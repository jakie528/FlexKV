# CMakeLists.txt
cmake_minimum_required(VERSION 3.10)
project(kv_integration_tests)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_definitions(-DPROJECT_ROOT_DIR="${CMAKE_SOURCE_DIR}")

# Add compiler warnings
add_compile_options(-Wall -Wextra)

## Find the shared library
#find_library(KV739_LIB 
#    NAMES flex_kv_store.so
#    PATHS ${CMAKE_SOURCE_DIR} ${CMAKE_SOURCE_DIR}/lib
#    NO_DEFAULT_PATH
#)

# Find the shared library
find_library(KV739_LIB
    NAMES flex_kv_store
    PATHS ${CMAKE_SOURCE_DIR}/../flexkv-rocks/flexkv-rocksdb/target/debug
          ${CMAKE_SOURCE_DIR}/../flexkv-rocks/flexkv-rocksdb/target/release
    NO_DEFAULT_PATH
)

if(NOT KV739_LIB)
    message(FATAL_ERROR "Cannot find flex_kv_store.so. Please ensure it's in the project root or lib directory.")
endif()

# Create a library target for common test utilities if needed
add_library(test_common INTERFACE)
target_include_directories(test_common INTERFACE ${CMAKE_SOURCE_DIR}/include)

# Function to add a test executable with proper linking
function(add_kv_test TEST_NAME TEST_SOURCE)
    add_executable(${TEST_NAME} ${TEST_SOURCE})
    target_link_libraries(${TEST_NAME} PRIVATE ${KV739_LIB} test_common pthread)
    # Add the test to CTest
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endfunction()

# Create test directories
# Availability Tests
add_kv_test(availability_test availability/availability.cpp)
add_kv_test(quorum_test availability/quorum_tests.cpp)

# Consistency Tests
add_kv_test(consistency_test consistency/consistency.cpp)
add_kv_test(multi_threaded_test consistency/multi_threaded.cpp)
add_kv_test(single_threaded_test consistency/single_threaded.cpp)

# Failure Tests
add_kv_test(failure_test failure/failure.cpp)

# Performance Tests
add_kv_test(performance_test perf/performance.cpp)
# add_kv_test(performance_multi_test perf/performance_multithread.cpp)

# Enable testing
enable_testing()

# Copy configuration file to binary directory
configure_file(${CMAKE_SOURCE_DIR}/servers.config 
              ${CMAKE_BINARY_DIR}/servers.config COPYONLY)

# Add a custom target to run all tests
add_custom_target(run_all_tests
    COMMAND ${CMAKE_CTEST_COMMAND} --output-on-failure
    DEPENDS availability_test 
            quorum_test 
            consistency_test 
            multi_threaded_test 
            single_threaded_test 
            failure_test 
            performance_test
            # performance_multi_test
)

# Add custom targets for running test categories
add_custom_target(run_availability_tests
    COMMAND ${CMAKE_CTEST_COMMAND} -R "availability|quorum" --output-on-failure
    DEPENDS availability_test quorum_test
)

add_custom_target(run_consistency_tests
    COMMAND ${CMAKE_CTEST_COMMAND} -R "consistency|threaded" --output-on-failure
    DEPENDS consistency_test multi_threaded_test single_threaded_test
)

add_custom_target(run_failure_tests
    COMMAND ${CMAKE_CTEST_COMMAND} -R "failure" --output-on-failure
    DEPENDS failure_test
)

add_custom_target(run_performance_tests
    COMMAND ${CMAKE_CTEST_COMMAND} -R "performance" --output-on-failure
    DEPENDS performance_test
)
