cmake_minimum_required(VERSION 3.15)
project(kv739_cpp_client)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Rust library
find_library(RAFT_KV_LIB
    NAMES flex_kv_store
    PATHS ${CMAKE_SOURCE_DIR}/../target/debug
          ${CMAKE_SOURCE_DIR}/../target/release
    NO_DEFAULT_PATH
)

if(NOT RAFT_KV_LIB)
    message(FATAL_ERROR "Rust library not found. Please build it first using 'cargo build'")
endif()

# Add include directory
include_directories(${CMAKE_SOURCE_DIR}/include)

# Add library target
add_library(kv739_cpp INTERFACE)
target_include_directories(kv739_cpp INTERFACE
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
)

# Add example executable
add_executable(kv739_example example/main.cpp)
target_link_libraries(kv739_example
    PRIVATE
    ${RAFT_KV_LIB}
    kv739_cpp
    pthread
    dl
)

# Installation rules
install(TARGETS kv739_cpp
    EXPORT kv739_cpp-targets
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
    INCLUDES DESTINATION include
)

install(
    DIRECTORY include/
    DESTINATION include
)

# Export targets
install(EXPORT kv739_cpp-targets
    FILE kv739_cpp-targets.cmake
    NAMESPACE kv739::
    DESTINATION lib/cmake/kv739_cpp
)

# Create and install config file
include(CMakePackageConfigHelpers)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/kv739_cpp-config-version.cmake"
    VERSION 0.9.17
    COMPATIBILITY SameMajorVersion
)

configure_package_config_file(
    "${CMAKE_CURRENT_SOURCE_DIR}/cmake/kv739_cpp-config.cmake.in"
    "${CMAKE_CURRENT_BINARY_DIR}/kv739_cpp-config.cmake"
    INSTALL_DESTINATION lib/cmake/kv739_cpp
)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/kv739_cpp-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/kv739_cpp-config-version.cmake"
    DESTINATION lib/cmake/kv739_cpp
)