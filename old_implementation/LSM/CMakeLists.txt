cmake_minimum_required(VERSION 3.10)

# LSM Memtable implementations
add_library(lsm_memtable STATIC
    memtable/skiplist_memtable.cpp
    memtable/skiplist_memtable.h
    memtable/custom_skiplist.h
    memtable/custom_skiplist_memtable.cpp
    memtable/custom_skiplist_memtable.h
    memtable/memtable_interface.h
    memtable/memtable_factory.cpp
    memtable/global.h
)

target_include_directories(lsm_memtable PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/memtable
)

target_compile_features(lsm_memtable PUBLIC cxx_std_17)

# Benchmark executables
add_executable(memtable_benchmark
    benchmark/memtable_benchmark.cpp
)

add_executable(simple_memtable_benchmark
    benchmark/simple_memtable_benchmark.cpp
)

target_link_libraries(memtable_benchmark
    lsm_memtable
    pthread
    numa
    util
)

target_link_libraries(simple_memtable_benchmark
    lsm_memtable
    pthread
    numa
    util
)

target_include_directories(memtable_benchmark PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/memtable
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark
)

target_include_directories(simple_memtable_benchmark PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/memtable
    ${CMAKE_CURRENT_SOURCE_DIR}/benchmark
)

# Test executable
add_executable(memtable_test
    test/memtable_test.cpp
)

target_link_libraries(memtable_test
    lsm_memtable
    pthread
    numa
    util
)

target_include_directories(memtable_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/memtable
    ${CMAKE_CURRENT_SOURCE_DIR}/test
) 