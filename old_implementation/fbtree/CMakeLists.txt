cmake_minimum_required(VERSION 3.10)
project(FBTreeBenchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Enable AVX2 for SIMD256 support
add_compile_definitions(AVX2_ENABLE)

# Find required packages
find_package(Threads REQUIRED)

# Check for NUMA library
find_library(NUMA_LIBRARY numa)
if(NUMA_LIBRARY)
    set(NUMA_LIBRARIES ${NUMA_LIBRARY})
    add_compile_definitions(HAVE_NUMA)
else()
    message(WARNING "NUMA library not found. NUMA functionality will be disabled.")
    set(NUMA_LIBRARIES "")
endif()

# Check for TBB library
find_library(TBB_LIBRARY tbb)
if(TBB_LIBRARY)
    set(TBB_LIBRARIES ${TBB_LIBRARY})
    add_compile_definitions(HAVE_TBB)
else()
    message(WARNING "TBB library not found. Trying to find tbbmalloc...")
    find_library(TBBMALLOC_LIBRARY tbbmalloc)
    if(TBBMALLOC_LIBRARY)
        set(TBB_LIBRARIES ${TBBMALLOC_LIBRARY})
        add_compile_definitions(HAVE_TBB)
    else()
        message(FATAL_ERROR "TBB library not found. FBTree requires Intel TBB.")
    endif()
endif()

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/../../IndexResearch/FBTree
    ${CMAKE_SOURCE_DIR}/../../IndexResearch/util
    ${CMAKE_SOURCE_DIR}/../../IndexResearch
    ${CMAKE_SOURCE_DIR}/../..
)

# Add source files
set(SOURCES
    fbtree.cpp
)

# Create executable
add_executable(fbtree_benchmark ${SOURCES})

# Link libraries
target_link_libraries(fbtree_benchmark
    ${NUMA_LIBRARIES}
    ${TBB_LIBRARIES}
    Threads::Threads
)

# Set compiler flags
target_compile_options(fbtree_benchmark PRIVATE
    -mavx2
    -msse4.2
    -O3
    -DNDEBUG
) 