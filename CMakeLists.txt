cmake_minimum_required(VERSION 3.10)
project(btree CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(THREADS_PREFER_PTHREAD_FLAG ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O2 -pthread -march=native")

# Set default build type to Debug if not specified
# if(NOT CMAKE_BUILD_TYPE)
#   set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build type" FORCE)
#   endif()
# # Add debug flags for GDB
# set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra")


find_package(Threads REQUIRED)
set(TBB_DIR "/usr/lib/x86_64-linux-gnu/cmake/TBB")

find_package(TBB REQUIRED)

add_executable(btree btree/btree.cpp)
target_include_directories(btree PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(btree PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/btree)
target_link_libraries(btree PRIVATE Threads::Threads numa)

add_executable(lsm lsm/lsm.cpp)
target_include_directories(lsm PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(lsm PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/lsm)
target_link_libraries(lsm PRIVATE Threads::Threads numa TBB::tbb) 