cmake_minimum_required(VERSION 3.10)
project(btree CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O2 -pthread -march=native")

find_package(Threads REQUIRED)

add_executable(btree btree/btree.cpp)
target_include_directories(btree PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(btree PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/btree)
target_link_libraries(btree PRIVATE Threads::Threads numa) 