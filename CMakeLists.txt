cmake_minimum_required(VERSION 3.26)
project(ThreadPool LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_executable(ThreadPool
    src/main.cpp
 src/thread_pool.cpp)
target_include_directories(ThreadPool PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
