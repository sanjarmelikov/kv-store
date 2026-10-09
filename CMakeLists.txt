cmake_minimum_required(VERSION 3.16)
project(kv_store CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()
add_compile_options(-Wall -Wextra -Wpedantic)

# Everything except main(), shared by the server and the unit tests.
add_library(kvcore STATIC
  src/resp.cpp src/store.cpp src/aof.cpp src/commands.cpp src/server.cpp
  src/poller_epoll.cpp src/poller_kqueue.cpp)
target_include_directories(kvcore PUBLIC src)

add_executable(kv-store src/main.cpp)
target_link_libraries(kv-store kvcore)

add_executable(unit_tests tests/unit_tests.cpp)
target_link_libraries(unit_tests kvcore)

enable_testing()
add_test(NAME unit COMMAND unit_tests)
