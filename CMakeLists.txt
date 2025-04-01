cmake_minimum_required(VERSION 2.8.12...3.29)
project(fsst-plus-experiments)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -g")

set(CMAKE_CXX_STANDARD 11)

include_directories(third_party/duckdb/src/include)
link_directories(third_party/duckdb/build/release/src)

include_directories(third_party/fsst)
add_subdirectory(third_party/fsst)

include_directories(src/cleaving)
include_directories(src/util)
include_directories(src/block)

add_executable(fsst_plus src/fsst_plus.cpp)
target_link_libraries(fsst_plus duckdb fsst)

add_executable(cleaving_test test/cleaving_test.cpp)
target_link_libraries(cleaving_test duckdb fsst)

add_executable(binary_search_test test/binary_search_test.cpp)







