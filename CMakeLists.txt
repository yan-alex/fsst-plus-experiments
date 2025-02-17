cmake_minimum_required(VERSION 2.8.12...3.29)
project(example-c++)

set(CMAKE_CXX_STANDARD 11)

include_directories(duckdb/src/include)
link_directories(duckdb/build/release/src)

add_executable(main main.cpp)
target_link_libraries(main duckdb fsst)

add_executable(example duckdb_loading_data_example.cpp)
target_link_libraries(example duckdb)

include_directories(fsst)
add_subdirectory(fsst)


