cmake_minimum_required(VERSION 2.8.12...3.29)
project(fsst-plus-experiments)

set(CMAKE_CXX_STANDARD 11)

include_directories(third_party/duckdb/src/include)
link_directories(third_party/duckdb/build/release/src)

include_directories(third_party/fsst)
add_subdirectory(third_party/fsst)

include_directories(src/cleaving)

add_executable(main src/main.cpp)
target_link_libraries(main duckdb fsst)

add_executable(basic_fsst src/basic_fsst.cpp)
target_link_libraries(basic_fsst duckdb fsst)

add_executable(cleaving_test test/cleaving_test.cpp)
target_link_libraries(cleaving_test duckdb fsst)

add_executable(example src/duckdb_loading_data_example.cpp)
target_link_libraries(example duckdb)





