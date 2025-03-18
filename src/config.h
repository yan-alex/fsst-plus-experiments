#pragma once
#include <ranges>


namespace config {
    extern const size_t total_strings;
    extern const size_t block_byte_capacity;
    extern const bool print_sorted_corpus; // whether to print sorted strings.
    extern const bool print_split_points; // whether to print split points.

    constexpr size_t block_granularity = 128;// number of elements per cleaving run.// TODO: Should be determined dynamically based on the string size. If the string is >32kb it can dreadfully compress to 64kb so we can't do jumpback. In that case compressed_block_granularity = 1. And btw, can't it be 129 if it fits? why not?
    constexpr size_t max_prefix_size = 120; // how far into the string to scan for a prefix. (max prefix size)
    constexpr size_t amount_strings_per_symbol_table = 120000; // 120000 = a duckdb row group
    size_t global_index = 0;

}
