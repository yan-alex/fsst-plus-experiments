#pragma once
#include <ranges>

struct BlockWritingMetadata {
    /*
     * Signifies the number of different prefixes/suffixes inside the block.
     * Starts from 0 and goes up to 128. For suffix it ends up being equal to the number of strings in the block.
     * For prefix it's generally less.
     */
    size_t prefix_n_in_block = 0;
    size_t suffix_n_in_block = 0;

    size_t prefix_area_size = 0;
    std::array<uint16_t, config::block_granularity> prefix_offsets_from_first_prefix{};
    size_t prefix_area_start_index = 0;

    std::array<uint16_t, config::block_granularity> suffix_offsets_from_first_suffix{};
    std::array<uint8_t, config::block_granularity> suffix_encoded_prefix_lengths{}; // the length of the prefix for suffix i
    std::array<uint8_t, config::block_granularity> suffix_prefix_index{}; // the index of the prefix for suffix i
    size_t suffix_area_start_index = 0;
};

struct BlockSizingMetadata {
    size_t prefix_last_index_added = UINT64_MAX;
    uint16_t suffix_offset_current = 0;
    size_t block_size = 0;
};