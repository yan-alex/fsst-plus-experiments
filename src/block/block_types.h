#pragma once
#include <ranges>

struct BlockMetadata {
    size_t prefix_n_in_block = 0; // number of strings inside the block
    size_t prefix_last_index_added = UINT64_MAX;
    size_t prefix_area_size = 0;
    std::array<uint16_t, config::block_granularity> prefix_offsets_from_first_prefix{};

    size_t suffix_n_in_block = 0; // number of strings inside the block
    uint16_t suffix_offset_current = 0;
    std::array<uint16_t, config::block_granularity> suffix_offsets_from_first_suffix{};
    std::array<uint8_t, config::block_granularity> suffix_encoded_prefix_lengths{}; // the length of the prefix for suffix i
    std::array<uint8_t, config::block_granularity> suffix_prefix_index{}; // the index of the prefix for suffix i


    size_t block_size = 0;
};
