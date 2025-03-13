#pragma once
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "basic_fsst.h"

inline void write_block_header(const BlockMetadata &b, uint8_t *&current_data_ptr) {
    // A 1) Write the number of strings as an uint_8
    Store<uint8_t>(b.suffix_n_in_block, current_data_ptr);
    current_data_ptr += sizeof(uint8_t);
    // A 2) Write the string_offsets[]
    for (size_t i = 0; i < b.suffix_n_in_block; i++) {
        uint16_t offset_array_size_to_go = (b.suffix_n_in_block - i) * sizeof(uint16_t);
        uint16_t string_offset = b.prefix_area_size + offset_array_size_to_go + b.suffix_offsets_from_first_suffix[i];
        Store<uint16_t>(string_offset, current_data_ptr);
        current_data_ptr += sizeof(uint16_t);
    }
}

inline void write_prefix_area(const FSSTCompressionResult &prefix_compression_result, const BlockMetadata &b,
                              const size_t prefix_area_start_index, uint8_t *&current_data_ptr) {
    for (size_t i = 0; i < b.prefix_n_in_block; i++) {
        const size_t prefix_index = prefix_area_start_index + i;
        const size_t prefix_length = prefix_compression_result.encoded_strings_length[prefix_index];

        std::cout << "Write Prefix " << i << " Length=" << prefix_length << '\n';
        const unsigned char *prefix_start = prefix_compression_result.encoded_strings[prefix_index];
        memcpy(current_data_ptr, prefix_start, prefix_length);
        current_data_ptr += prefix_length;
    }
}

inline void write_suffix_area(const FSSTCompressionResult &suffix_compression_result, const BlockMetadata &b,
                              const size_t &suffix_area_start_index, uint8_t *&current_data_ptr) {
    for (size_t i = 0; i < b.suffix_n_in_block; i++) {
        const size_t suffix_index = suffix_area_start_index + i;

        uint8_t prefix_index_for_suffix = b.suffix_prefix_index[suffix_index];
        uint8_t suffix_prefix_length = b.suffix_encoded_prefix_lengths[suffix_index];
        const bool suffix_has_prefix = suffix_prefix_length != 0;

        // write the length of the prefix, can be zero
        Store<uint8_t>(suffix_prefix_length, current_data_ptr);
        current_data_ptr += sizeof(uint8_t);

        // if there is a prefix, calculate offset and store it
        if (suffix_has_prefix) {
            size_t prefix_offset_from_first_prefix = b.prefix_offsets_from_first_prefix[prefix_index_for_suffix];
            size_t suffix_offset_from_first_suffix = b.suffix_offsets_from_first_suffix[suffix_index];
            uint16_t prefix_jumpback_offset = (b.prefix_area_size - prefix_offset_from_first_prefix) +
                                              suffix_offset_from_first_suffix;

            Store<uint16_t>(prefix_jumpback_offset, current_data_ptr);
            current_data_ptr += sizeof(uint16_t);
        }

        // write the suffix
        const size_t suffix_length = suffix_compression_result.encoded_strings_length[suffix_index];
        const unsigned char *suffix_start = suffix_compression_result.encoded_strings[suffix_index];
        memcpy(current_data_ptr, suffix_start, suffix_length);
        current_data_ptr += suffix_length;
    }
}


inline void write_block(const FSSTPlusCompressionResult compression_result,
                        const FSSTCompressionResult &prefix_compression_result,
                        const FSSTCompressionResult &suffix_compression_result, const BlockMetadata &b,
                        const size_t suffix_area_start_index, const size_t prefix_area_start_index) {
    uint8_t *current_data_ptr = compression_result.data;

    // A) WRITE THE HEADER
    write_block_header(b, current_data_ptr);

    // B) WRITE THE PREFIX AREA
    write_prefix_area(prefix_compression_result, b, prefix_area_start_index, current_data_ptr);

    // C) WRITE SUFFIX AREA
    write_suffix_area(suffix_compression_result, b, suffix_area_start_index, current_data_ptr);

    std::cout << "Just wrote block of " << b.block_size << " bytes\n";
}
