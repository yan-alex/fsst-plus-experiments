#pragma once
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "basic_fsst.h"

inline void WriteBlockHeader(const BlockWritingMetadata &wm, uint8_t *&current_data_ptr) {
    // A 1) Write the number of strings as an uint_8
    Store<uint8_t>(wm.suffix_n_in_block, current_data_ptr);
    current_data_ptr += sizeof(uint8_t);
    std::cout << "Write BlockHeader num_strings: " << wm.suffix_n_in_block << "\n";

    // A 2) Write the suffix_data_area_offsets[]
    for (size_t i = 0; i < wm.suffix_n_in_block; i++) {
        const uint16_t offset_array_size_to_go = (wm.suffix_n_in_block - i) * sizeof(uint16_t) - sizeof(uint16_t); // Count itself with - sizeof(uint16_t). So the offsetting starts at the value's end.
        uint16_t suffix_data_area_offset = wm.prefix_area_size + offset_array_size_to_go + wm.suffix_offsets_from_first_suffix[i];
        Store<uint16_t>(suffix_data_area_offset, current_data_ptr);
        current_data_ptr += sizeof(uint16_t);
    }
}

inline void WritePrefixArea(const FSSTCompressionResult &prefix_compression_result, const BlockWritingMetadata &wm,
                              const size_t prefix_area_start_index, uint8_t *&current_data_ptr) {
    for (size_t i = 0; i < wm.prefix_n_in_block; i++) {
        const size_t prefix_index = prefix_area_start_index + i;
        const size_t prefix_length = prefix_compression_result.encoded_strings_length[prefix_index];

        // std::cout << "Write Prefix " << i << " Length=" << prefix_length << '\n';
        const unsigned char *prefix_start = prefix_compression_result.encoded_strings[prefix_index];
        memcpy(current_data_ptr, prefix_start, prefix_length);
        current_data_ptr += prefix_length;
    }
}

inline void WriteSuffixArea(const FSSTCompressionResult &suffix_compression_result, const BlockWritingMetadata &wm,
                              const size_t &suffix_area_start_index, uint8_t *&current_data_ptr) {
    for (size_t i = 0; i < wm.suffix_n_in_block; i++) {
        const size_t suffix_index = suffix_area_start_index + i;
        
        // Add bounds check
        if (suffix_index >= suffix_compression_result.encoded_strings.size()) {
            std::cerr << "⚠️ Invalid suffix index: " << suffix_index << "\n";
            break;
        }
        
        uint8_t prefix_index_for_suffix = wm.suffix_prefix_index[i];
        uint8_t suffix_prefix_length = wm.suffix_encoded_prefix_lengths[i];
        const bool suffix_has_prefix = suffix_prefix_length != 0;

        // write the length of the prefix, can be zero
        Store<uint8_t>(suffix_prefix_length, current_data_ptr);
        current_data_ptr += sizeof(uint8_t);

        // if there is a prefix, calculate offset and store it
        if (suffix_has_prefix) {
            size_t prefix_offset_from_first_prefix = wm.prefix_offsets_from_first_prefix[prefix_index_for_suffix];
            size_t suffix_offset_from_first_suffix = wm.suffix_offsets_from_first_suffix[i]; // should it index by suffix_index or by i?
            uint16_t prefix_jumpback_offset = (wm.prefix_area_size - prefix_offset_from_first_prefix) +
                                              suffix_offset_from_first_suffix; // TODO: Is too big on the second block.

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


inline uint8_t * WriteBlock(uint8_t *block_start,
                        const FSSTCompressionResult &prefix_compression_result,
                        const FSSTCompressionResult &suffix_compression_result, const BlockWritingMetadata &wm) {

    uint8_t *current_data_ptr = block_start;
    std::cout << "current_data_ptr before writing block " << static_cast<const void*>(current_data_ptr) << "\n";

    // A) WRITE THE HEADER
    WriteBlockHeader(wm, current_data_ptr);

    // B) WRITE THE PREFIX AREA // TODO: Wrong for block 2
    WritePrefixArea(prefix_compression_result, wm, wm.prefix_area_start_index, current_data_ptr);

    // C) WRITE SUFFIX AREA
    WriteSuffixArea(suffix_compression_result, wm, wm.suffix_area_start_index, current_data_ptr);


    std::cout << "current_data_ptr after writing block " << static_cast<const void*>(current_data_ptr) << "\n";
    std::cout << "difference: " << (current_data_ptr - block_start) << "\n\n";
    return current_data_ptr;
}
