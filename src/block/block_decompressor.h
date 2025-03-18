#pragma once
#include <ranges>
#include "duckdb.hpp"
#include <iostream>
#include <sys/stat.h>

#include "basic_fsst.h"

inline void decompress_block(const uint8_t *block_start, const fsst_decoder_t &prefix_decoder,
                      const fsst_decoder_t &suffix_decoder, const uint8_t *block_stop) {
    const size_t n_strings = Load<uint8_t>(block_start);
    std::cout << "Read BlockHeader num_strings: " << n_strings << "\n";
    const uint8_t *suffix_data_area_offsets_ptr = block_start + sizeof(uint8_t);

    constexpr size_t BUFFER_SIZE = 1000000;
    auto *result = new unsigned char[BUFFER_SIZE];

    for (int i = 0; i < n_strings; i ++ ) {
        const uint8_t *suffix_data_area_offset_ptr = suffix_data_area_offsets_ptr + i * sizeof(uint16_t);
        const uint16_t suffix_data_area_offset = Load<uint16_t>(suffix_data_area_offset_ptr);

        // Count itself with + sizeof(uint16_t). So the offsetting starts at the value's end.
        const uint8_t *suffix_data_area_start = suffix_data_area_offset_ptr + sizeof(uint16_t) + suffix_data_area_offset;
        const uint8_t prefix_length = Load<uint8_t>(suffix_data_area_start);

        uint16_t suffix_data_area_length;
        if (i < n_strings-1) {
            // Add +sizeof(uint16_t) because next offset starts from itself
            const uint16_t next_suffix_offset = Load<uint16_t>(suffix_data_area_offset_ptr + sizeof(uint16_t)); // next suffix offset
            // diff between this offset and next suffix offset
            suffix_data_area_length = next_suffix_offset + sizeof(uint16_t) - suffix_data_area_offset;
        } else {
            // last suffix, have to refer to block_stop to calc its length
             suffix_data_area_length = block_stop - suffix_data_area_start;
        }

        if (prefix_length == 0) {
            const uint8_t *encoded_suffix_ptr = suffix_data_area_start + sizeof(uint8_t);
            // suffix only
            const size_t decompressed_suffix_size = fsst_decompress(&suffix_decoder,
                                                                    suffix_data_area_length - sizeof(uint8_t),
                                                                    encoded_suffix_ptr, BUFFER_SIZE, result);
            std::cout << i << " decompressed: ";
            for (int j = 0; j < decompressed_suffix_size; j++) {
                std::cout << result[j];
            }
            std::cout << "\n";
        } else {
            const uint8_t *jumpback_offset_ptr = suffix_data_area_start + sizeof(uint8_t);
            const uint16_t jumpback_offset = Load<uint16_t>(jumpback_offset_ptr);

            const uint8_t *encoded_suffix_ptr = jumpback_offset_ptr + sizeof(uint16_t);

            const uint8_t *encoded_prefix_ptr =
                    encoded_suffix_ptr - jumpback_offset - sizeof(uint8_t) - sizeof(uint16_t);

            // Step 1) Decompress prefix
            const size_t decompressed_prefix_size = fsst_decompress(&prefix_decoder, prefix_length,
                                                                    encoded_prefix_ptr,
                                                                    BUFFER_SIZE, result);
            std::cout << i << " decompressed: ";
            for (int j = 0; j < decompressed_prefix_size; j++) {
                std::cout << result[j];
            }

            // Step 2) Decompress suffix
            const size_t decompressed_suffix_size = fsst_decompress(&suffix_decoder,
                                                                    suffix_data_area_length - sizeof(uint8_t) -
                                                                    sizeof(uint16_t),
                                                                    encoded_suffix_ptr,
                                                                    BUFFER_SIZE, result);
            for (int j = 0; j < decompressed_suffix_size; j++) {
                std::cout << result[j];
            }
            std::cout << "\n";
        }
    }
}
