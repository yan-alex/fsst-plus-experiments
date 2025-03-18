#pragma once
#include <ranges>
#include "duckdb.hpp"
#include <iostream>
#include "basic_fsst.h"
#include "../config.h"

inline bool TextMatches(const unsigned char *result, const unsigned char *original, const size_t size) {
    for (int i = 0; i < size; ++i) {
        if (result[i] != original[i]) {
            std::cout<<"MISMATCH: "<<"result[i]: "<<result[i] << "original[i]: "<< original[i];
            return false;
        }
    }
    return true;
}
inline void decompress_block(const uint8_t *block_start, const fsst_decoder_t &prefix_decoder,
const fsst_decoder_t &suffix_decoder, const uint8_t *block_stop,
std::vector<size_t> lenIn,
std::vector<const unsigned char *> strIn) {
    if (config::print_decompressed_corpus) {
        std::cout << " ------- Block " << config::global_index/128 << "\n";
    }

    const size_t n_strings = Load<uint8_t>(block_start);
    const uint8_t *suffix_data_area_offsets_ptr = block_start + sizeof(uint8_t);

    constexpr size_t BUFFER_SIZE = 1000000;
    unsigned char *result = new unsigned char[BUFFER_SIZE];

    for (int i = 0; i < n_strings; i ++ ) {
        const uint8_t *suffix_data_area_offset_ptr = suffix_data_area_offsets_ptr + i * sizeof(uint16_t);
        const uint16_t suffix_data_area_offset = Load<uint16_t>(suffix_data_area_offset_ptr);

        // Count itself with + sizeof(uint16_t). So the offsetting starts at the value's end.
        const uint8_t *suffix_data_area_start = suffix_data_area_offset_ptr + sizeof(uint16_t) + suffix_data_area_offset;
        const uint8_t prefix_length = Load<uint8_t>(suffix_data_area_start);

        uint16_t suffix_data_area_length;
        if (i < n_strings-1) {
            // By adding +sizeof(uint16_t) we get the next suffix_data_area_offset
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
            fsst_decompress(&suffix_decoder,
                            suffix_data_area_length - sizeof(uint8_t),
                            encoded_suffix_ptr, BUFFER_SIZE, result);
            if (config::print_decompressed_corpus) {
                std::cout << i << " decompressed: ";
                std::cout << result << "\n";
            }
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


            // Step 2) Decompress suffix
            const size_t decompressed_suffix_size = fsst_decompress(&suffix_decoder,
                                                                    suffix_data_area_length - sizeof(uint8_t) -
                                                                    sizeof(uint16_t),
                                                                    encoded_suffix_ptr,
                                                                    BUFFER_SIZE, result + decompressed_prefix_size);
            if (config::print_decompressed_corpus) {
                std::cout << i << " decompressed: ";
                std::cout << result << "\n";
            }

            // Test if it's correct!
            size_t decompressed_size = decompressed_suffix_size + decompressed_prefix_size;
            if (decompressed_size != lenIn[config::global_index] || !TextMatches(result, strIn[config::global_index], decompressed_suffix_size + decompressed_prefix_size)) {
                std::cerr << "‼️ ERROR: Decompression mismatch:\n"<<"result:   " << result << "\noriginal: " << strIn[config::global_index] << "\n";
            }


        }
        config::global_index += 1;
    }
}
