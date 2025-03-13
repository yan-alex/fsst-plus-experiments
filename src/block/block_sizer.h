#pragma once
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "basic_fsst.h"
#include "generic_utils.h"
#include <block_types.h>


inline void calculate_block_size(const std::vector<SimilarityChunk> &similarity_chunks,
                                 const FSSTCompressionResult &prefix_compression_result,
                                 const FSSTCompressionResult &suffix_compression_result, BlockMetadata &b,
                                 const size_t suffix_area_start_index) {
    // Each blocks starts with the num_strings (uint8_t) and the base offset (uint8_t)
    constexpr size_t initial_block_size = sizeof(uint8_t) + sizeof(uint8_t);
    b.block_size += initial_block_size;

    while (b.suffix_n_in_block < config::compressed_block_granularity) {
        const size_t suffix_index = suffix_area_start_index + b.suffix_n_in_block;
        const size_t prefix_index_for_suffix = find_similarity_chunk_corresponding_to_index(suffix_index, similarity_chunks);
        const SimilarityChunk &chunk = similarity_chunks[prefix_index_for_suffix];
        const bool suffix_has_prefix = chunk.prefix_length != 0;

        if (prefix_index_for_suffix != b.prefix_last_index_added) {
            // we add a new prefix to our block. The block grows only by the size of the encoded prefix
            const size_t prefix_size = prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
            const size_t block_size_with_prefix = b.block_size + prefix_size;

            // exit loop if we can't fit prefix
            if (block_size_with_prefix >= config::compressed_block_byte_capacity) {
                break;
            }

            std::cout << "Add Prefix " << b.prefix_n_in_block << " Length=" << prefix_size << '\n';

            // increase the number of prefixes for this block, add the size to the block size
            b.prefix_offsets_from_first_prefix[b.prefix_n_in_block] = b.prefix_area_size;
            b.prefix_n_in_block += 1;
            b.prefix_area_size += prefix_size;
            b.block_size += prefix_size;


            // make sure we don't add it again
            b.prefix_last_index_added = prefix_index_for_suffix;
        }

        // calculate the potential size of the suffix
        size_t suffix_total_size = 0;
        // 1) We also have the encoded suffix
        suffix_total_size += suffix_compression_result.encoded_strings_length[suffix_index];
        // 2) We will always add the size of the prefix for this suffix, 0 if there is no prefix
        suffix_total_size += sizeof(uint8_t);
        // 3) Prefix length != zero: We have to add the jumpback (uint16_t)
        if (suffix_has_prefix) {
            suffix_total_size += sizeof(uint16_t);
        }
        // 4) This means we have to add a new string offset in the block-header (uint16_t)
        constexpr size_t suffix_block_header_size = sizeof(uint16_t);

        // exit loop if we can't fit suffix
        if (b.block_size + suffix_total_size + suffix_block_header_size >= config::compressed_block_byte_capacity) {
            break;
        }
        // store the string offset
        b.suffix_offsets_from_first_suffix[b.suffix_n_in_block] = b.suffix_offset_current;
        b.suffix_encoded_prefix_lengths[b.suffix_n_in_block] = prefix_compression_result.encoded_strings_length[
            prefix_index_for_suffix]; // the ENCODED prefix length
        b.suffix_prefix_index[b.suffix_n_in_block] = b.prefix_n_in_block - 1;
        // as we already increased prefix_n_in_block before
        b.suffix_offset_current += suffix_total_size;

        // we can add the suffix
        b.block_size += suffix_total_size;
        b.suffix_n_in_block += 1;
    }

    std::cout << "N Strings: " << b.suffix_n_in_block << " N Prefixes: " << b.prefix_n_in_block << " Block size: " << b.
            block_size << " Pre size: " << b.prefix_area_size << std::endl;
}
