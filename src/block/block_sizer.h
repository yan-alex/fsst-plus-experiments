#pragma once
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "basic_fsst.h"
#include "generic_utils.h"
#include <block_types.h>

inline bool TryAddPrefix(BlockMetadata &b,
                         const FSSTCompressionResult &prefix_compression_result,
                         const size_t prefix_index_for_suffix) {
    const size_t prefix_size = prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
    if (b.block_size + prefix_size >= config::block_byte_capacity) {
        return false;
    }
    b.prefix_offsets_from_first_prefix[b.prefix_n_in_block] = b.prefix_area_size;
    b.prefix_n_in_block += 1;
    b.prefix_area_size += prefix_size;
    b.block_size += prefix_size;
    b.prefix_last_index_added = prefix_index_for_suffix;
    return true;
}

inline size_t CalculateSuffixSize(const FSSTCompressionResult &suffix_compression_result,
                                  const std::vector<SimilarityChunk> &similarity_chunks,
                                  const size_t suffix_index) {
    const size_t suffix_encoded_length = suffix_compression_result.encoded_strings_length[suffix_index];
    constexpr size_t prefix_length_byte = sizeof(uint8_t);
    const bool suffix_has_prefix = (similarity_chunks[find_similarity_chunk_corresponding_to_index(
                                suffix_index, similarity_chunks
                              )].prefix_length != 0);
    const size_t jumpback_size = suffix_has_prefix ? sizeof(uint16_t) : 0;
    return suffix_encoded_length + prefix_length_byte + jumpback_size;
}

inline bool CanFitInBlock(const BlockMetadata &b,
                          const size_t additional_size) {
    // We also need space for one uint16_t header offset
    constexpr size_t suffix_block_header_offset = sizeof(uint16_t);
    return (b.block_size + additional_size + suffix_block_header_offset
            < config::block_byte_capacity);
}

/*
 * This function's purpose is to figure out how many prefixes and suffixes can be grouped into a single block,
 * without exceeding the block’s byte capacity
 */
inline void CalculateBlockSize(const std::vector<SimilarityChunk> &similarity_chunks,
                                 const FSSTCompressionResult &prefix_compression_result,
                                 const FSSTCompressionResult &suffix_compression_result, BlockMetadata &b,
                                 const size_t suffix_area_start_index) {
    // Start with the space for num_strings
    b.block_size += sizeof(uint8_t);

    // Try to fit as many suffixes as possible, up to 128
    while (b.suffix_n_in_block < config::block_granularity) {
        const size_t suffix_index = suffix_area_start_index + b.suffix_n_in_block; // starts at 0
        const size_t prefix_index_for_suffix =
            find_similarity_chunk_corresponding_to_index(suffix_index, similarity_chunks);

        // If new prefix is needed, try to add it
        if (prefix_index_for_suffix != b.prefix_last_index_added) {
            if (!TryAddPrefix(b, prefix_compression_result, prefix_index_for_suffix)) {
                break;
            }
        }

        // Calculate suffix size
        size_t suffix_size = CalculateSuffixSize(
            suffix_compression_result, similarity_chunks, suffix_index
        );
        // Check capacity
        if (!CanFitInBlock(b, suffix_size)) {
            break;
        }

        // We can fit the suffix plus its header offset
        constexpr size_t suffix_block_header_size = sizeof(uint16_t);
        b.block_size += suffix_block_header_size + suffix_size;

        // Update suffix metadata
        b.suffix_offsets_from_first_suffix[b.suffix_n_in_block] = b.suffix_offset_current;
        b.suffix_encoded_prefix_lengths[b.suffix_n_in_block] =
            prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
        b.suffix_prefix_index[b.suffix_n_in_block] = b.prefix_n_in_block - 1;
        b.suffix_offset_current += suffix_size;
        b.suffix_n_in_block += 1;
    }

    std::cout << "N Strings: " << b.suffix_n_in_block
              << " N Prefixes: " << b.prefix_n_in_block
              << " Block size: " << b.block_size
              << " Pre size: " << b.prefix_area_size << std::endl;
}
// /*
//  * into a single block without exceeding the block’s byte capacity
//  */
// inline void calculate_block_size(const std::vector<SimilarityChunk> &similarity_chunks,
//                                  const FSSTCompressionResult &prefix_compression_result,
//                                  const FSSTCompressionResult &suffix_compression_result, BlockMetadata &b,
//                                  const size_t suffix_area_start_index) {
//     // Each block starts with the num_strings (uint8_t)
//     b.block_size += sizeof(uint8_t);
//
//     while (b.suffix_n_in_block < config::compressed_block_granularity) {
//         const size_t suffix_index = suffix_area_start_index + b.suffix_n_in_block;
//         const size_t prefix_index_for_suffix = find_similarity_chunk_corresponding_to_index(suffix_index, similarity_chunks);
//         const SimilarityChunk &chunk = similarity_chunks[prefix_index_for_suffix];
//         const bool suffix_has_prefix = chunk.prefix_length != 0;
//
//         if (prefix_index_for_suffix != b.prefix_last_index_added) {
//             // Add prefix if needed
//             const size_t prefix_size = prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
//             const size_t block_size_with_prefix = b.block_size + prefix_size;
//
//             // Exit loop if we can't fit prefix
//             if (block_size_with_prefix >= config::compressed_block_byte_capacity) {
//                 break;
//             }
//             b.prefix_offsets_from_first_prefix[b.prefix_n_in_block] = b.prefix_area_size;
//             b.prefix_n_in_block += 1;
//             b.prefix_area_size += prefix_size;
//             b.block_size += prefix_size;
//             b.prefix_last_index_added = prefix_index_for_suffix;
//         }
//
//         // Potential size of the suffix
//         size_t suffix_total_size = 0;
//         suffix_total_size += suffix_compression_result.encoded_strings_length[suffix_index]; // encoded suffix
//         suffix_total_size += sizeof(uint8_t); // prefix length byte
//         if (suffix_has_prefix) {
//             suffix_total_size += sizeof(uint16_t); // jumpback when prefix != 0
//         }
//
//         // We'll also need space for one uint16_t in the block's header offsets
//         constexpr size_t suffix_block_header_size = sizeof(uint16_t);
//
//         // Exit loop if we can't fit suffix
//         if (b.block_size + suffix_total_size + suffix_block_header_size >= config::compressed_block_byte_capacity) {
//             break;
//         }
//
//         // Account for the header offset plus the suffix payload
//         b.block_size += suffix_block_header_size;
//         b.block_size += suffix_total_size;
//
//         b.suffix_offsets_from_first_suffix[b.suffix_n_in_block] = b.suffix_offset_current;
//         b.suffix_encoded_prefix_lengths[b.suffix_n_in_block] =
//             prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
//         b.suffix_prefix_index[b.suffix_n_in_block] = b.prefix_n_in_block - 1;
//
//         b.suffix_offset_current += suffix_total_size;
//         b.suffix_n_in_block += 1;
//     }
//
//     std::cout << "N Strings: " << b.suffix_n_in_block
//               << " N Prefixes: " << b.prefix_n_in_block
//               << " Block size: " << b.block_size
//               << " Pre size: " << b.prefix_area_size << std::endl;
// }
