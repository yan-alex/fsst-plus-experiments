#pragma once
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "basic_fsst.h"
#include "generic_utils.h"
#include <block_types.h>

inline bool TryAddPrefix(BlockSizingMetadata &sm,
                         BlockWritingMetadata &wm,
                         const FSSTCompressionResult &prefix_compression_result,
                         const size_t prefix_index_for_suffix) {
    const size_t prefix_size = prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
    if (sm.block_size + prefix_size >= config::block_byte_capacity) {
        return false;
    }
    wm.prefix_offsets_from_first_prefix[wm.prefix_n_in_block] = wm.prefix_area_size;
    wm.prefix_n_in_block += 1;
    wm.prefix_area_size += prefix_size;
    sm.block_size += prefix_size;
    sm.prefix_last_index_added = prefix_index_for_suffix;
    return true;
}

inline size_t CalculateSuffixPlusHeaderSize(const FSSTCompressionResult &suffix_compression_result,
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

inline bool CanFitInBlock(const BlockSizingMetadata &bsm,
                          const size_t additional_size) {
    // We also need space for one uint16_t header offset
    constexpr size_t suffix_block_header_offset = sizeof(uint16_t);
    return (bsm.block_size + additional_size + suffix_block_header_offset
            < config::block_byte_capacity);
}

/*
 * This function's purpose is to figure out how many prefixes and suffixes can be grouped into a single block,
 * without exceeding the block’s byte capacity
 */
inline size_t CalculateBlockSizeAndPopulateWritingMetadata(const std::vector<SimilarityChunk> &similarity_chunks,
                                 const FSSTCompressionResult &prefix_compression_result,
                                 const FSSTCompressionResult &suffix_compression_result,
                                 BlockWritingMetadata &wm,
                                 const size_t suffix_area_start_index) {
    BlockSizingMetadata sm;
    // Start with the space for num_strings
    sm.block_size += sizeof(uint8_t);

    // Try to fit as many suffixes as possible, up to 128
    size_t strings_to_go = suffix_compression_result.encoded_strings.size() - suffix_area_start_index;
    while (wm.suffix_n_in_block < std::min(strings_to_go, config::block_granularity)) {
        const size_t suffix_index = suffix_area_start_index + wm.suffix_n_in_block; // starts at 0
        const size_t prefix_index_for_suffix =
            find_similarity_chunk_corresponding_to_index(suffix_index, similarity_chunks);

        // If new prefix is needed, try to add it
        if (prefix_index_for_suffix != sm.prefix_last_index_added) {
            if (!TryAddPrefix(sm, wm, prefix_compression_result, prefix_index_for_suffix)) {
                break;
            }
        }

        // Calculate suffix size
        size_t suffix_size = CalculateSuffixPlusHeaderSize(
            suffix_compression_result, similarity_chunks, suffix_index
        );
        // Check capacity
        if (!CanFitInBlock(sm, suffix_size)) {
            break;
        }

        // We can fit the suffix plus its offset in the block header
        constexpr size_t block_header_suffix_offset_size = sizeof(uint16_t);
        sm.block_size += suffix_size + block_header_suffix_offset_size;

        // Update suffix metadata
        wm.suffix_offsets_from_first_suffix[wm.suffix_n_in_block] = sm.suffix_offset_current;
        wm.suffix_encoded_prefix_lengths[wm.suffix_n_in_block] =
            prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
        wm.suffix_prefix_index[wm.suffix_n_in_block] = wm.prefix_n_in_block - 1;
        sm.suffix_offset_current += suffix_size;
        wm.suffix_n_in_block += 1;
    }

    std::cout << "\n 🟪 BLOCK SIZING RESULTS: N Strings: " << wm.suffix_n_in_block
              << " N Prefixes: " << wm.prefix_n_in_block
              << " sm.block_size: " << sm.block_size
              << " wm.prefix_area_size: " << wm.prefix_area_size << " 🟪 \n";

    return sm.block_size;
}
