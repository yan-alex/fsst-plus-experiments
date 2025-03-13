#pragma once
#include <array>
#include <vector>
#include "config.h"




struct FSSTPlusCompressionResult {
    // uint32_t n_blocks;
    // uint32_t* blocks_offset; //TODO: Should be array of uint32
    uint8_t* data;
    // std::vector<CompressedBlock> compressed_blocks;
};

struct CheckpointInfo {
    size_t checkpoint_string_index;
    size_t checkpoint_similarity_chunk_index;
};


void print_compression_stats(size_t total_strings_amount, size_t total_string_size, size_t total_compressed_string_size);