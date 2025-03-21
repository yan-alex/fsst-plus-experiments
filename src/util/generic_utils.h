//
// Created by Yan Lanna Alexandre on 07/03/2025.
//
#pragma once
#include "cleaving_types.h"
#include <vector>

inline size_t FindSimilarityChunkCorrespondingToIndex(const size_t &target_index,
                                                           const std::vector<SimilarityChunk> &similarity_chunks) {
    // Binary search
    size_t l = 0, r = similarity_chunks.size() - 1;
    while (l <= r) {
        const size_t m = l + (r - l) / 2;
        if (similarity_chunks[m].start_index < target_index && similarity_chunks[m + 1].start_index <= target_index) {
            l = m + 1;
            continue;
        }
        if (target_index < similarity_chunks[m].start_index) {
            r = m - 1;
            continue;
        }
        return m;
    }

    /*
     * When we can't find_similarity_chunk_corresponding_to_index,
     * assuming it corresponds to the last chunk
     */
    return similarity_chunks.size()-1; //TODO Is this correct for all occasions? Write tests for this binary search.
}


inline size_t CalculateInputSize(const StringCollection &input) {
    size_t total_string_size = 0;
    for (size_t string_length: input.lengths) {
        total_string_size += string_length;
    }
    return total_string_size;
}