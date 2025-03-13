//
// Created by Yan Lanna Alexandre on 07/03/2025.
//
#pragma once
#include "cleaving_types.h"
#include <iostream>
#include <vector>

inline size_t find_similarity_chunk_corresponding_to_index(const size_t &target_index,
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
    std::cerr << "Couldn't find_similarity_chunk_corresponding_to_index: " << target_index << "\n";
    throw std::logic_error("ERROR on find_similarity_chunk_corresponding_to_index()");
}
