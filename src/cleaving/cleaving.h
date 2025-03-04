//
// Created by Yan Lanna Alexandre on 04/03/2025.
//
#include <ranges>

#ifndef CLEAVING_H
#define CLEAVING_H


struct SimilarityChunk {
    size_t start_index; // Starts here and goes on until next chunk's index, or until the end of the 128 block
    size_t prefix_length;
};

void truncated_sort(
    std::vector<size_t> &lenIn,
    std::vector<const unsigned char *> &strIn,
    size_t start_index);

std::vector<SimilarityChunk> form_similarity_chunks(
    std::vector<size_t>& lenIn,
    std::vector<const unsigned char*>& strIn,
    size_t start_index);

void cleave(std::vector<size_t> &lenIn,
            std::vector<const unsigned char *> &strIn,
            const std::vector<SimilarityChunk> &similarity_chunks,
            std::vector<size_t> &prefixLenIn,
            std::vector<const unsigned char*> &prefixStrIn,
            std::vector<size_t> &suffixLenIn,
            std::vector<const unsigned char*> &suffixStrIn
);

struct CleavedInputs {
    std::vector<size_t> prefixLenIn;
    std::vector<const unsigned char*> prefixStrIn;
    std::vector<size_t> suffixLenIn;
    std::vector<const unsigned char*> suffixStrIn;
};

#endif //CLEAVING_H
