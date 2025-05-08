//
// Created by Yan Lanna Alexandre on 04/03/2025.
//
#pragma once
#include <ranges>
#include "print_utils.h"
#include "../config.h" // Not needed but prevents ClionIDE from complaining
#include <algorithm>
#include <limits>
#include <unordered_set>

inline void Sort(std::vector<size_t> &lenIn, std::vector<unsigned char *> &strIn,
                           const size_t start_index, const size_t cleaving_run_n, StringCollection &input) {
    // Create index array
    std::vector<size_t> indices(cleaving_run_n);
    for (size_t i = start_index; i < start_index + cleaving_run_n; ++i) {
        indices[i - start_index] = i;
    }

    std::sort(indices.begin(), indices.end(), [&](size_t i, size_t j) {
        const size_t len_i = std::min(lenIn[i], config::max_prefix_size); //& ~7;
        const size_t len_j = std::min(lenIn[j], config::max_prefix_size);//& ~7;

        const int cmp = memcmp(strIn[i], strIn[j], std::min(len_i, len_j));
        return cmp < 0 || (cmp == 0 && len_i > len_j);
    });


    // Reorder both vectors based on sorted indices
    const std::vector<size_t> tmp_len(lenIn);
    const std::vector<unsigned char *> tmp_str(strIn);

    // Create temporary copies of the original input vectors
    const std::vector<size_t> tmp_input_lengths(input.lengths);
    const std::vector<const unsigned char *> tmp_input_string_ptrs(input.string_ptrs);

    for (size_t k = 0; k < cleaving_run_n; ++k) {
        lenIn[start_index + k] = tmp_len[indices[k]];
        strIn[start_index + k] = tmp_str[indices[k]];

        // ALSO REORDER THE ORIGINAL UNCOMPRESSED INPUT
        // we do this to be able to pairwise compare later on when we verify decompression, to see if it matches with the original
        input.lengths[start_index + k] = tmp_input_lengths[indices[k]];
        input.string_ptrs[start_index + k] = tmp_input_string_ptrs[indices[k]];
    }

    // Print strings
    if (config::print_sorted_corpus) {
        std::cout << "Sorted strings: \n";
        PrintEncodedStrings(lenIn, strIn);
    }
}

inline size_t CalculatePrefixMatch(const unsigned char * str1, const unsigned char * str2, const size_t length_to_check) {
    size_t l = 0;
    for (int i = 0; i < length_to_check; ++i) {
        if (str1[i] == str2[i]) {
            l++;
        } else {
            return l;
        }
    }
    return l;
};

inline std::vector<size_t> CalcLengthsForChunk(size_t prefix_index_in_chunk, const std::vector<unsigned char *> &strIn,
    const std::vector<size_t> &lenIn,
    const size_t start_index,
    const size_t size) {
    if (size == 1) {
        return std::vector<size_t>{0};
    }

    unsigned char * prefixStr = strIn[start_index + prefix_index_in_chunk];
    size_t prefixLen = lenIn[start_index + prefix_index_in_chunk];

    std::vector<size_t> prefix_lengths;
    for (int i = start_index; i < start_index+size; ++i) {
        prefix_lengths.push_back(CalculatePrefixMatch(strIn[i], prefixStr, std::min(std::min(prefixLen, lenIn[i]), config::max_prefix_size)));
    }

    return prefix_lengths;
}
int reverse_memcmp(const void *s1, const void *s2, size_t n) {
    if(n == 0)
        return 0;

    // Grab pointers to the end and walk backwards
    const unsigned char *p1 = (const unsigned char*)s1 + n - 1;
    const unsigned char *p2 = (const unsigned char*)s2 + n - 1;

    while(n > 0)
    {
        // If the current characters differ, return an appropriately signed
        // value; otherwise, keep searching backwards
        if(*p1 != *p2)
            return *p1 - *p2;
        p1--;
        p2--;
        n--;
    }

    return 0;
}
inline std::vector<EnhancedSimilarityChunk> FormEnhancedSimilarityChunks(
    const std::vector<size_t> &lenIn,
    const std::vector<unsigned char *> &strIn,
    const size_t start_index,
    const size_t size) {
    if (size == 0) return {}; // No strings to process

    // Precompute prefix sums of string lengths (cumulatively adding the length of each element)
    std::vector<size_t> length_prefix_sum(size + 1, 0);
    for (size_t i = 0; i < size; ++i) {
        length_prefix_sum[i + 1] = length_prefix_sum[i] + lenIn[start_index + i];
    }

    // Precompute all pairwise prefix matches
    // prefix_match_matrix[i][j] = longest common prefix between string at position (start_index + i) and (start_index + j)
    std::vector<std::vector<size_t>> prefix_match_matrix(size, std::vector<size_t>(size, 0));
    for (size_t i = 0; i < size; ++i) {
        prefix_match_matrix[i][i] = lenIn[start_index + i]; // Match with self is the full length
        for (size_t j = i + 1; j < size; ++j) {
            size_t match_length = CalculatePrefixMatch(
                strIn[start_index + i], 
                strIn[start_index + j], 
                std::min(
                    std::min(lenIn[start_index + i], lenIn[start_index + j]), 
                    config::max_prefix_size
                )
            );
            prefix_match_matrix[i][j] = match_length;
            prefix_match_matrix[j][i] = match_length; // Matrix is symmetric
        }
    }

    constexpr size_t INF = std::numeric_limits<size_t>::max();
    std::vector<size_t> dp(size + 1, INF);
    std::vector<size_t> start_index_for_i(size + 1, 0);
    std::vector<std::vector<size_t>> prefix_lengths_for_i(size + 1, std::vector<size_t>(size + 1, 0));
    std::vector<size_t> prefix_index_in_chunk_for_i(size + 1, 0);

    dp[0] = 0;

    // Dynamic programming to find the optimal partitioning
    for (size_t i = 1; i <= size; ++i) {
        for (size_t j = 0; j < i; ++j) {
            for (int prefix_index_in_chunk = 0; prefix_index_in_chunk < i-j; ++prefix_index_in_chunk) {
                if (prefix_index_in_chunk > 0) {
                    // Check if this prefix string is identical to any we've already processed
                    unsigned char* current_prefix = strIn[j + start_index + prefix_index_in_chunk];
                    size_t current_prefix_len = lenIn[j + start_index + prefix_index_in_chunk];
                    // Compare against all previously processed prefixes
                    unsigned char* prev_prefix = strIn[j + start_index + prefix_index_in_chunk-1];
                    size_t prev_prefix_len = lenIn[j + start_index + prefix_index_in_chunk-1];
                    
                    // Skip if lengths are different
                    if (current_prefix_len != prev_prefix_len) continue;
                    
                    // Compare actual string content
                    const size_t len_to_compare = std::min(current_prefix_len, config::max_prefix_size);
                    if (reverse_memcmp(current_prefix, prev_prefix, len_to_compare) == 0) {
                        continue;
                    }
                }
                // printf("Dynamic Programming i:%lu j:%lu prefix_index_in_chunk:%d \n", i, j, prefix_index_in_chunk);
                const size_t n = i - j;
                
                // Fast calculation of prefix lengths using precomputed matrix
                std::vector<size_t> prefix_lengths(n);
                for (size_t k = 0; k < n; ++k) {
                    // Get the precomputed match between string at position k in this chunk
                    // and the selected prefix string at position prefix_index_in_chunk
                    prefix_lengths[k] = prefix_match_matrix[j + k][j + prefix_index_in_chunk];
                }
                
                size_t overhead = 0;
                size_t compression_gain = 0;
                for (const size_t prefix_length : prefix_lengths) {
                    overhead += 1; // one byte uint8 always exists (prefix_length)
                    if (prefix_length != 0){
                        overhead += 2;
                    }

                    // also calculate savings right away
                    compression_gain += prefix_length;
                }
                compression_gain-=prefix_lengths[prefix_index_in_chunk]; // the prefix itself is not counted as gain

                const size_t sum_len = length_prefix_sum[i] - length_prefix_sum[j];

                const size_t total_cost = dp[j] + overhead + sum_len - compression_gain;

                if (total_cost < dp[i]) {
                    dp[i] = total_cost;
                    start_index_for_i[i] = j;
                    prefix_lengths_for_i[i] = prefix_lengths;
                    prefix_index_in_chunk_for_i[i] = prefix_index_in_chunk;
                }
            }
        }
    }

    // Reconstruct the chunks and their prefix lengths
    std::vector<EnhancedSimilarityChunk> chunks;
    size_t idx = size;
    while (idx > 0) {
        const size_t start_idx = start_index_for_i[idx];
        const std::vector<size_t> prefix_lengths = prefix_lengths_for_i[idx];
        const size_t prefix_index = prefix_index_in_chunk_for_i[idx];
        EnhancedSimilarityChunk chunk;
        chunk.start_index = start_index + start_idx;
        chunk.prefix_lengths = prefix_lengths;
        chunk.prefix_index =  prefix_index;
        chunks.push_back(chunk);
        idx = start_idx;
    }
    // The chunks are reversed, so we need to reverse them back
    std::reverse(chunks.begin(), chunks.end());

    return chunks;
}

inline CleavedResult Cleave(const std::vector<size_t> &lenIn,
                            const std::vector<unsigned char *> &strIn,
                            const std::vector<EnhancedSimilarityChunk> &enhanced_similarity_chunks,
                            size_t n
) {
    auto cleaved_result = CleavedResult(n);

    std::vector<size_t> *pl = &cleaved_result.prefixes.lengths;
    std::vector<const unsigned char *> *ps = &cleaved_result.prefixes.string_ptrs;
    std::vector<size_t> *sl = &cleaved_result.suffixes.lengths;
    std::vector<const unsigned char *> *ss = &cleaved_result.suffixes.string_ptrs;
    for (size_t i = 0; i < enhanced_similarity_chunks.size(); i++) {
        const EnhancedSimilarityChunk &chunk = enhanced_similarity_chunks[i];
        const size_t stop_index = i == enhanced_similarity_chunks.size() - 1
                                      ? lenIn.size()
                                      : enhanced_similarity_chunks[i + 1].start_index;
        // Prefix
        size_t prefix_length = chunk.prefix_lengths[chunk.prefix_index];
        pl->push_back(prefix_length);
        ps->push_back(strIn[chunk.start_index + chunk.prefix_index]);

        for (size_t j = chunk.start_index; j < stop_index; j++) {
            if (j - chunk.start_index >= chunk.prefix_lengths.size()) {
                throw std::logic_error("prefix_lengths index out of bounds.");
            }
            // Suffix
            sl->push_back(lenIn[j] - chunk.prefix_lengths[j - chunk.start_index]);
            ss->push_back(strIn[j] + chunk.prefix_lengths[j - chunk.start_index]);
            if (config::print_split_points) {
                PrintStringWithSplitPoints(strIn, *sl, *ss, chunk, j, i%2==0);
            }
        }
    }

    return cleaved_result;
}