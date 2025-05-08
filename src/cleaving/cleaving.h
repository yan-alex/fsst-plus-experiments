//
// Created by Yan Lanna Alexandre on 04/03/2025.
//
#pragma once
#include <ranges>
#include "print_utils.h"
#include "../config.h" // Not needed but prevents ClionIDE from complaining
#include <algorithm>
#include <limits>

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

inline std::vector<EnhancedSimilarityChunk> FormEnhancedSimilarityChunks(
    const std::vector<size_t> &lenIn,
    const std::vector<unsigned char *> &strIn,
    const size_t start_index,
    const size_t size) {
    if (size == 0) return {}; // No strings to process

    std::vector<size_t> lcp(size - 1); // LCP between consecutive strings
    std::vector<std::vector<size_t> > min_lcp(size, std::vector<size_t>(size));

    // Precompute LCPs up to config::max_prefix_size characters
    for (size_t i = 0; i < size - 1; ++i) {
        const size_t max_lcp = std::min(std::min(lenIn[start_index + i], lenIn[start_index + i + 1]), config::max_prefix_size);
        size_t l = 0;
        const unsigned char *s1 = strIn[start_index + i];
        const unsigned char *s2 = strIn[start_index + i + 1];
        while (l < max_lcp && s1[l] == s2[l]) {
            ++l;
        }
        // Prevents splitting the escape code 255 from its escaped byte.
        if (l!=0 && static_cast<int>(s1[l-1]) == 255) {
            --l;
        }
        lcp[i] = l;
    }
    // Precompute min_lcp[i][j]
    for (size_t i = 0; i < size; ++i) {
        min_lcp[i][i] = std::min(lenIn[start_index + i], config::max_prefix_size);
        for (size_t j = i + 1; j < size; ++j) {
            min_lcp[i][j] = std::min(min_lcp[i][j - 1], lcp[j - 1]);
        }
    }

    // Precompute prefix sums of string lengths (cumulatively adding the length of each element)
    std::vector<size_t> length_prefix_sum(size + 1, 0);
    for (size_t i = 0; i < size; ++i) {
        length_prefix_sum[i + 1] = length_prefix_sum[i] + lenIn[start_index + i];
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
            const size_t min_common_prefix = min_lcp[j][i - 1]; // can be max 128 a.k.a. config::max_prefix_size
            for (int prefix_index_in_chunk = 0; prefix_index_in_chunk < i-j; ++prefix_index_in_chunk) {
                // printf("Dynamic Programming i:%lu j:%lu prefix_index_in_chunk:%d \n", i, j, prefix_index_in_chunk);
                const size_t n = i - j;
                const std::vector<size_t>prefix_lengths = CalcLengthsForChunk(prefix_index_in_chunk, strIn, lenIn, j + start_index, n);
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
                // const size_t total_cost = dp[j] + overhead + sum_len - (n - 1) * p; // (n - 1) * p is the compression gain. n are strings in current range, p is the common prefix length in this range

                const size_t total_cost = dp[j] + overhead + sum_len - compression_gain;


                if (total_cost < dp[i]) {
                    dp[i] = total_cost;
                    start_index_for_i[i] = j;
                    prefix_lengths_for_i[i] = prefix_lengths;
                    prefix_index_in_chunk_for_i[i] = prefix_index_in_chunk;
                }
                // if (p < min_common_prefix and p + 8 > min_common_prefix) {
                //     p = min_common_prefix;
                // }else {
                //     p += 8;
                // }
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