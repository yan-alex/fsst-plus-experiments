//
// Created by Yan Lanna Alexandre on 04/03/2025.
//
#pragma once
#include <ranges>
#include "print_utils.h"
#include "../config.h" // Not needed but prevents ClionIDE from complaining
#include <algorithm>
#include <limits>

// Sort all strings based on their starting characters truncated to the largest multiple of 8 bytes (up to config::max_prefix_size bytes)
inline void TruncatedSort(std::vector<size_t> &lenIn, std::vector<unsigned char *> &strIn,
                           const size_t start_index, const size_t cleaving_run_n, StringCollection &input) {
    // Create index array
    std::vector<size_t> indices(cleaving_run_n);
    for (size_t i = start_index; i < start_index + cleaving_run_n; ++i) {
        indices[i - start_index] = i;
    }

    // For each string, calculate its truncated length once
    std::vector<size_t> truncated_lengths(cleaving_run_n);
    for (size_t i = 0; i < cleaving_run_n; ++i) {
        size_t idx = start_index + i;
        truncated_lengths[i] = std::min(lenIn[idx], config::max_prefix_size);
    }

    // Find the maximum truncated length to determine how many passes we need
    size_t max_truncated_len = 0;
    for (size_t len : truncated_lengths) {
        max_truncated_len = std::max(max_truncated_len, len);
    }

    // Buckets for radix sort (256 possible values for each byte)
    std::vector<std::vector<size_t>> buckets(256);
    // Use current_indices to track the current ordering
    std::vector<size_t> current_indices = indices;
    std::vector<size_t> next_indices(cleaving_run_n);

    // Start from the least significant byte within the truncated region and work backwards
    // This is an LSD (Least Significant Digit) radix sort which is stable
    for (int byte_pos = max_truncated_len - 1; byte_pos >= 0; --byte_pos) {
        // Clear all buckets
        for (auto& bucket : buckets) {
            bucket.clear();
        }
        
        // Distribute indices into buckets based on the byte at byte_pos
        for (size_t i = 0; i < cleaving_run_n; ++i) {
            size_t idx = current_indices[i];
            size_t rel_idx = idx - start_index;
            
            // If this string's truncated length includes this byte position
            if (byte_pos < truncated_lengths[rel_idx]) {
                unsigned char byte_val = strIn[idx][byte_pos];
                buckets[byte_val].push_back(idx);
            } else {
                // String is too short for this position, put it in bucket 0
                buckets[0].push_back(idx);
            }
        }
        
        // Collect indices from buckets back into next_indices
        size_t pos = 0;
        for (size_t i = 0; i < 256; ++i) {
            for (size_t idx : buckets[i]) {
                next_indices[pos++] = idx;
            }
        }
        
        // Update current_indices for the next pass
        std::swap(current_indices, next_indices);
    }
    
    // Reorder using the sorted indices
    const std::vector<size_t> tmp_len(lenIn);
    const std::vector<unsigned char *> tmp_str(strIn);
    
    // Create temporary copies of the original input vectors
    const std::vector<size_t> tmp_input_lengths(input.lengths);
    const std::vector<const unsigned char *> tmp_input_string_ptrs(input.string_ptrs);
    
    for (size_t k = 0; k < cleaving_run_n; ++k) {
        lenIn[start_index + k] = tmp_len[current_indices[k]];
        strIn[start_index + k] = tmp_str[current_indices[k]];
        
        // ALSO REORDER THE ORIGINAL UNCOMPRESSED INPUT
        // we do this to be able to pairwise compare later on when we verify decompression, to see if it matches with the original
        input.lengths[start_index + k] = tmp_input_lengths[current_indices[k]];
        input.string_ptrs[start_index + k] = tmp_input_string_ptrs[current_indices[k]];
    }
    
    // Print strings
    if (config::print_sorted_corpus) {
        std::cout << "Sorted strings: \n";
        PrintEncodedStrings(lenIn, strIn);
    }
}

inline std::vector<SimilarityChunk> FormSimilarityChunks(
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
    std::vector<size_t> prev(size + 1, 0);
    std::vector<size_t> p_for_i(size + 1, 0);

    dp[0] = 0;

    // Dynamic programming to find the optimal partitioning
    for (size_t i = 1; i <= size; ++i) {
        for (size_t j = 0; j < i; ++j) {
            const size_t min_common_prefix = min_lcp[j][i - 1]; // can be max 128 a.k.a. config::max_prefix_size
            size_t p = 0;
            while (p <= min_common_prefix) {
                const size_t n = i - j;
                const size_t per_string_overhead = 1 + (p > 0 ? 2 : 0); // 1 because u will always exist, 2 for pointer
                const size_t overhead = n * per_string_overhead;
                const size_t sum_len = length_prefix_sum[i] - length_prefix_sum[j];
                const size_t total_cost = dp[j] + overhead + sum_len - (n - 1) * p;
                // (n - 1) * p is the compression gain. n are strings in current range, p is the common prefix length in this range

                if (total_cost < dp[i]) {
                    dp[i] = total_cost;
                    prev[i] = j;
                    p_for_i[i] = p;
                }
                if (p < min_common_prefix and p + 8 > min_common_prefix) {
                    p = min_common_prefix;
                }else {
                    p += 8;
                }
            }
        }
    }

    // Reconstruct the chunks and their prefix lengths
    std::vector<SimilarityChunk> chunks;
    size_t idx = size;
    while (idx > 0) {
        const size_t start_idx = prev[idx];
        const size_t prefix_length = p_for_i[idx];
        SimilarityChunk chunk;
        chunk.start_index = start_index + start_idx;
        chunk.prefix_length = prefix_length;
        chunks.push_back(chunk);
        idx = start_idx;
    }
    // The chunks are reversed, so we need to reverse them back
    std::reverse(chunks.begin(), chunks.end());

    return chunks;
}

inline CleavedResult Cleave(const std::vector<size_t> &lenIn,
                            const std::vector<unsigned char *> &strIn,
                            const std::vector<SimilarityChunk> &similarity_chunks,
                            size_t n
) {
    auto cleaved_result = CleavedResult(n);

    std::vector<size_t> *pl = &cleaved_result.prefixes.lengths;
    std::vector<const unsigned char *> *ps = &cleaved_result.prefixes.string_ptrs;
    std::vector<size_t> *sl = &cleaved_result.suffixes.lengths;
    std::vector<const unsigned char *> *ss = &cleaved_result.suffixes.string_ptrs;

    std::vector<float> splitpoint_coverages;

    for (size_t i = 0; i < similarity_chunks.size(); i++) {
        const SimilarityChunk &chunk = similarity_chunks[i];
        const size_t stop_index = i == similarity_chunks.size() - 1
                                      ? lenIn.size()
                                      : similarity_chunks[i + 1].start_index;

        // Prefix
        pl->push_back(chunk.prefix_length);
        ps->push_back(strIn[chunk.start_index]);
        for (size_t j = chunk.start_index; j < stop_index; j++) {
            // Suffix
            sl->push_back(lenIn[j] - chunk.prefix_length);
            ss->push_back(strIn[j] + chunk.prefix_length);
            if (config::print_split_points) {
                PrintStringWithSplitPoints(strIn, *sl, *ss, chunk, j);
                if (lenIn[j] > 0) {
                    splitpoint_coverages.push_back(chunk.prefix_length/static_cast<float>(lenIn[j]));
                }
            }
        }
    }

    if (config::print_split_points) {
        // get mean of splitpoint_coverages with cpp 11
        float sum = 0.0f;
        for (const auto& coverage : splitpoint_coverages) {
            sum += coverage;
        }
        float mean = splitpoint_coverages.empty() ? 0.0f : sum / splitpoint_coverages.size();
        std::cout << "> 👨‍💻 Mean splitpoint coverage for " << n << " strings: " << mean << std::endl;
    }

    return cleaved_result;
}