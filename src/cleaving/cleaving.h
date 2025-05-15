//
// Created by Yan Lanna Alexandre on 04/03/2025.
//
#pragma once
#include <ranges>
#include "print_utils.h"
#include "../config.h" // Not needed but prevents ClionIDE from complaining
#include <algorithm>
#include <limits>
#include <unordered_map>
#include <functional>


inline void InplaceHashGroupingSort(std::vector<unsigned char *> &strIn, const size_t start_index, const size_t &cleaving_run_n, size_t indices[], size_t truncated_lengths[]) {
    constexpr uint64_t prime_number = 1000000007;
    
    // Recursive helper function
    std::function<void(size_t*, size_t, size_t, size_t)> sortBucket = [&](size_t* bucket_indices, size_t bucket_size, size_t offset, size_t max_length) {
        // Base cases: single item bucket or we've processed all bytes
        if (bucket_size <= 1 || offset >= max_length) {
            return;
        }
        
        // Group strings by their hash at current offset
        std::unordered_map<uint64_t, std::vector<size_t>> ht;
        
        // Find max length in this bucket for termination condition
        size_t bucket_max_length = 0;
        
        for (size_t i = 0; i < bucket_size; i++) {
            size_t idx = bucket_indices[i];
            bucket_max_length = std::max(bucket_max_length, truncated_lengths[idx - start_index]);
            
            uint64_t hash_key = 0;
            if (offset < truncated_lengths[idx - start_index]) {
                // If at least 8 bytes remain, use fast path
                if (offset + 8 <= truncated_lengths[idx - start_index]) {
                    hash_key = *reinterpret_cast<const uint64_t*>(strIn[idx] + offset);
                }
                hash_key = hash_key * prime_number;
            }
            
            ht[hash_key].push_back(idx);
        }
        
        // Reorder indices and recursively process sub-buckets
        size_t pos = 0;
        for (auto& [hash_key, group] : ht) {
            // Copy group back to the bucket
            for (size_t idx : group) {
                bucket_indices[pos++] = idx;
            }
            
            // Recursively sort this group if it has multiple items
            if (group.size() > 1) {
                sortBucket(bucket_indices + (pos - group.size()), group.size(), offset + 8, bucket_max_length);
            }
        }
    };
    
    // Find maximum string length
    size_t max_length = 0;
    for (size_t i = 0; i < cleaving_run_n; i++) {
        max_length = std::max(max_length, truncated_lengths[i]);
    }
    
    // Start recursive sorting
    sortBucket(indices, cleaving_run_n, 0, max_length);
}

// Sort all strings based on their starting characters truncated (up to config::max_prefix_size bytes)

inline void TruncatedSort(std::vector<size_t> &lenIn, std::vector<unsigned char *> &strIn,
                           const size_t start_index, const size_t &cleaving_run_n, StringCollection &input) {
    // Create index array
    size_t indices[cleaving_run_n];
    for (size_t i = 0; i < cleaving_run_n; ++i) {
        indices[i] = start_index + i;
    }

    // For each string, calculate its truncated length once
    size_t truncated_lengths[cleaving_run_n];
    for (size_t i = 0; i < cleaving_run_n; ++i) {
        const size_t idx = start_index + i;
        // truncated_lengths[i] = std::min(lenIn[idx], config::max_prefix_size);

        // HERE WE CAN MAKE THE TRADEOFF OF SPEED vs. GOOD SORTING
        truncated_lengths[i] = std::min(lenIn[idx], static_cast<size_t>(8));
    }

    // // std::sort uses IntroSort (hybrid of quicksort, heapsort, and insertion sort )
    // std::sort(indices, indices + cleaving_run_n, [&](size_t a, size_t b) {
    //     const size_t a_rel = a - start_index;
    //     const size_t b_rel = b - start_index;
    //
    //     const size_t common_len = std::min(truncated_lengths[a_rel], truncated_lengths[b_rel]);
    //     for (size_t i = 0; i < common_len; ++i) {
    //         if (strIn[a][i] != strIn[b][i]) {
    //             return strIn[a][i] < strIn[b][i];
    //         }
    //     }
    //
    //     // If all bytes match up to the shorter length, longer string comes first
    //     return truncated_lengths[a_rel] > truncated_lengths[b_rel];
    // });

    InplaceHashGroupingSort(strIn, start_index, cleaving_run_n, indices, truncated_lengths);

    // Create temporary storage arrays instead of vectors
    size_t tmp_len[cleaving_run_n];
    unsigned char* tmp_str[cleaving_run_n];
    size_t tmp_input_lengths[cleaving_run_n];
    const unsigned char* tmp_input_string_ptrs[cleaving_run_n];
    
    // Copy only the relevant slice we need to reorder
    for (size_t i = 0; i < cleaving_run_n; ++i) {
        size_t idx = start_index + i;
        tmp_len[i] = lenIn[idx];
        tmp_str[i] = strIn[idx];
        tmp_input_lengths[i] = input.lengths[idx];
        tmp_input_string_ptrs[i] = input.string_ptrs[idx];
    }

    // Reorder using the sorted indices
    for (size_t k = 0; k < cleaving_run_n; ++k) {
        size_t src_idx = indices[k] - start_index;
        size_t dst_idx = start_index + k;
        
        lenIn[dst_idx] = tmp_len[src_idx];
        strIn[dst_idx] = tmp_str[src_idx];
        input.lengths[dst_idx] = tmp_input_lengths[src_idx];
        input.string_ptrs[dst_idx] = tmp_input_string_ptrs[src_idx];
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