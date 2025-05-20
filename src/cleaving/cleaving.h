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
#include <vector>

inline std::vector<SimilarityChunk> updateChunksOptimally(std::vector<SimilarityChunk> &old_chunks, std::vector<SimilarityChunk> &new_chunks) {
    // Simple, fast greedy algorithm: concatenate new_chunks to old_chunks.
    // This assumes that chunks are generated sequentially and new_chunks logically follow old_chunks.
    std::vector<SimilarityChunk> result;
    result.reserve(old_chunks.size() + new_chunks.size());

    result.insert(result.end(), old_chunks.begin(), old_chunks.end());
    result.insert(result.end(), new_chunks.begin(), new_chunks.end());

    return result;
};

inline std::vector<SimilarityChunk> sortBucket(uint8_t* local_indices, size_t start_pos, const size_t start_index, const uint8_t number_of_elements, const size_t horizontal_offset, const size_t max_length, unsigned char* tmp_str[128], size_t truncated_lengths[], uint8_t* ht, uint8_t* index_store_flat, uint8_t* index_store_sizes) {
    printf("⚡️🪣 sortBucket() start at: %d, %d elements, horizontal_offset: %lu\n", local_indices[0], number_of_elements, horizontal_offset);
    for (int i = 0; i < number_of_elements; ++i) {
        printf("local index %hhu\n", local_indices[i]);
    }
    for (int i = 0; i < number_of_elements; ++i) {
        std::cout << "string i " << std::setw(3) << i << ": ";
        const uint8_t actual_index = local_indices[i];
        for (int j = 0; j < truncated_lengths[actual_index]; ++j) {
            std::cout << static_cast<int>((tmp_str[actual_index])[j]) << " ";
        }
        std::cout << "\n";
    }
    constexpr uint64_t prime_number = 0xc6a4a7935bd1e995U;
    // Base cases: single item bucket or we've processed all bytes
    if (number_of_elements <= 1 || horizontal_offset >= max_length) {
        std::vector<SimilarityChunk> chunks;

        SimilarityChunk chunk;
        chunk.start_index = start_pos;
        chunk.prefix_length = std::min(horizontal_offset, truncated_lengths[local_indices[0]]);

        chunks.push_back(chunk);
        return chunks;
    }
    // printf("☣️RESETTING ALL ARRAYS ☣️");
    // Initialize hash table
    for (int i = 0; i < 65536; ++i) {
        ht[i] = 255;
    }

    /*
     * current bucket pointer.
     * this works functionally the same as the hashkey, but we use it to access the 2d matrix
     */
    // Reset index store sizes and flat array
    for (int i = 0; i < 128; ++i) {
        index_store_sizes[i] = 0;
    }
    for (int i = 0; i < 128 * 128; ++i) {
        index_store_flat[i] = 0;
    }

    uint8_t unique_values_seen = 0;

    // Find max length in this bucket for termination condition
    size_t bucket_max_length = 0;

    for (size_t i = 0; i < number_of_elements; i++) {
        // printf("hashing loop i:%lu up to %d\n", i, number_of_elements);

        const uint8_t local_i = local_indices[i];
        bucket_max_length = std::max(bucket_max_length, truncated_lengths[local_i]);

        uint64_t hash = 0;
        if (horizontal_offset < truncated_lengths[local_i]) {
            // If at least 8 bytes remain, use fast 64-bit read
            if (horizontal_offset + 8 <= truncated_lengths[local_i]) {
                hash = *reinterpret_cast<const uint64_t*>(tmp_str[local_i] + horizontal_offset);
                // Better bit mixing with single operation
                hash = (hash * prime_number) ^ (hash >> 37);
            } else {
                // Handle smaller chunks with a single read
                memcpy(&hash, tmp_str[local_i] + horizontal_offset, truncated_lengths[local_i] - horizontal_offset);
                hash = (hash * prime_number) ^ (hash >> 37);
            }
            hash = hash & 0xFFFF;
        }
        const uint16_t hash_key = static_cast<uint16_t>(hash);
        // printf("ht[%d] = %d\n", hash_key, ht[hash_key]);
        if (ht[hash_key] == 255) {
            ht[hash_key] = unique_values_seen;
            // printf("set ht[%d] to %d\n", hash_key, unique_values_seen);
            unique_values_seen ++;
        }
        index_store_flat[ht[hash_key] * 128 + index_store_sizes[ht[hash_key]]] = local_i;
        index_store_sizes[ht[hash_key]]++;
        // printf("index_store_sizes[%d]++, is now %d\n", ht[hash_key], index_store_sizes[ht[hash_key]]);
    }

    std::vector<SimilarityChunk> chunks;

    for (int i = 0; i < unique_values_seen; ++i) {
        // printf("retrieving  index_store_sizes[%d]... value is: %d\n", i, index_store_sizes[i]);

        const uint8_t group_size = index_store_sizes[i];
        for (int j = 0; j < group_size; ++j) {
            local_indices[start_pos + j] = index_store_flat[i * 128 + j];
        }
        // if (group_size > 1 && horizontal_offset + 8 < bucket_max_length  ) {
        uint8_t* ht2 = new uint8_t[65536];
        uint8_t* index_store_flat2 = new uint8_t[128 * 128];
        uint8_t* index_store_sizes2 = new uint8_t[128];

        std::vector<SimilarityChunk> bucket_chunks = sortBucket(local_indices + start_pos, start_pos, start_index, group_size, horizontal_offset + 8, bucket_max_length, tmp_str, truncated_lengths, ht2, index_store_flat2, index_store_sizes2);

        delete[] ht2;
        delete[] index_store_flat2;
		delete[] index_store_sizes2;
        // for (SimilarityChunk bucket_chunk : bucket_chunks) {
        //     chunks.push_back(bucket_chunk);
        // }
        chunks = updateChunksOptimally(chunks, bucket_chunks);

        // }
        start_pos += group_size;
    }
  
    return chunks;
}

// Sort all strings based on their starting characters truncated (up to config::max_prefix_size bytes)
inline std::vector<SimilarityChunk> TruncatedSort(std::vector<size_t> &lenIn, std::vector<unsigned char *> &strIn,
                                                  const size_t start_index, const size_t &cleaving_run_n,
                                                  StringCollection &input) {



    // Create temporary storage arrays instead of vectors
    size_t tmp_len[128];
    unsigned char* tmp_str[128];
    size_t tmp_input_lengths[128];
    const unsigned char* tmp_input_string_ptrs[128];

    uint8_t indices[128];
    size_t truncated_lengths[128];

    // Allocate arrays for sortBucket
    uint8_t* ht = new uint8_t[65536];
    uint8_t* index_store_flat = new uint8_t[128 * 128];
    uint8_t* index_store_sizes = new uint8_t[128];

    size_t max_length = 0;

    for (size_t i = 0; i < cleaving_run_n; ++i) {
        indices[i] = i;

        const size_t idx = start_index + i;
        // HERE WE CAN MAKE THE TRADEOFF OF SPEED vs. GOOD SORTING
        truncated_lengths[i] = std::min(lenIn[idx], static_cast<size_t>(16));

        max_length = std::max(max_length, truncated_lengths[i]);

        tmp_len[i] = lenIn[idx];
        tmp_str[i] = strIn[idx];
        tmp_input_lengths[i] = input.lengths[idx];
        tmp_input_string_ptrs[i] = input.string_ptrs[idx];
    }

    // // std::sort uses IntroSort (hybrid of quicksort, heapsort, and insertion sort )
    // std::sort(indices, indices + cleaving_run_n, [&](uint8_t a, uint8_t b) {
    //     const size_t a_corpus = a + start_index;
    //     const size_t b_corpus = b + start_index;
    //
    //     const size_t common_len = std::min(truncated_lengths[a], truncated_lengths[b]);
    //     for (size_t i = 0; i < common_len; ++i) {
    //         if (strIn[a_corpus][i] != strIn[b_corpus][i]) {
    //             return strIn[a_corpus][i] < strIn[b_corpus][i];
    //         }
    //     }
    //     // If all bytes match up to the shorter length, longer string comes first
    //     return truncated_lengths[a] > truncated_lengths[b];
    // });

    const std::vector<SimilarityChunk> similarity_chunks = sortBucket(indices, 0, start_index, cleaving_run_n, 0, max_length, tmp_str, truncated_lengths, ht, index_store_flat, index_store_sizes);

    // Reorder using the sorted indices
    for (size_t i = 0; i < cleaving_run_n; ++i) {
        const size_t src_idx = indices[i];
        const size_t dst_idx = start_index + i;
        
        lenIn[dst_idx] = tmp_len[src_idx];
        strIn[dst_idx] = tmp_str[src_idx];
        input.lengths[dst_idx] = tmp_input_lengths[src_idx];
        input.string_ptrs[dst_idx] = tmp_input_string_ptrs[src_idx];
    }

    // Deallocate arrays
    delete[] ht;
    delete[] index_store_flat;
    delete[] index_store_sizes;
    
    // Print strings
    if (config::print_sorted_corpus) {
        std::cout << "Sorted strings: \n";
        PrintEncodedStrings(lenIn, strIn);
    }
    return similarity_chunks;
}

// SOON TO BE DEPRECATED
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
                PrintStringWithSplitPoints(strIn, *sl, *ss, chunk, j, i%2==0);
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