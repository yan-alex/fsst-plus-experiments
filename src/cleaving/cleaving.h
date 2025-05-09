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
            break;
        }
    }
    // Prevents splitting the escape code 255 from its escaped byte.
    if (l!=0 && static_cast<int>(str1[l-1]) == 255) {
        --l;
    }
    return l;
};

inline void compute_all_prefix_lengths(const std::vector<size_t> &lenIn, const std::vector<unsigned char *> &strIn, const size_t &start_index, const size_t &size, std::vector<size_t> &flat_prefix_matrix) {
    flat_prefix_matrix.resize(size * size, 0);

    for (size_t i = 0; i < size; ++i) {
        flat_prefix_matrix[i * size + i] = std::min(lenIn[start_index + i], config::max_prefix_size); // Match with self is the full length
        // Prevents splitting the escape code 255 from its escaped byte.
        if (flat_prefix_matrix[i * size + i] != 0 && static_cast<int>(strIn[start_index + i][flat_prefix_matrix[i * size + i] - 1]) == 255) {
            flat_prefix_matrix[i * size + i]--;
        }

        for (size_t j = i + 1; j < size; ++j) {
            size_t match_length = CalculatePrefixMatch(
                strIn[start_index + i],
                strIn[start_index + j],
                std::min(
                    std::min(lenIn[start_index + i], lenIn[start_index + j]),
                    config::max_prefix_size
                )
            );
            flat_prefix_matrix[i * size + j] = match_length;
            flat_prefix_matrix[j * size + i] = match_length; // Matrix is symmetric
        }
    }
}

inline size_t compute_total_cost(const std::vector<size_t> &length_prefix_sum, const std::vector<size_t> &dp, const size_t i, const size_t j, const int prefix_index_in_chunk, const size_t* prefix_lengths_ptr, const size_t prefix_lengths_size) {
    // Fast path for single-element chunks
    if (prefix_lengths_size == 1) {
        return dp[j] + 1 + (length_prefix_sum[i] - length_prefix_sum[j]);  // Just 1 byte overhead, no compression gain
    }

    size_t overhead = prefix_lengths_size; // One byte per element for length
    size_t compression_gain = 0;
    
    // Direct array access using pointer arithmetic instead of vector indexing
    for (size_t idx = 0; idx < prefix_lengths_size; ++idx) {
        const size_t prefix_length = prefix_lengths_ptr[idx];
        if (prefix_length != 0) {
            overhead += 2;
            compression_gain += prefix_length;
        }
    }
    
    // Subtract the prefix itself from compression gain
    compression_gain -= prefix_lengths_ptr[prefix_index_in_chunk];

    const size_t sum_len = length_prefix_sum[i] - length_prefix_sum[j];
    return dp[j] + overhead + sum_len - compression_gain;
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

    // flat_prefix_matrix stores
    std::vector<size_t> flat_prefix_matrix;
    flat_prefix_matrix.reserve(size * size); // Reserve before compute to avoid resize inside function
    compute_all_prefix_lengths(lenIn, strIn, start_index, size, flat_prefix_matrix);
    
    constexpr size_t INF = std::numeric_limits<size_t>::max();
    std::vector<size_t> dp(size + 1, INF);
    std::vector<size_t> start_index_for_i(size + 1, 0);
    
    // Use a flat array to hold prefix_lengths data instead of vector of vectors
    std::vector<size_t> prefix_lengths_storage;
    prefix_lengths_storage.reserve(size * size / 2); // Rough estimate to avoid reallocations
    
    // Indirection array that points to the start of each prefix_lengths in storage
    std::vector<size_t> prefix_lengths_offsets(size + 1, 0);
    std::vector<size_t> prefix_lengths_sizes(size + 1, 0);
    
    std::vector<size_t> prefix_index_in_chunk_for_i(size + 1, 0);

    dp[0] = 0;

    // Preallocate a single vector for reuse to minimize allocations/deallocations
    std::vector<size_t> prefix_lengths;
    prefix_lengths.reserve(size); // Reserve maximum possible size once
    
    // Second working vector to avoid copies
    std::vector<size_t> best_prefix_lengths;
    best_prefix_lengths.reserve(size);
    
    // Dynamic programming to find the optimal partitioning
    for (size_t i = 1; i <= size; ++i) {
        size_t best_j = 0;
        size_t best_prefix_index = 0;
        size_t best_cost = INF;
        
        for (size_t j = 0; j < i; ++j) {
            const size_t curr_chunk_range = i - j;
            
            for (int prefix_index_in_chunk = 0; prefix_index_in_chunk < curr_chunk_range; ++prefix_index_in_chunk) {
                if (prefix_index_in_chunk > 0) {
                    // Check if this prefix string is identical to any we've already processed
                    unsigned char* current_prefix = strIn[j + start_index + prefix_index_in_chunk];
                    size_t current_prefix_len = lenIn[j + start_index + prefix_index_in_chunk];
                    unsigned char* prev_prefix = strIn[j + start_index + prefix_index_in_chunk-1];
                    size_t prev_prefix_len = lenIn[j + start_index + prefix_index_in_chunk-1];

                    // ✂️ PRUNING ✂️ TODO: Remove? This costs a bit of compression ratio, but speeds up a lot.
                    if (current_prefix_len == prev_prefix_len
                        and
                        *(current_prefix + current_prefix_len) == *(prev_prefix + prev_prefix_len)) {
                        break;
                    }
                }
                
                // Clear without deallocating memory
                prefix_lengths.clear();
                
                if (curr_chunk_range == 1) {
                    prefix_lengths.push_back(0);
                } else {
                    // Resize without initialization - more efficient than resize with default value
                    if (prefix_lengths.capacity() < curr_chunk_range) {
                        prefix_lengths.reserve(curr_chunk_range); // Ensure we have enough capacity
                    }
                    prefix_lengths.resize(curr_chunk_range);
                    
                    // Direct flat matrix access with precomputed indices for better cache locality
                    const size_t base_j = j * size;
                    const size_t prefix_idx_offset = j + prefix_index_in_chunk;
                    
                    // Access flat arrays directly with pointer math for better performance
                    size_t* prefix_ptr = prefix_lengths.data();
                    const size_t* matrix_ptr = flat_prefix_matrix.data();

                    for (size_t k = 0; k < curr_chunk_range; ++k) {
                        const size_t matrix_idx = base_j + k * size + prefix_idx_offset;
                        prefix_ptr[k] = matrix_ptr[matrix_idx];
                    }
                }

                // Pass raw pointer and size to avoid vector indexing overhead
                const size_t total_cost = compute_total_cost(length_prefix_sum, dp, i, j, prefix_index_in_chunk, 
                                                           prefix_lengths.data(), prefix_lengths.size());

                if (total_cost < best_cost) {
                    best_cost = total_cost;
                    best_j = j;
                    best_prefix_index = prefix_index_in_chunk;
                    
                    // Swap instead of copying - much more efficient
                    best_prefix_lengths.swap(prefix_lengths);
                }
            }
        }
        
        // Only update dp and related arrays if we found a valid solution
        if (best_cost < INF) {
            dp[i] = best_cost;
            start_index_for_i[i] = best_j;
            
            // Store best_prefix_lengths in our flat storage
            prefix_lengths_offsets[i] = prefix_lengths_storage.size();
            prefix_lengths_sizes[i] = best_prefix_lengths.size();
            
            // Append the best prefix lengths to our flat storage
            prefix_lengths_storage.insert(
                prefix_lengths_storage.end(), 
                best_prefix_lengths.begin(), 
                best_prefix_lengths.end()
            );
            
            prefix_index_in_chunk_for_i[i] = best_prefix_index;
            
            // Clear best_prefix_lengths without releasing memory
            best_prefix_lengths.clear();
        }
    }

    // Release memory we don't need anymore
    flat_prefix_matrix.clear();
    flat_prefix_matrix.shrink_to_fit();
    prefix_lengths.clear();
    prefix_lengths.shrink_to_fit();
    best_prefix_lengths.clear();
    best_prefix_lengths.shrink_to_fit();
    dp.clear();
    dp.shrink_to_fit();
    start_index_for_i.shrink_to_fit(); // Don't clear as we need the data

    // Reconstruct the chunks and their prefix lengths
    std::vector<EnhancedSimilarityChunk> chunks;
    chunks.reserve(size); // Reserve in case all elements are in their own chunk
    
    size_t idx = size;
    while (idx > 0) {
        const size_t start_idx = start_index_for_i[idx];
        
        EnhancedSimilarityChunk chunk;
        chunk.start_index = start_index + start_idx;
        chunk.prefix_index = prefix_index_in_chunk_for_i[idx];
        
        // Copy the prefix lengths from our flat storage
        const size_t offset = prefix_lengths_offsets[idx];
        const size_t pl_size = prefix_lengths_sizes[idx];
        
        if (pl_size > 0) {
            chunk.prefix_lengths.assign(
                prefix_lengths_storage.begin() + offset,
                prefix_lengths_storage.begin() + offset + pl_size
            );
        }
        
        chunks.push_back(std::move(chunk));
        idx = start_idx;
    }
    
    // Release remaining memory
    prefix_lengths_storage.clear();
    prefix_lengths_storage.shrink_to_fit();
    prefix_lengths_offsets.clear();
    prefix_lengths_offsets.shrink_to_fit();
    prefix_lengths_sizes.clear();
    prefix_lengths_sizes.shrink_to_fit();
    
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
    
    // Reserve space to avoid reallocations
    const size_t total_chunks = enhanced_similarity_chunks.size();
    const size_t total_strings = lenIn.size();
    pl->reserve(total_chunks);
    ps->reserve(total_chunks);
    sl->reserve(total_strings);
    ss->reserve(total_strings);
    
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