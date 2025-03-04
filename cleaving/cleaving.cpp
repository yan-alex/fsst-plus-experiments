#include "cleaving.h"
#include <ranges>

namespace config {
    constexpr size_t cleaving_run_n = 128; // number of elements per cleaving run
    constexpr size_t max_prefix_size = 120; // how far into the string to scan for a prefix. (max prefix size)
}

std::vector<SimilarityChunk> form_similarity_chunks(
    std::vector<size_t>& lenIn,
    std::vector<const unsigned char*>& strIn,
    const size_t start_index)
{
    size_t N = std::min(lenIn.size() - start_index, config::cleaving_run_n);

    if (N == 0) return {}; // No strings to process

    std::vector<size_t> lcp(N - 1); // LCP between consecutive strings
    std::vector<std::vector<size_t>> min_lcp(N, std::vector<size_t>(N));

    // Precompute LCPs up to config::max_prefix_size characters
    for (size_t i = 0; i < N - 1; ++i) {
        size_t max_lcp = std::min({lenIn[start_index + i], lenIn[start_index + i + 1], config::max_prefix_size});
        size_t l = 0;
        const unsigned char* s1 = strIn[start_index + i];
        const unsigned char* s2 = strIn[start_index + i + 1];
        while (l < max_lcp && s1[l] == s2[l]) {
            ++l;
        }
        lcp[i] = l;
    }

    // Precompute min_lcp[i][j]
    for (size_t i = 0; i < N; ++i) {
        min_lcp[i][i] = std::min(lenIn[start_index + i], config::max_prefix_size);
        for (size_t j = i + 1; j < N; ++j) {
            min_lcp[i][j] = std::min(min_lcp[i][j - 1], lcp[j - 1]);
        }
    }

    // Precompute prefix sums of string lengths (cumulatively adding the lenght of each element)
    std::vector<size_t> length_prefix_sum(N + 1, 0);
    for (size_t i = 0; i < N; ++i) {
        length_prefix_sum[i + 1] = length_prefix_sum[i] + lenIn[start_index + i];
    }

    const size_t INF = std::numeric_limits<size_t>::max();
    std::vector<size_t> dp(N + 1, INF);
    std::vector<size_t> prev(N + 1, 0);
    std::vector<size_t> p_for_i(N + 1, 0);

    dp[0] = 0;

    // Dynamic programming to find the optimal partitioning
    for (size_t i = 1; i <= N; ++i) {
        for (size_t j = 0; j < i; ++j) {
            size_t min_common_prefix = min_lcp[j][i - 1]; // can be max 128 a.k.a. config::max_prefix_size
            for (size_t p = 0; p <= min_common_prefix; p += 8) {

                size_t n = i - j;
                size_t per_string_overhead = 1 + (p > 0 ? 2 : 0);// 1 because u will always exist, 2 for pointer
                size_t overhead = n * per_string_overhead;
                size_t sum_len = length_prefix_sum[i] - length_prefix_sum[j];
                size_t total_cost = dp[j] + overhead + sum_len - (n - 1) * p;  // (n - 1) * p is the compression gain. n are strings in current range, p is the common prefix length in this range

                if (total_cost < dp[i]) {
                    dp[i] = total_cost;
                    prev[i] = j;
                    p_for_i[i] = p;
                }
            }
        }
    }

    // Reconstruct the chunks and their prefix lengths
    std::vector<SimilarityChunk> chunks;
    size_t idx = N;
    while (idx > 0) {
        size_t start_idx = prev[idx];
        size_t prefix_length = p_for_i[idx];
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