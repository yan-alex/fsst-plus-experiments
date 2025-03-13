#pragma once
#include <array>
#include <vector>
#include "config.h"
#include "basic_fsst.h"

struct FSSTPlusCompressionResult {
    uint8_t *data;
};

inline size_t calc_max_fsstplus_data_size(const FSSTCompressionResult &prefix_compression_result,
                                              const FSSTCompressionResult &suffix_compression_result) {
    size_t result = {0};

    const size_t ns = suffix_compression_result.encoded_strings.size(); // number of strings
    const size_t nb = std::ceil(static_cast<double>(ns) / 128); // number of blocks
    const size_t all_blocks_overhead = nb * (1 + 1 + 128 * 2); // block header3
    result += all_blocks_overhead;
    result += calc_encoded_strings_size(prefix_compression_result);
    result += calc_encoded_strings_size(suffix_compression_result);
    result += ns * 3;
    return result;
}