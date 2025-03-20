#pragma once
#include <vector>
#include "basic_fsst.h"
#include <cmath>
struct FSSTPlusCompressionResult {
    fsst_encoder_t *prefix_encoder;
    fsst_encoder_t *suffix_encoder;
    uint8_t *data_start;
    uint8_t *data_end;
};


inline size_t CalcMaxFSSTPlusDataSize(const FSSTCompressionResult &prefix_compression_result,
                                              const FSSTCompressionResult &suffix_compression_result) {
    size_t result = {0};

    const size_t ns = suffix_compression_result.encoded_strings.size(); // number of strings
    const size_t nb = ceil(static_cast<double>(ns) / 128); // number of blocks
    const size_t all_blocks_overhead = nb * (1 + 1 + 128 * 2); // block header3
    result += all_blocks_overhead;
    result += CalcEncodedStringsSize(prefix_compression_result);
    result += CalcEncodedStringsSize(suffix_compression_result);
    result += ns * 3;
    // Add extra safety padding to avoid potential buffer overflows
    result += (nb * 1024); // 1KB extra per blockfor safety TODO: No real reason this should be done but was failing without
    return result;
}