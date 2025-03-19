#pragma once
#include <vector>
#include "basic_fsst.h"

struct FSSTPlusCompressionResult {
    uint8_t *data;
};

struct GlobalMetadata {

};

inline size_t CalcMaxFSSTPlusDataSize(const FSSTCompressionResult &prefix_compression_result,
                                              const FSSTCompressionResult &suffix_compression_result) {
    size_t result = {0};

    const size_t ns = suffix_compression_result.encoded_strings.size(); // number of strings
    const size_t nb = std::ceil(static_cast<double>(ns) / 128); // number of blocks
    const size_t all_blocks_overhead = nb * (1 + 1 + 128 * 2); // block header3
    result += all_blocks_overhead;
    result += CalcEncodedStringsSize(prefix_compression_result);
    result += CalcEncodedStringsSize(suffix_compression_result);
    result += ns * 3;
    return result;
}