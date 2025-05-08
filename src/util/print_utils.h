//
// Created by Yan Lanna Alexandre on 07/03/2025.
//
#pragma once
#include "cleaving_types.h"
#include <iomanip>
#include <iostream>
#include <vector>
#include <fsst.h>

inline void PrintStringsConst(const std::vector<size_t> &lenIn, const std::vector<const unsigned char *> &strIn) {
    // Print strings
    for (size_t i = 0; i < lenIn.size(); ++i) {
        std::cout << "i " << std::setw(3) << i << " ";
        std::cout << "length " << std::setw(5) << lenIn[i] << " : ";
        for (size_t j = 0; j < lenIn[i]; ++j) {
            std::cout << strIn[i][j];
        }
        std::cout << std::endl;
    }
}

inline void PrintEncodedStrings(const std::vector<size_t> &lenIn, const std::vector<unsigned char *> &strIn) {
    // Print strings
    for (size_t i = 0; i < lenIn.size(); ++i) {
        std::cout << "i " << std::setw(3) << i << ": ";
        for (size_t j = 0; j < lenIn[i]; ++j) {
            std::cout << static_cast<int>(strIn[i][j]) << " ";
        }
        std::cout << std::endl;
    }
}

inline void PrintEncodedStringsConst(const std::vector<size_t> &lenIn, const std::vector<const unsigned char *> &strIn) {
    // Print strings
    for (size_t i = 0; i < lenIn.size(); ++i) {
        std::cout << "i " << std::setw(3) << i << ": ";
        for (size_t j = 0; j < lenIn[i]; ++j) {
            std::cout << static_cast<int>(strIn[i][j]) << " ";
        }
        std::cout << std::endl;
    }
}

inline void PrintStringWithSplitPoints(
    const std::vector<unsigned char *> &strIn,
    const std::vector<size_t> &suffixLenIn,
    const std::vector<const unsigned char *> &suffixStrIn,
    const EnhancedSimilarityChunk &chunk,
    const size_t string_index,
    bool chunk_alternator
) {
    std::string chunk_flag;
    if (chunk_alternator) {
        chunk_flag = "🟥";
    } else {
        chunk_flag = "🟦";
    }
    const size_t prefix_length = chunk.prefix_lengths[string_index - chunk.start_index];
    const size_t suffix_length = suffixLenIn[suffixLenIn.size() - 1];

    std::cout << "string " << std::setw(3) << string_index << ": " << "prefix_length: "<< std::setw(2) << prefix_length <<  " suffix_length: "<< std::setw(2)<<suffix_length << " " << chunk_flag;



    for (size_t j = 0; j < prefix_length; j++) {
        std::cout << static_cast<int>(strIn[chunk.start_index][j]) << " ";
    }

    std::cout << "✳️"; // ✳️ = split point

    for (size_t j = 0; j <suffix_length; j++) {
        std::cout << static_cast<int>(suffixStrIn[suffixLenIn.size() - 1][j]) << " ";
    }
    std::cout << "\n";
}

inline void PrintDecoderSymbolTable(const fsst_decoder_t &decoder) {
    std::cout << "\n==============================================\n";
    std::cout << "\tSTART FSST Decoder Symbol Table:\n";
    std::cout << "==============================================\n";

    // std::cout << "Version: " << decoder.version << "\n";
    // std::cout << "ZeroTerminated: " << static_cast<int>(decoder.zeroTerminated) << "\n";

    for (int code = 0; code < 255; ++code) {
        // Check if the symbol for this code is defined (non-zero length).
        if (decoder.len[code] > 0) {
            std::cout << "Code " << code
                    << " (length " << static_cast<int>(decoder.len[code]) << "): ";
            const unsigned long long sym = decoder.symbol[code];
            // Print each symbol byte as a character (stored in little-endian order)
            for (int i = 0; i < decoder.len[code]; ++i) {
                const unsigned char byte = static_cast<unsigned char>((sym >> (8 * i)) & 0xFF);
                // unsigned char byte = static_cast<unsigned char>(sym);
                std::cout << static_cast<char>(byte);
            }
            std::cout << "\n";
        }
    }
    std::cout << "==============================================\n";
    std::cout << "\tEND FSST Decoder Symbol Table:\n";
    std::cout << "==============================================\n";
}

inline void PrintCompressionStats(
    const size_t total_strings_amount,
    const size_t total_string_size,
    const size_t total_compressed_string_size
) {
    // Print compression stats
    std::cout << "✅ ✅ ✅ Compressed " << total_strings_amount << " strings ✅ ✅ ✅\n";
    std::cout << "Original   size: " << total_string_size << " bytes\n";
    std::cout << "Compressed size: " << total_compressed_string_size << " bytes\n";
    std::cout << "Compression factor: " << static_cast<double>(total_string_size) / total_compressed_string_size <<
            "\n";
}
