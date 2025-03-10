//
// Created by Yan Lanna Alexandre on 07/03/2025.
//
#include <fsst.h>
#include "cleaving_types.h"
#include <iomanip>
#include <iostream>
#include <vector>

#ifndef UTILS_H
#define UTILS_H

inline void print_strings(const std::vector<size_t> &lenIn, std::vector<const unsigned char *> &strIn) {
    // Print strings
    for (size_t i = 0; i < lenIn.size(); ++i) {
        std::cout << "i " << std::setw(3) << i << ": ";
        for (size_t j = 0; j < lenIn[i]; ++j) {
            std::cout << strIn[i][j];
        }
        std::cout << std::endl;
    }
};

inline void print_string_with_split_points(std::vector<const unsigned char *> &strIn,
                               std::vector<size_t> &suffixLenIn,
                               std::vector<const unsigned char *> &suffixStrIn,
                               const SimilarityChunk &chunk,
                               size_t string_index) {
    std::cout << "string" << string_index << ": ";
    for (size_t j = 0; j < chunk.prefix_length; j++) {
        std::cout << strIn[chunk.start_index][j];
    }
    std::cout << "✳️"; // ✳️ = split point
    for (size_t j = 0; j < suffixLenIn[suffixLenIn.size()-1]; j++) {
        std::cout << suffixStrIn[suffixStrIn.size()-1][j];
    }
    std::cout << std::endl;
}

void print_decoder_symbol_table(fsst_decoder_t &decoder) {
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
            unsigned long long sym = decoder.symbol[code];
            // Print each symbol byte as a character (stored in little-endian order)
            for (int i = 0; i < decoder.len[code]; ++i) {
                unsigned char byte = static_cast<unsigned char>((sym >> (8 * i)) & 0xFF);
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

inline void print_compression_stats(size_t total_strings_amount, size_t total_string_size, size_t total_compressed_string_size) {
	// Print compression stats
	std::cout << "✅ ✅ ✅ Compressed " << total_strings_amount << " strings ✅ ✅ ✅\n";
	std::cout << "Original   size: " << total_string_size << " bytes\n";
	std::cout << "Compressed size: " << total_compressed_string_size << " bytes\n";
	std::cout << "Compression factor: " << (double)(total_string_size) / total_compressed_string_size << "\n\n";
}
#endif // UTILS_H
