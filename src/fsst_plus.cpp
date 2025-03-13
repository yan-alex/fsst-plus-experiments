#include "config.h"
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "fsst_plus.h"
#include "cleaving.h"
#include "basic_fsst.h"
#include "duckdb_utils.h"
#include <string>
#include "block_sizer.h"
#include "block_writer.h"

namespace config {
    constexpr size_t total_strings = 1000; // # of input strings
    constexpr size_t compressed_block_byte_capacity = UINT16_MAX; // ~64KB capacity
    constexpr bool print_sorted_corpus = false;
    constexpr bool print_split_points = true; // prints compressed corpus displaying split points
}

void decompress_block(const uint8_t *block_start, const fsst_decoder_t &fsst_decoder_prefix,
                      const fsst_decoder_t &fsst_decoder_suffix) {
    const auto n_strings = Load<uint8_t>(block_start);
    const uint8_t *string_offsets_addr = block_start + sizeof(uint8_t);

    constexpr auto BUFFER_SIZE = 100000;
    auto *result = new unsigned char[BUFFER_SIZE];

    // todo: we can't decompress the last string at the moment
    for (int i = 0; i < (n_strings - 1) * sizeof(uint16_t); i += sizeof(uint16_t)) {
        const auto curr_string_offset = string_offsets_addr + i;
        const auto offset = Load<uint16_t>(curr_string_offset);
        const auto next_offset = Load<uint16_t>(curr_string_offset + sizeof(uint16_t));

        // Add +sizeof(uint16_t) because next offset starts from itself
        const uint16_t full_suffix_length = next_offset + sizeof(uint16_t) - offset;

        const uint8_t *prefix_length_addr = curr_string_offset + offset;
        const auto prefix_length = Load<uint8_t>(prefix_length_addr);

        if (prefix_length == 0) {
            const uint8_t *encoded_suffix_addr = prefix_length_addr + sizeof(uint8_t);
            // suffix only
            const size_t decompressed_suffix_size = fsst_decompress(&fsst_decoder_suffix,
                                                                    full_suffix_length - sizeof(uint8_t),
                                                                    encoded_suffix_addr, BUFFER_SIZE, result);
            std::cout << i / 2 << " decompressed: ";
            for (int j = 0; j < decompressed_suffix_size; j++) {
                std::cout << result[j];
            }
            std::cout << "\n";
        } else {
            const uint8_t *jumpback_offset_addr = prefix_length_addr + sizeof(uint8_t);
            const auto jumpback_offset = Load<uint16_t>(jumpback_offset_addr);

            const uint8_t *encoded_suffix_addr = jumpback_offset_addr + sizeof(uint16_t);

            const uint8_t *encoded_prefix_addr =
                    encoded_suffix_addr - jumpback_offset - sizeof(uint8_t) - sizeof(uint16_t);

            // Step 1) Decompress prefix
            const size_t decompressed_prefix_size = fsst_decompress(&fsst_decoder_prefix, prefix_length,
                                                                    encoded_prefix_addr,
                                                                    BUFFER_SIZE, result);
            std::cout << i / 2 << " decompressed: ";
            for (int j = 0; j < decompressed_prefix_size; j++) {
                std::cout << result[j];
            }

            // Step 2) Decompress suffix
            const size_t decompressed_suffix_size = fsst_decompress(&fsst_decoder_suffix,
                                                                    full_suffix_length - sizeof(uint8_t) - sizeof(
                                                                        uint16_t),
                                                                    encoded_suffix_addr,
                                                                    BUFFER_SIZE, result);
            for (int j = 0; j < decompressed_suffix_size; j++) {
                std::cout << result[j];
            }
            std::cout << "\n";
        }
    }
}

int main() {
    DuckDB db(nullptr);
    Connection con(db);

    const string query =
            "SELECT Url FROM read_parquet('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/clickbenchurl.parquet') LIMIT "
            + std::to_string(config::total_strings) + ";";

    // ======= RUN BASIC FSST TO COMPARE =======
    run_basic_fsst(con, query);

    // Now continue with main's own processing
    const auto result = con.Query(query);
    auto data_chunk = result->Fetch();

    size_t total_strings_amount = {0};
    size_t total_string_size = {0};
    size_t total_compressed_string_size = {0};

    std::cout <<
            "===============================================\n" <<
            "==========START FSST PLUS COMPRESSION==========\n" <<
            "===============================================\n";

    const size_t n = std::min(config::amount_strings_per_symbol_table, config::total_strings);

    std::vector<std::string> original_strings;
    original_strings.reserve(n);

    std::vector<size_t> lenIn;
    lenIn.reserve(n);
    std::vector<const unsigned char *> strIn;
    strIn.reserve(n);

    // Cleaving results will be stored here
    std::vector<size_t> prefixLenIn;
    prefixLenIn.reserve(n);
    std::vector<const unsigned char *> prefixStrIn;
    prefixStrIn.reserve(n);
    std::vector<size_t> suffixLenIn;
    suffixLenIn.reserve(n);
    std::vector<const unsigned char *> suffixStrIn;
    suffixStrIn.reserve(n);

    std::vector<SimilarityChunk> similarity_chunks;
    similarity_chunks.reserve(n);

    std::cout << "🔷 " << n << " strings for this symbol table 🔷 \n";
    while (data_chunk) {
        const size_t data_chunk_size = data_chunk->size();

        std::cout << "> " << data_chunk_size << " strings in DataChunk\n";

        // Populate lenIn and strIn
        extract_strings_from_data_chunk(data_chunk, original_strings, lenIn, strIn);

        data_chunk = result->Fetch();
    }

    // Cleaving runs
    for (size_t i = 0; i < n; i += config::compressed_block_granularity) {
        const size_t cleaving_run_n = std::min(lenIn.size() - i, config::compressed_block_granularity);

        std::cout << "Current Cleaving Run coverage: " << i << ":" << i + cleaving_run_n - 1 << std::endl;

        truncated_sort(lenIn, strIn, i, cleaving_run_n);

        const std::vector<SimilarityChunk> cleaving_run_similarity_chunks = form_similarity_chunks(
            lenIn, strIn, i, cleaving_run_n);
        similarity_chunks.insert(similarity_chunks.end(),
                                 cleaving_run_similarity_chunks.begin(),
                                 cleaving_run_similarity_chunks.end());
    }

    cleave(lenIn, strIn, similarity_chunks, prefixLenIn, prefixStrIn, suffixLenIn, suffixStrIn);

    FSSTPlusCompressionResult compression_result;

    FSSTCompressionResult prefix_compression_result = fsst_compress(prefixLenIn, prefixStrIn);
    // size_t encoded_prefixes_byte_size = calc_encoded_strings_size(prefix_result);
    FSSTCompressionResult suffix_compression_result = fsst_compress(suffixLenIn, suffixStrIn);
    // size_t encoded_suffixes_byte_size = calc_encoded_strings_size(suffix_result);

    // Allocate the maximum size possible for the corpus
    compression_result.data = new uint8_t[calc_max_fsstplus_data_size(prefix_compression_result,
                                                                      suffix_compression_result)];
    // start index for this block into all prefixes (stored in prefix_compression_result)
    size_t prefix_area_start_index = 0;
    // start index for this block into all suffixes (stored in suffix_compression_result)
    size_t suffix_area_start_index = 0;
    
    BlockMetadata b;
    // update metadata with the relevant info
    calculate_block_size(similarity_chunks, prefix_compression_result, suffix_compression_result, b,
                         suffix_area_start_index);

    // use metadata to write correctly
    write_block(compression_result, prefix_compression_result, suffix_compression_result, b, suffix_area_start_index,
                prefix_area_start_index);

    // go on to next block
    prefix_area_start_index += b.prefix_n_in_block;
    suffix_area_start_index += b.suffix_n_in_block;

    // decompress to check all went well
    decompress_block(compression_result.data, fsst_decoder(prefix_compression_result.encoder),
                     fsst_decoder(suffix_compression_result.encoder));

    // compression_result.run_start_offsets = {nullptr}; // Placeholder
    // total_compressed_string_size += compress(prefixLenIn, prefixStrIn, suffixLenIn, suffixStrIn, similarity_chunks, compression_result);

    total_strings_amount += lenIn.size();
    for (size_t string_length: lenIn) {
        total_string_size += string_length;
    }

    print_compression_stats(total_strings_amount, total_string_size, total_compressed_string_size);
    std::cout << "TODO: Save compressed data to the database.\n\n";
    // get the next chunk, continue loop

    // // Cleanup
    // std::cout << "Cleanup";
    // fsst_destroy(encoder);
    return 0;
}
