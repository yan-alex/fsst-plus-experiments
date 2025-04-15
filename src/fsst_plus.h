#pragma once
#include <block_sizer.h>
#include <vector>
#include "basic_fsst.h"
#include "block_types.h"
#include <cmath>
struct FSSTPlusCompressionResult {
    fsst_encoder_t *prefix_encoder;
    fsst_encoder_t *suffix_encoder;
    uint8_t *data_start;
    uint8_t *data_end;
};

struct FSSTPlusSizingResult {
    std::vector<BlockWritingMetadata> wms;
    std::vector<size_t> block_sizes_pfx_summed;
};

inline size_t CalcMaxFSSTPlusDataSize(const FSSTCompressionResult &prefix_compression_result,
                                              const FSSTCompressionResult &suffix_compression_result) {
    size_t result = {0};

    const size_t ns = suffix_compression_result.encoded_string_ptrs.size(); // number of strings
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

inline StringCollection RetrieveData(const unique_ptr<MaterializedQueryResult> &result, unique_ptr<DataChunk> &data_chunk, const size_t &n) {
    // std::cout << "🔷 " << n << " strings for this symbol table 🔷 \n";

    StringCollection input(n);

    // Use input.data to store actual string contents, ensuring ownership persists
    while (data_chunk) {
        // Populate input.data, input.lengths, and input.strings
        ExtractStringsFromDataChunk(data_chunk, input.data, input.lengths, input.string_ptrs);

        data_chunk = result->Fetch();
    }

    return input;
}

inline FSSTPlusSizingResult SizeEverything(const size_t &n, const std::vector<SimilarityChunk> &similarity_chunks, const FSSTCompressionResult &prefix_compression_result, const FSSTCompressionResult &suffix_compression_result, const size_t &block_granularity) {
    // First calculate total size of all blocks
    std::vector<BlockWritingMetadata> wms;
    std::vector<size_t> block_sizes_pfx_summed;

    size_t suffix_area_start_index = 0; // start index for this block into all suffixes (stored in suffix_compression_result)

    while (suffix_area_start_index < n) {
        // Create fresh metadata for each block
        BlockWritingMetadata wm(block_granularity);  // Instead of reusing previous metadata
        wm.suffix_area_start_index = suffix_area_start_index;

        size_t block_size = CalculateBlockSizeAndPopulateWritingMetadata(
            similarity_chunks, prefix_compression_result, suffix_compression_result, wm,
            suffix_area_start_index, block_granularity);
        size_t prefix_summed = block_sizes_pfx_summed.empty()
                                   ? block_size
                                   : block_sizes_pfx_summed.back() + block_size;
        block_sizes_pfx_summed.push_back(prefix_summed);
        wms.push_back(wm);

        suffix_area_start_index += wm.number_of_suffixes;
    }
    // std::cout << "We have this many blocks: " << wms.size() << "\n";
    return FSSTPlusSizingResult{wms, block_sizes_pfx_summed};
};

inline void RunDictionaryCompression(duckdb::Connection &con, const string &column_name, const string &dataset_path, const size_t &n, const size_t &total_string_size) {
    // Quote the column name to handle spaces and special characters correctly in the SQL query.
    const string quoted_column_name = "\"" + column_name + "\"";
    const string query = "SELECT length(string_agg(DISTINCT " + quoted_column_name + ")) as dict_size, COUNT(DISTINCT " + quoted_column_name + ") as dist, ceil(log2(dist) / 8) as size_of_code, COUNT(" + quoted_column_name + ") * size_of_code as codes_size, CAST(dict_size + codes_size as BIGINT)  as total_compressed_size FROM read_parquet('"+dataset_path+"');";
    const auto result = con.Query(query);

    auto chunk = result->Fetch();
    auto &vector = chunk->data[4];
    const unsigned long *total_compressed_size = FlatVector::GetData<unsigned long>(vector);

    double compression_factor = static_cast<double>(total_string_size) / *total_compressed_size;

    // Store results in the database
    std::string insert_query = "INSERT INTO results VALUES ('" +
                               global::dataset_folders + "', '" +
                               global::dataset + "', '" +
                               global::column + "', '" +
                               global::algo + "', " +
                               std::to_string(global::amount_of_rows) + ", " +
                               std::to_string(0) + ", " +
                               std::to_string(compression_factor) + ", " +
                               std::to_string(n) + ", " +
                               std::to_string(total_string_size) + ");";

    try {
        con.Query(insert_query);
        std::cout << "Inserted result for " << global::dataset << "." << column_name << std::endl;
    } catch (std::exception& e) {
        std::cerr << "🚨 Failed to insert result: " << e.what() << std::endl;
    }

};
