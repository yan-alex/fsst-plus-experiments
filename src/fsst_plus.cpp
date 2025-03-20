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
#include "block_decompressor.h"
#include "cleaving_types.h"

namespace config {
    constexpr size_t total_strings = 100000; // # of input strings
    constexpr size_t block_byte_capacity = UINT16_MAX; // ~64KB capacity
    constexpr bool print_sorted_corpus = false;
    constexpr bool print_split_points = false; // prints compressed corpus displaying split points
    constexpr bool print_decompressed_corpus = false;
}


uint8_t *find_block_start(uint8_t *block_start_offsets, const int i) {
    uint8_t *offset_ptr = block_start_offsets + (i * sizeof(uint32_t));
    const uint32_t offset = Load<uint32_t>(offset_ptr);
    uint8_t *block_start =  offset_ptr + offset;
    return block_start;
}

void decompress_all(uint8_t *global_header, const fsst_decoder_t &prefix_decoder,
const fsst_decoder_t &suffix_decoder,
std::vector<size_t> &lenIn,
std::vector<const unsigned char *> &strIn) {
    global::global_index = 0;
    uint16_t num_blocks = Load<uint16_t>(global_header);
    uint8_t *block_start_offsets = global_header + sizeof(uint16_t);
    for (int i = 0; i < num_blocks; ++i) {
        const uint8_t *block_start = find_block_start(block_start_offsets, i);
        /*
         * Block stop is next block's start. Note that this also works for the last block, so no over-read,
         * as we save an "extra" data_end_offset, pointing to where the last block stops. This is needed to
         * calculate the length
         */
        const uint8_t *block_stop = find_block_start(block_start_offsets, i + 1);

        DecompressBlock(block_start, prefix_decoder, suffix_decoder, block_stop, lenIn, strIn);
    }
}

StringCollection RetrieveData(const unique_ptr<MaterializedQueryResult> &result, unique_ptr<DataChunk> &data_chunk, const size_t &n) {
    std::cout << "🔷 " << n << " strings for this symbol table 🔷 \n";

    auto input = StringCollection(n);

    std::vector<std::string> original_strings;
    original_strings.reserve(n);

    while (data_chunk) {
        // Populate lenIn and strIn
        ExtractStringsFromDataChunk(data_chunk, original_strings, input.lengths, input.strings);

        data_chunk = result->Fetch();
    }

    return input;
}


int main() {
    DuckDB db(nullptr);
    Connection con(db);

    // // Create the benchchmark_results table
    // const std::string create_benchchmark_results_query =
    //     "CREATE TABLE IF NOT EXISTS benchchmark_results ("
    //     "id INTEGER PRIMARY KEY, "
    //     "dataset VARCHAR, "
    //     "column VARCHAR, "
    //     "algo VARCHAR, "
    //     "amount_of_rows DOUBLE, "
    //     "run_time_ms DOUBLE, "
    //     "compression_factor VARCHAR, "
    //     ");";
    // auto benchchmark_results_create_result = con.Query(create_benchchmark_results_query);
    // if (!benchchmark_results_create_result->success) {
    //     std::cerr << "Failed to create benchchmark_results table: " << benchchmark_results_create_result->error << std::endl;
    //     return 1;
    // }

    // Create the benchchmark_results table
    const string query =
            "SELECT Url FROM read_parquet('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/data/refined/clickbench.parquet')"
            "LIMIT " + std::to_string(config::total_strings) +
             ";";

    // ======= RUN BASIC FSST TO COMPARE =======
    RunBasicFSST(con, query);

    // Now continue with main's own processing
    const auto result = con.Query(query);
    auto data_chunk = result->Fetch();


    std::cout <<
            "===============================================\n" <<
            "==========START FSST PLUS COMPRESSION==========\n" <<
            "===============================================\n";

    const size_t n = std::min(config::amount_strings_per_symbol_table, static_cast<size_t>(result->RowCount()));


    StringCollection input = RetrieveData(result, data_chunk, n);

    std::vector<SimilarityChunk> similarity_chunks;
    similarity_chunks.reserve(n);

    // Figure out the optimal split points (similarity chunks)
    for (size_t i = 0; i < n; i += config::block_granularity) {
        const size_t cleaving_run_n = std::min(input.lengths.size() - i, config::block_granularity);

        std::cout << "Current Cleaving Run coverage: " << i << ":" << i + cleaving_run_n - 1 << std::endl;

        TruncatedSort(input.lengths, input.strings, i, cleaving_run_n);

        const std::vector<SimilarityChunk> cleaving_run_similarity_chunks = FormSimilarityChunks(
            input.lengths, input.strings, i, cleaving_run_n);
        similarity_chunks.insert(similarity_chunks.end(),
                                 cleaving_run_similarity_chunks.begin(),
                                 cleaving_run_similarity_chunks.end());
    }


    // Cleave based on split points
    CleavedResult cleaved_result = Cleave(input.lengths, input.strings, similarity_chunks, n);

    FSSTPlusCompressionResult compression_result;

    FSSTCompressionResult prefix_compression_result = FSSTCompress(cleaved_result.prefixes);
    FSSTCompressionResult suffix_compression_result = FSSTCompress(cleaved_result.suffixes);

    // Allocate the maximum size possible for the corpus
    compression_result.data = new uint8_t[CalcMaxFSSTPlusDataSize(prefix_compression_result,
                                                                      suffix_compression_result)*4]; //TODO: why *4 needed? function should work without it

    /*
     *  >>> WRITE GLOBAL HEADER <<<
     *
     * To write num_blocks, we must know how many blocks we have. But first let's
     * calculate the size of each block (giving us also the number of blocks),
     * allowing us to write block_start_offsets[] and data_end_offset also.
     */

    // First calculate total size of all blocks
    std::vector<BlockWritingMetadata> wms;
    std::vector<size_t> prefix_sum_block_sizes;

    size_t prefix_area_start_index = 0; // start index for this block into all prefixes (stored in prefix_compression_result)
    size_t suffix_area_start_index = 0; // start index for this block into all suffixes (stored in suffix_compression_result)

    while (suffix_area_start_index < n) {
        // Create fresh metadata for each block
        BlockWritingMetadata wm;  // Instead of reusing previous metadata
        wm.prefix_area_start_index = prefix_area_start_index;
        wm.suffix_area_start_index = suffix_area_start_index;

        size_t block_size = CalculateBlockSizeAndPopulateWritingMetadata(
            similarity_chunks, prefix_compression_result, suffix_compression_result, wm,
            suffix_area_start_index);
        size_t prefix_summed = prefix_sum_block_sizes.empty()
                                        ? block_size
                                        : prefix_sum_block_sizes.back() + block_size;
        prefix_sum_block_sizes.push_back(prefix_summed);
        wms.push_back(wm);

        // go on to next block
        prefix_area_start_index += wm.prefix_n_in_block;
        suffix_area_start_index += wm.suffix_n_in_block;
    }
    uint8_t* global_header_ptr = compression_result.data;

    // Now we can write!

    // A) write num_blocks
    size_t n_blocks = prefix_sum_block_sizes.size();
    Store<uint16_t>(n_blocks ,global_header_ptr);
    global_header_ptr+=sizeof(uint16_t);

    // B) write block_start_offsets[]
    for (size_t i = 0; i < n_blocks; i++) {
        size_t offsets_to_go = (n_blocks - i); // count itself, so that the "base" begins at the offset's end
        size_t global_header_size_ahead =
                offsets_to_go * sizeof(uint32_t)
                + sizeof(uint32_t); // data_end_offset size
        size_t total_block_size_ahead =  i == 0 ? 0 : prefix_sum_block_sizes[i-1];

        Store<uint32_t>(global_header_size_ahead + total_block_size_ahead, global_header_ptr);
        global_header_ptr +=sizeof(uint32_t);
    }

    // C) write data_end_offset
    Store<uint32_t>(prefix_sum_block_sizes.back() + sizeof(uint32_t) // count itself, so that the "base" begins at the offset's end
,global_header_ptr);
    global_header_ptr +=sizeof(uint32_t);

    uint8_t* next_block_start_ptr = global_header_ptr;

    //  >>> WRITE BLOCKS <<<
    for (BlockWritingMetadata wm: wms) {
        // use metadata to write correctly
        next_block_start_ptr = WriteBlock(next_block_start_ptr, prefix_compression_result, suffix_compression_result, wm);
    }

    // decompress to check all went well
    decompress_all(compression_result.data, fsst_decoder(prefix_compression_result.encoder),
                         fsst_decoder(suffix_compression_result.encoder), input.lengths, input.strings);


    PrintCompressionStats(n, CalculateInputSize(input), next_block_start_ptr - compression_result.data );
    std::cout << "TODO: Save compressed data to the database.\n\n";

    std::cout << "Cleanup\n";
    fsst_destroy(prefix_compression_result.encoder);
    fsst_destroy(suffix_compression_result.encoder);
    return 0;
}
