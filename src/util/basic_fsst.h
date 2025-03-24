#pragma once
#include <fsst.h>
#include <print_utils.h>
#include <string>

#include "duckdb.hpp"
#include <string>
#include <chrono>
#include "../global.h"
struct FSSTCompressionResult {
    fsst_encoder_t *encoder;
    std::vector<size_t> encoded_strings_length;
    std::vector<unsigned char *> encoded_strings;
    unsigned char *output_buffer;
};

inline fsst_encoder_t *CreateEncoder(const std::vector<size_t> &lenIn, std::vector<const unsigned char *> &strIn) {
    constexpr int zeroTerminated = 0; // DuckDB strings are not zero-terminated
    fsst_encoder_t *encoder = fsst_create(
        lenIn.size(), /* IN: number of strings in batch to sample from. */
        lenIn.data(), /* IN: byte-lengths of the inputs */
        strIn.data(), /* IN: string start pointers. */
        zeroTerminated
        /* IN: whether input strings are zero-terminated. If so, encoded strings are as well (i.e. symbol[0]=""). */
    );
    return encoder;
}

inline size_t CalcSymbolTableSize(fsst_encoder_t *encoder) {
    size_t result = 0;
    // Correctly calculate decoder size by serialization
    unsigned char buffer[FSST_MAXHEADER];
    size_t serialized_size = fsst_export(encoder, buffer);
    result += serialized_size;

    return result;
}

inline void ExtractStringsFromResultChunk(const duckdb::unique_ptr<duckdb::DataChunk> &data_chunk,
                                              std::vector<std::string> &original_strings, std::vector<size_t> &lenIn,
                                              std::vector<const unsigned char *> &strIn) {

    auto &vector = data_chunk->data[0];
    auto vector_data = duckdb::FlatVector::GetData<duckdb::string_t>(vector);

    // Populate lenIn and strIn
    for (size_t i = 0; i < data_chunk->size(); i++) {
        if (!vector_data[i].Empty()) {
            std::string str = vector_data[i].GetString();

            original_strings.push_back(str);
            const std::string &stored_str = original_strings.back();
            lenIn.push_back(stored_str.size());
            // strIn.push_back(reinterpret_cast<const unsigned char*>(str.c_str())); // This is not working, it just points to the single str value
            strIn.push_back(reinterpret_cast<const unsigned char *>(stored_str.c_str()));
        }
    }
};

// Declaration for the function that runs basic FSST compression and prints its results, using the provided DuckDB connection, parquet file path, and limit.
inline void RunBasicFSST(duckdb::Connection &con, const std::string &query) {
    auto start_time = std::chrono::high_resolution_clock::now();
    
    const auto result = con.Query(query);
    auto data_chunk = result->Fetch();

    global::amount_of_rows = result->RowCount();
    
    size_t total_strings_amount = {0};
    size_t total_string_size = {0};
    size_t total_compressed_string_size = {0};

    while (data_chunk) {
        const size_t n = data_chunk->size();

        // std::cout << "🔷 " << n << " strings in DataChunk 🔷\n";

        std::vector<std::string> original_strings;
        original_strings.reserve(n);

        std::vector<size_t> lenIn;
        lenIn.reserve(n);

        std::vector<const unsigned char *> strIn;
        strIn.reserve(n);
        // Populate lenIn and strIn
        ExtractStringsFromResultChunk(data_chunk, original_strings, lenIn, strIn);

        // Create FSST encoder
        fsst_encoder_t *encoder = CreateEncoder(lenIn, strIn);

        // Compression outputs
        std::vector<size_t> lenOut(lenIn.size());
        std::vector<unsigned char *> strOut(lenIn.size());

        // Calculate worst-case output size (2*input)
        size_t max_out_size = 0;
        for (auto len: lenIn) max_out_size += 2 * len;

        // Allocate output buffer
        unsigned char *output = static_cast<unsigned char *>(malloc(max_out_size*2)); //TODO: *2 Shouldnt be needed but segfault otherwise

        /* =============================================
         * ================ COMPRESSION ================
         * ===========================================*/

        // Perform compression
        size_t num_compressed = fsst_compress(
            encoder, /* IN: encoder obtained from fsst_create(). */
            lenIn.size(), /* IN: number of strings in batch to compress. */
            lenIn.data(), /* IN: byte-lengths of the inputs */
            strIn.data(), /* IN: input string start pointers. */
            max_out_size, /* IN: byte-length of output buffer. */
            output, /* OUT: memory buffer to put the compressed strings in (one after the other). */
            lenOut.data(), /* OUT: byte-lengths of the compressed strings. */
            strOut.data() /* OUT: output string start pointers. */
        );

        /* =============================================
         * =============== DECOMPRESSION ===============
         * ===========================================*/

        // fsst_decoder_t decoder = fsst_decoder(encoder);
        // verify_decompression_correctness(original_strings, lenIn, lenOut, strOut, num_compressed, decoder);

        for (size_t compressed_string_length: lenOut) {
            total_compressed_string_size += compressed_string_length;
        }
        total_compressed_string_size += CalcSymbolTableSize(encoder);
        total_strings_amount += lenIn.size();
        for (const size_t string_length: lenIn) {
            total_string_size += string_length;
        }
        
        // Free FSST encoder and output buffer for this batch
        fsst_destroy(encoder);
        free(output);

        // Get the next chunk, continue loop
        data_chunk = result->Fetch();
    }
    
    auto end_time = std::chrono::high_resolution_clock::now();
    global::run_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
    global::compression_factor = static_cast<double>(total_string_size) / total_compressed_string_size;
    
    PrintCompressionStats(total_strings_amount, total_string_size, total_compressed_string_size);
    
    // Store results in the database
    std::string insert_query = "INSERT INTO results VALUES ('" +
                               global::dataset + "', '" +
                               global::column + "', '" +
                               global::algo + "', " +
                               std::to_string(global::amount_of_rows) + ", " +
                               std::to_string(global::run_time_ms) + ", " +
                               std::to_string(global::compression_factor) + ", " +
                               std::to_string(total_strings_amount) + ", " +
                               std::to_string(total_string_size) + ");";
        
    try {
        con.Query(insert_query);
        std::cout << "Inserted result for Basic FSST on " << global::dataset << "." << global::column << std::endl;
    } catch (std::exception& e) {
        std::cerr << "Failed to insert Basic FSST result: " << e.what() << std::endl;
    }
}

inline FSSTCompressionResult FSSTCompress(StringCollection &input) {
    // Create FSST encoder
    fsst_encoder_t *encoder = CreateEncoder(input.lengths, input.string_ptrs);

    // Compression outputs
    std::vector<size_t> lenOut(input.lengths.size());
    std::vector<unsigned char *> strOut(input.string_ptrs.size());

    // Calculate worst-case output size (2 * input)

    // size_t max_out_size = 7; // TODO: SHOULD BE 0 but that gives errors when running w 5 strings for some reason
    // for (auto len: lenIn)
    //     max_out_size += 2 * len;

    size_t max_out_size = 120000*1000;
    // Allocate output buffer
    unsigned char *output = static_cast<unsigned char *>(malloc(max_out_size));

    //////////////// COMPRESSION ////////////////
    fsst_compress(
        encoder, /* IN: encoder obtained from fsst_create(). */
        input.lengths.size(), /* IN: number of strings in batch to compress. */
        input.lengths.data(), /* IN: byte-lengths of the inputs */
        input.string_ptrs.data(), /* IN: input string start pointers. */
        max_out_size, /* IN: byte-length of output buffer. */
        output, /* OUT: memory buffer to put the compressed strings in (one after the other). */
        lenOut.data(), /* OUT: byte-lengths of the compressed strings. */
        strOut.data() /* OUT: output string start pointers. Will all point into [output,output+size). */
    );

    // print_compressed_strings(lenIn, strIn, lenOut, strOut, num_compressed);

    // fsst_decoder_t decoder = fsst_decoder(encoder);
    // print_decoder_symbol_table(decoder);

    return FSSTCompressionResult{encoder, lenOut, strOut, output};
}

inline size_t CalcEncodedStringsSize(const FSSTCompressionResult &compression_result) {
    size_t result = 0;
    const size_t size = compression_result.encoded_strings_length.size();
    for (size_t i = 0; i < size; ++i) {
        result += compression_result.encoded_strings_length[i];
    }
    return result;
}
