#pragma once
#include <fsst.h>
#include <print_utils.h>

#include "duckdb.hpp"
#include <string>
#include <__fwd/string.h>

struct FSSTCompressionResult {
    fsst_encoder_t *encoder;
    std::vector<size_t> encoded_strings_length;
    std::vector<unsigned char *> encoded_strings;
};

inline fsst_encoder_t *create_encoder(const std::vector<size_t> &lenIn, std::vector<const unsigned char *> &strIn) {
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

inline size_t calc_symbol_table_size(fsst_encoder_t *encoder) {
    size_t result = 0;
    // Correctly calculate decoder size by serialization
    unsigned char buffer[FSST_MAXHEADER];
    size_t serialized_size = fsst_export(encoder, buffer);
    result += serialized_size;

    return result;
}

inline void verify_decompression_correctness(const std::vector<std::string> &original_strings, const std::vector<size_t> &lenIn,
                                             const std::vector<size_t> &lenOut, const std::vector<unsigned char *> &strOut,
                                             const size_t num_compressed, const fsst_decoder_t &decoder) {
    for (size_t i = 0; i < num_compressed; i++) {
        // Allocate decompression buffer
        auto *decompressed = static_cast<unsigned char *>(malloc(lenIn[i]));

        const size_t decompressed_size = fsst_decompress(
            &decoder, /* IN: use this symbol table for compression. */
            lenOut[i], /* IN: byte-length of compressed string. */
            strOut[i], /* IN: compressed string. */
            lenIn[i], /* IN: byte-length of output buffer. */
            decompressed /* OUT: memory buffer to put the decompressed string in. */
        );
        const std::string_view decompressed_view(reinterpret_cast<char *>(decompressed), decompressed_size);
        // Verify decompression
        if (decompressed_size != lenIn[i] ||
            decompressed_view != original_strings[i]) {
            std::cerr << "Decompression mismatch for string " << i << " Expected: " << original_strings[i] << " Got: "
                    << decompressed_view << std::endl;
            throw std::logic_error("Decompression mismatch detected. Terminating.");
        }
    }
    std::cout << "\nDecompression successful\n";
}

inline void extract_strings_from_result_chunk(const duckdb::unique_ptr<duckdb::DataChunk> &data_chunk,
                                              std::vector<std::string> &original_strings, std::vector<size_t> &lenIn,
                                              std::vector<const unsigned char *> &strIn) {
    std::cout << " Compression run coverage: 0:" << data_chunk->size() - 1;

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
inline void run_basic_fsst(duckdb::Connection &con, const std::string &query) {
    const auto result = con.Query(query);
    auto data_chunk = result->Fetch();

    size_t total_strings_amount = {0};
    size_t total_string_size = {0};
    size_t total_compressed_string_size = {0};
    std::cout <<
            "===============================================\n" <<
            "==========START BASIC FSST COMPRESSION=========\n" <<
            "===============================================\n";
    while (data_chunk) {
        const size_t n = data_chunk->size();
        std::cout << "🔷 " << n << " strings in DataChunk 🔷\n";

        std::vector<std::string> original_strings;
        original_strings.reserve(n);

        std::vector<size_t> lenIn;
        lenIn.reserve(n);

        std::vector<const unsigned char *> strIn;
        strIn.reserve(n);
        // Populate lenIn and strIn
        extract_strings_from_result_chunk(data_chunk, original_strings, lenIn, strIn);

        // Create FSST encoder
        fsst_encoder_t *encoder = create_encoder(lenIn, strIn);

        // Compression outputs
        std::vector<size_t> lenOut(lenIn.size());
        std::vector<unsigned char *> strOut(lenIn.size());

        // Calculate worst-case output size (2*input)
        size_t max_out_size = 0;
        for (auto len: lenIn) max_out_size += 2 * len;

        // Allocate output buffer
        unsigned char *output = static_cast<unsigned char *>(malloc(max_out_size));

        std::cout << "\n";

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
        total_compressed_string_size += calc_symbol_table_size(encoder);
        total_strings_amount += lenIn.size();
        for (const size_t string_length: lenIn) {
            total_string_size += string_length;
        }


        // Get the next chunk, continue loop
        data_chunk = result->Fetch();
    }

    print_compression_stats(total_strings_amount, total_string_size, total_compressed_string_size);

    // // Cleanup
    // std::cout << "Cleanup\n";
    // fsst_destroy(encoder);
}

inline FSSTCompressionResult fsst_compress(const std::vector<size_t> &lenIn, std::vector<const unsigned char *> &strIn) {
    // Create FSST encoder
    fsst_encoder_t *encoder = create_encoder(lenIn, strIn);

    // Compression outputs
    std::vector<size_t> lenOut(lenIn.size());
    std::vector<unsigned char *> strOut(lenIn.size());

    // Calculate worst-case output size (2 * input)
    size_t max_out_size = 0;
    for (auto len: lenIn)
        max_out_size += 2 * len;

    // Allocate output buffer
    unsigned char *output = static_cast<unsigned char *>(malloc(max_out_size));

    //////////////// COMPRESSION ////////////////
    fsst_compress(
        encoder, /* IN: encoder obtained from fsst_create(). */
        lenIn.size(), /* IN: number of strings in batch to compress. */
        lenIn.data(), /* IN: byte-lengths of the inputs */
        strIn.data(), /* IN: input string start pointers. */
        max_out_size, /* IN: byte-length of output buffer. */
        output, /* OUT: memory buffer to put the compressed strings in (one after the other). */
        lenOut.data(), /* OUT: byte-lengths of the compressed strings. */
        strOut.data() /* OUT: output string start pointers. Will all point into [output,output+size). */
    );

    // print_compressed_strings(lenIn, strIn, lenOut, strOut, num_compressed);

    // fsst_decoder_t decoder = fsst_decoder(encoder);
    // print_decoder_symbol_table(decoder);

    return FSSTCompressionResult{encoder, lenOut, strOut};
}

inline void verify_decompression_correctness(const std::vector<std::string> &original_strings, const std::vector<size_t> &lenIn,
                                             const std::vector<size_t> &lenOut, const std::vector<unsigned char *> &strOut,
                                             const size_t &num_compressed, const fsst_decoder_t &decoder) {
    for (size_t i = 0; i < num_compressed; i++) {
        // Allocate decompression buffer
        auto *decompressed = static_cast<unsigned char *>(malloc(lenIn[i]));

        size_t decompressed_size = fsst_decompress(
            &decoder, /* IN: use this symbol table for compression. */
            lenOut[i], /* IN: byte-length of compressed string. */
            strOut[i], /* IN: compressed string. */
            lenIn[i], /* IN: byte-length of output buffer. */
            decompressed /* OUT: memory buffer to put the decompressed string in. */
        );
        const std::string_view decompressed_view(reinterpret_cast<char *>(decompressed), decompressed_size);
        // Verify decompression
        if (decompressed_size != lenIn[i] ||
            decompressed_view != original_strings[i]) {
            std::cerr << "Decompression mismatch for string " << i << " Expected: " << original_strings[i] << " Got: "
                    << decompressed_view << std::endl;
            throw std::logic_error("Decompression mismatch detected. Terminating.");
        }
    }
    std::cout << "\nDecompression successful\n";
}
inline size_t calc_encoded_strings_size(const FSSTCompressionResult &compression_result) {
    size_t result = 0;
    const size_t size = compression_result.encoded_strings_length.size();
    for (size_t i = 0; i < size; ++i) {
        result += compression_result.encoded_strings_length[i];
    }
    return result;
}
