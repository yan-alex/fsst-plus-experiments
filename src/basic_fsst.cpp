#include "duckdb.hpp"
#include "fsst.h"
#include <iostream>
#include <iomanip>

using namespace duckdb;

fsst_encoder_t* create_encoder(const std::vector<size_t>& lenIn, std::vector<const unsigned char*>& strIn) {
	const int zeroTerminated = 0; // DuckDB strings are not zero-terminated
	fsst_encoder_t* encoder = fsst_create(
		lenIn.size(),        /* IN: number of strings in batch to sample from. */
		lenIn.data(),        /* IN: byte-lengths of the inputs */
		strIn.data(),        /* IN: string start pointers. */
		zeroTerminated       /* IN: whether input strings are zero-terminated. If so, encoded strings are as well (i.e. symbol[0]=""). */
	);
	return encoder;
}

void extract_strings_from_result_chunk(const unique_ptr<DataChunk> &data_chunk, std::vector<std::string> &original_strings, std::vector<size_t> &lenIn, std::vector<const unsigned char *> &strIn) {
	std::cout << data_chunk->size() << " strings will be used this run.\n";
	
	auto &vector = data_chunk->data[0];
	auto vector_data = FlatVector::GetData<string_t>(vector);

	// Populate lenIn and strIn
	for (size_t i = 0; i < data_chunk->size(); i++) {
		if (!vector_data[i].Empty()) {
			std::string str = vector_data[i].GetString();

			original_strings.push_back(str);
			const std::string &stored_str = original_strings.back();
			lenIn.push_back(stored_str.size());
			// strIn.push_back(reinterpret_cast<const unsigned char*>(str.c_str())); // This is not working, it just points to the single str value
			strIn.push_back(reinterpret_cast<const unsigned char*>(stored_str.c_str()));
		}
	}
}


void print_compressed_strings(std::vector<size_t>& lenIn, std::vector<const unsigned char *>& strIn, std::vector<size_t>& lenOut, std::vector<unsigned char *>& strOut, size_t num_compressed) {
	// Print compressed strings
	for (size_t i = 0; i < num_compressed; i++) {
		std::cout << strIn[i]
				<< " was compressed to ";
		for (size_t j = 0; j < lenOut[i]; j++) {
			std::cout << static_cast<int>(strOut[i][j]) << " "; // Print each byte as an integer
		}
		std::cout << "\n";
	}
	size_t total_original = {0};
	size_t total_compressed = {0};
	for (size_t i = 0; i < num_compressed; i++) {
		total_original += lenIn[i];
		total_compressed += lenOut[i];
	}



	std::cout << "✅ ✅ ✅ Compressed " << lenIn.size() << " strings ✅ ✅ ✅\n";
	std::cout << "Original   size: " << total_original << " bytes\n";
	std::cout << "Compressed size: " << total_compressed << " bytes\n";
	std::cout << "Compression ratio: " << total_original/static_cast<double>(total_compressed) << "\n\n";
}


void verify_decompression_correctness(std::vector<std::string>& original_strings, std::vector<size_t>& lenIn, std::vector<size_t>& lenOut, std::vector<unsigned char *>& strOut, size_t num_compressed, fsst_decoder_t& decoder) {
	for (size_t i = 0; i < num_compressed; i++) {
		// Allocate decompression buffer
		auto* decompressed = static_cast<unsigned char*>(malloc(lenIn[i]));

		size_t decompressed_size = fsst_decompress(
			&decoder, /* IN: use this symbol table for compression. */
			lenOut[i],  /* IN: byte-length of compressed string. */
			strOut[i], /* IN: compressed string. */
			lenIn[i], /* IN: byte-length of output buffer. */
			decompressed /* OUT: memory buffer to put the decompressed string in. */
		);
		const std::string_view decompressed_view(reinterpret_cast<char*>(decompressed), decompressed_size);
		// Verify decompression
		if (decompressed_size != lenIn[i] ||
		    decompressed_view != original_strings[i]) {
			std::cerr << "Decompression mismatch for string " << i <<" Expected: "<< original_strings[i] <<" Got: " << decompressed_view <<std::endl;
			throw std::logic_error("Decompression mismatch detected. Terminating.");
		}
	}
	std::cout << "\nDecompression successful\n";

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

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	const auto result = con.Query("SELECT Url FROM read_parquet('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/clickbenchurl.parquet') LIMIT 2048;");
	auto next_chunk = result->Fetch();

	while (next_chunk) {
		std::vector<std::string> original_strings;
		original_strings.reserve(10000);

		std::vector<size_t> lenIn;
		lenIn.reserve(10000);

		std::vector<const unsigned char*> strIn;
		strIn.reserve(10000);
		
		// Populate lenIn and strIn
		extract_strings_from_result_chunk(next_chunk, original_strings, lenIn, strIn);

		// Create FSST encoder
		fsst_encoder_t* encoder = create_encoder(lenIn, strIn);

		// Compression outputs
		std::vector<size_t> lenOut(lenIn.size());
		std::vector<unsigned char*> strOut(lenIn.size());

		// Calculate worst-case output size (2*input)
		size_t max_out_size = 0;
		for (auto len : lenIn) max_out_size += 2 * len;

		// Allocate output buffer
		unsigned char* output = static_cast<unsigned char*>(malloc(max_out_size));


		std::cout << "\n";

		/* =============================================
		 * ================ COMPRESSION ================
		 * ===========================================*/

		// Perform compression
		size_t num_compressed = fsst_compress(
			encoder,				/* IN: encoder obtained from fsst_create(). */
		lenIn.size(),/* IN: number of strings in batch to compress. */
			lenIn.data(),/* IN: byte-lengths of the inputs */
			strIn.data(), /* IN: input string start pointers. */
			max_out_size,/* IN: byte-length of output buffer. */
			output,/* OUT: memory buffer to put the compressed strings in (one after the other). */
			lenOut.data(),/* OUT: byte-lengths of the compressed strings. */
			strOut.data()/* OUT: output string start pointers. Will all point into [output,output+size). */
		);

		print_compressed_strings(lenIn, strIn, lenOut, strOut, num_compressed);

		fsst_decoder_t decoder = fsst_decoder(encoder);

		// print_decoder_symbol_table(decoder);

		/* =============================================
		 * =============== DECOMPRESSION ===============
		 * ===========================================*/


		verify_decompression_correctness(original_strings, lenIn, lenOut, strOut, num_compressed, decoder);
		

		//TODO: Save decoder symbol table somewhere together with compressed data?
		

		// // Cleanup
		std::cout << "Cleanup";
		fsst_destroy(encoder);

		// get the next chunk, start loop again
		next_chunk = result->Fetch();
	}
}
