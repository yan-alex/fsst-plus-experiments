#include "duckdb.hpp"
#include "fsst.h"
#include <iostream>

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	const auto result = con.Query("SELECT \"Case Number\" FROM read_csv('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/Chicago Crimes 2012-2017.csv', strict_mode=False) LIMIT 10;");

	// Store original strings for verification
	std::vector<std::string> original_strings;
	original_strings.reserve(10000);
	// Vectors to store string lengths and pointers
	std::vector<size_t> lenIn;
	lenIn.reserve(10000);
	std::vector<const unsigned char*> strIn;
	strIn.reserve(10000);
	auto next_chunk = result->Fetch();
	while (next_chunk) {
		// to get the number of values within the chunk, we can use the size() method of the chunk
		const auto chunk_size = next_chunk->size();

		auto &vector = next_chunk->data[0];
		auto vector_data = FlatVector::GetData<string_t>(vector);


		// Populate lenIn and strIn
		for (size_t i = 0; i < next_chunk->size(); i++) {
			if (!vector_data[i].Empty()) {
				std::string str = vector_data[i].GetString();

				original_strings.push_back(str);
				const std::string &stored_str = original_strings.back();
				lenIn.push_back(stored_str.size());
				// strIn.push_back(reinterpret_cast<const unsigned char*>(str.c_str())); // This is not working, it just points to the single str value
				//TODO: Fix this. Make it so it's an array of pointers to each string in original_strings
				strIn.push_back(reinterpret_cast<const unsigned char*>(stored_str.c_str()));
			}
		}
		break;
		// get the next chunk, start loop again
		next_chunk = result->Fetch();
	}

	// Create FSST encoder
	const int zeroTerminated = 0; // DuckDB strings are not zero-terminated
	fsst_encoder_t* encoder = fsst_create(
		2000,        /* IN: number of strings in batch to sample from. */
		lenIn.data(),        /* IN: byte-lengths of the inputs */
		strIn.data(),        /* IN: string start pointers. */
		zeroTerminated       /* IN: whether input strings are zero-terminated. If so, encoded strings are as well (i.e. symbol[0]=""). */
	);

	// Prepare compression outputs
	std::vector<size_t> lenOut(lenIn.size());
	std::vector<unsigned char*> strOut(lenIn.size());

	// Calculate worst-case output size (2*input)
	size_t max_out_size = 0;
	for (auto len : lenIn) max_out_size += 2 * len;

	// Allocate output buffer
	unsigned char* output = static_cast<unsigned char*>(malloc(max_out_size));

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

	// Print compressed strings
	for (size_t i = 0; i < num_compressed; i++) {
		std::cout << "Compressed string :" << strIn[i]
				  << " to ";

		for (size_t j = 0; j < lenOut[i]; j++) {
			std::cout << static_cast<int>(strOut[i][j]) << " "; // Print each byte as an integer
		}
		std::cout << "\n";
	}


	// Get decoder for decompression
	fsst_decoder_t decoder = fsst_decoder(encoder);

	// Verify compression correctness
	size_t total_original = 0, total_compressed = 0;
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
			} else {
				std::cout << "Decompression successful for string " << i <<" Expected: "<< original_strings[i] <<" Got: " << decompressed_view <<std::endl;
			}

		total_original += lenIn[i];
		total_compressed += lenOut[i];
	}

	// Print compression stats
	std::cout << "Compressed " << num_compressed << " strings\n";
	std::cout << "Original size: " << total_original << " bytes\n";
	std::cout << "Compressed size: " << total_compressed << " bytes\n";
	std::cout << "Compression ratio: "
			  << (double)total_compressed/total_original * 100
			  << "%\n";

	// Cleanup
	std::cout << "Cleanup";
	fsst_destroy(encoder);
}
