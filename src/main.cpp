#include "duckdb.hpp"
#include "fsst.h"
#include <iostream>
#include <iomanip>
#include <ranges>
#include "main.h"
#include "cleaving.h"
#include "cleaving.cpp"

#include <sys/stat.h>


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

void extract_strings_from_data_chunk(const unique_ptr<DataChunk>& data_chunk, std::vector<std::string>& original_strings, std::vector<size_t>& lenIn, std::vector<const unsigned char *>& strIn) {

	auto &vector = data_chunk->data[0];
	auto vector_data = FlatVector::GetData<string_t>(vector);

	// Populate lenIn and strIn
	for (size_t i = 0; i < data_chunk->size(); i++) {
		// if (!vector_data[i].Empty()) {
			std::string str = vector_data[i].GetString();

			original_strings.push_back(str);
			const std::string &stored_str = original_strings.back(); // Creates a reference to the string that was just added to the vector.
			lenIn.push_back(stored_str.size()); 
			strIn.push_back(reinterpret_cast<const unsigned char*>(stored_str.c_str())); // c_str() returns a pointer to the internal character array, which is a temporary array owned by the string object.
		// }
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


	// Print compression stats
	std::cout << "✅ ✅ ✅ Compressed " << num_compressed << " strings ✅ ✅ ✅\n";
	std::cout << "Original   size: " << total_original << " bytes\n";
	std::cout << "Compressed size: " << total_compressed << " bytes\n";
	std::cout << "Compression ratio: " << (double)total_compressed/total_original * 100 << "%\n\n";
}

void verify_decompression_correctness(std::vector<std::string>& original_strings, std::vector<size_t>& lenIn, std::vector<size_t>& lenOut, std::vector<unsigned char *>& strOut, size_t& num_compressed, fsst_decoder_t& decoder) {
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

void print_decoder_symbol_table(fsst_decoder_t& decoder) {
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

CompressionResult* compress(std::vector<size_t>& lenIn, std::vector<const unsigned char *>& strIn, bool isPrefix, std::vector<SimilarityChunk> similarity_chunks) {
	// Create FSST encoder
	fsst_encoder_t *encoder = create_encoder(lenIn, strIn);

	// Compression outputs
	std::vector<size_t> lenOut(lenIn.size());
	std::vector<unsigned char *> strOut(lenIn.size());

	// Calculate worst-case output size (2 * input)
	size_t max_out_size = 0;
	for (auto len : lenIn)
		max_out_size += 2 * len;

	// Allocate output buffer
	unsigned char *output = static_cast<unsigned char *>(malloc(max_out_size));

	//////////////// COMPRESSION ////////////////
	size_t iter = fsst_compress(
		encoder,       /* IN: encoder obtained from fsst_create(). */
		lenIn.size(),  /* IN: number of strings in batch to compress. */
		lenIn.data(),  /* IN: byte-lengths of the inputs */
		strIn.data(),  /* IN: input string start pointers. */
		max_out_size,  /* IN: byte-length of output buffer. */
		output,        /* OUT: memory buffer to put the compressed strings in (one after the other). */
		lenOut.data(), /* OUT: byte-lengths of the compressed strings. */
		strOut.data()  /* OUT: output string start pointers. Will all point into [output,output+size). */
	);

	// print_compressed_strings(lenIn, strIn, lenOut, strOut, num_compressed);

	// fsst_decoder_t decoder = fsst_decoder(encoder);
	// print_decoder_symbol_table(decoder);

	if (isPrefix) {
		auto* result = new PrefixCompressionResult();
		result->encoded_prefixes = strOut;
		result->encoder = encoder;
		return result;
	} else {
		std::vector<SuffixData> data;
		for (size_t i = 0; i < similarity_chunks.size(); i++) {
			const SimilarityChunk& chunk = similarity_chunks[i];

			size_t stop_index = i == similarity_chunks.size() - 1 ? lenIn.size() : similarity_chunks[i+1].start_index;

			for (size_t j = chunk.start_index; j < stop_index; ++j) {
				if (chunk.prefix_length == 0) {
					data.push_back(SuffixData{static_cast<unsigned char>(0), strOut[j]});
				} else {
					//TODO: Is static_cast<unsigned char>(chunk.prefix_length) ok here? Can it overflow the char?
					data.push_back(
						SuffixDataWithPrefix{
							static_cast<unsigned char>(chunk.prefix_length),
							static_cast<unsigned short>(1),//TODO: 1 is a placeholder. Change to real jump-back offset
							strOut[j]
						}
					);
				}
			}
		}
		auto* result = new SuffixCompressionResult();
		result->data = data;
		result->encoder = encoder;
		return result;
	}
}

void print_strings(std::vector<size_t> &lenIn, std::vector<const unsigned char *> &strIn) {
	// Print strings
	for (size_t i = 0; i < lenIn.size(); ++i) {
		std::cout<<"i " << std::setw(3) << i << ": ";
		for (size_t j = 0; j < lenIn[i]; ++j) {
			std::cout << strIn[i][j];
		}
		std::cout << std::endl;
	}
}
// Sort all strings based on their starting characters truncated to the largest multiple of 8 bytes (up to config::max_prefix_size bytes)
void truncated_sort(std::vector<size_t>& lenIn,  std::vector<const unsigned char*>& strIn, const size_t start_index) {
	// std::cout << "Before sorting:\n";
	// print_strings(lenIn, strIn);
	const size_t n = std::min(lenIn.size()-1 - start_index, config::cleaving_run_n);

	// Create index array
	std::vector<size_t> indices(n);
	for (size_t i = start_index; i < start_index+n; ++i) {
		indices[i-start_index] = i;
	}

	// Sort indices based on truncated string comparison
	std::sort(indices.begin(), indices.end(), [&](size_t i, size_t j) {
		// Calculate truncated lengths as largest multiple of 8 <= min(config::max_prefix_size, original length)
		size_t len_i = std::min(lenIn[i], config::max_prefix_size) & ~7;
		size_t len_j = std::min(lenIn[j], config::max_prefix_size) & ~7;

		// Compare truncated strings
		int cmp = memcmp(strIn[i], strIn[j], std::min(len_i, len_j));
		return cmp < 0 || (cmp == 0 && len_i < len_j);
	});


	// Reorder both vectors based on sorted indices
	std::vector<size_t> tmp_len(lenIn);
	std::vector<const unsigned char*> tmp_str(strIn);

	for (size_t k = 0; k < n; ++k) {
		lenIn[start_index + k] = tmp_len[indices[k]];
		strIn[start_index + k] = tmp_str[indices[k]];
	}


	// std::cout << "\n\nAfter sorting:\n";
	// print_strings(lenIn, strIn);
}


/*
Segment the sorted data into similarity chunks and identify an optimal split point for each
chunk—separating a common prefix from the variable suffixes.
– Note: A heuristic or simple cost model (possibly using metadata captured during the
sorting process) will be investigated to determine reliable boundaries for similarity chunks
and the corresponding split points.

*/

size_t calc_match_len(std::vector<const unsigned char *> &strIn, std::vector<size_t>& lenIn, size_t i) {
	size_t min_len = std::min(lenIn[i-1], lenIn[i]);
	size_t match_len = 0;
	while (
		match_len + 8 <= min_len && // out of bounds check
		std::memcmp(strIn[i-1] + match_len, strIn[i] + match_len, 8) == 0 // next 8 bytes match
		) {
		match_len += 8;
	}
	return match_len;
}


void cleave(std::vector<size_t> &lenIn,
            std::vector<const unsigned char *> &strIn,
            const std::vector<SimilarityChunk> &similarity_chunks,
            std::vector<size_t> &prefixLenIn,
			std::vector<const unsigned char*> &prefixStrIn,
			std::vector<size_t> &suffixLenIn,
			std::vector<const unsigned char*> &suffixStrIn
) {
	for (size_t i = 0; i < similarity_chunks.size(); i++) {
		const SimilarityChunk& chunk = similarity_chunks[i];
		size_t stop_index = i == similarity_chunks.size() - 1 ? lenIn.size() : similarity_chunks[i+1].start_index;

		// Prefix
		prefixLenIn.push_back(chunk.prefix_length);
		prefixStrIn.push_back(strIn[chunk.start_index]);

		for (size_t j = chunk.start_index; j < stop_index; j++) {
			// Suffix
			suffixLenIn.push_back(lenIn[j] - chunk.prefix_length);
			suffixStrIn.push_back(strIn[j] + chunk.prefix_length);
		}
	}
}

size_t calc_compressed_prefix_size(const PrefixCompressionResult* prefix_sompression_result) {
	size_t result = 0;
	size_t size = prefix_sompression_result->encoded_prefixes.size();
	unsigned char * first_pointer = prefix_sompression_result->encoded_prefixes[0];
	unsigned char * last_pointer = prefix_sompression_result->encoded_prefixes[size - 1];
	result += last_pointer - first_pointer;

    // Correctly calculate decoder size by serialization
    unsigned char buffer[FSST_MAXHEADER];
    size_t serialized_size = fsst_export(prefix_sompression_result->encoder, buffer);
    result += serialized_size;
	
	return result;
}
size_t calc_compressed_suffix_size(const SuffixCompressionResult* suffix_compression_result) {
	size_t result = 0;
	size_t size = suffix_compression_result->data.size();
	for (size_t i = 0; i < size; i++) {
		SuffixData data = suffix_compression_result->data[i];
		if (data.prefix_length != 0 ){
			// data = static_cast<SuffixDataWithPrefix>(data);
			// TODO: Add size to result correctly? how?

		}
	};


    // Correctly calculate decoder size by serialization
    unsigned char buffer[FSST_MAXHEADER];
    size_t serialized_size = fsst_export(suffix_compression_result->encoder, buffer);
    result += serialized_size;

	return result;
}

int main() {
	DuckDB db(nullptr);
	Connection con(db);

	const auto result = con.Query("SELECT Url FROM read_parquet('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/clickbenchurl.parquet') LIMIT 2000;");
	auto data_chunk = result->Fetch();

	while (data_chunk) {
		const size_t n = data_chunk->size();
		std::cout << "\n 🔷 🔷 🔷 " << n << " strings in DataChunk 🔷 🔷 🔷 \n\n";

		std::vector<std::string> original_strings;
		original_strings.reserve(n);

		std::vector<size_t> lenIn;
		lenIn.reserve(n);

		std::vector<const unsigned char*> strIn;
		strIn.reserve(n);

		// Populate lenIn and strIn
		extract_strings_from_data_chunk(data_chunk, original_strings, lenIn, strIn);
		// print_strings(lenIn, strIn);


		// Cleaving results will be stored here
		std::vector<size_t> prefixLenIn;
		prefixLenIn.reserve(n);
		std::vector<const unsigned char*> prefixStrIn;
		prefixStrIn.reserve(n);
		std::vector<size_t> suffixLenIn;
		suffixLenIn.reserve(n);
		std::vector<const unsigned char*> suffixStrIn;
		suffixStrIn.reserve(n);

		std::vector<SimilarityChunk> similarity_chunks;
		similarity_chunks.reserve(n);

		// Cleaving runs
		for (size_t i = 0; i < n; i+=config::cleaving_run_n) {
			std::cout << "Current Cleaving Run coverage: " << i <<":"<< i+config::cleaving_run_n-1 << std::endl;

			truncated_sort(lenIn, strIn, i);

			// print_strings(lenIn, strIn);
			const std::vector<SimilarityChunk> cleaving_run_similarity_chunks = form_similarity_chunks(lenIn, strIn, i);
			similarity_chunks.insert(similarity_chunks.end(),
									cleaving_run_similarity_chunks.begin(),
									cleaving_run_similarity_chunks.end());
			
		}
		cleave(lenIn, strIn, similarity_chunks, prefixLenIn, prefixStrIn, suffixLenIn, suffixStrIn);


        PrefixCompressionResult* prefixCompressionResult = static_cast<PrefixCompressionResult*>(compress(prefixLenIn, prefixStrIn, true, similarity_chunks));

        SuffixCompressionResult* suffixCompressionResult = static_cast<SuffixCompressionResult*>(compress(suffixLenIn, suffixStrIn, false, similarity_chunks));

		// size_t compressed_prefix_size = calc_compressed_prefix_size(prefixCompressionResult);
		// size_t compressed_suffix_size = sizeof(*suffixCompressionResult);
		// size_t total_compressed = compressed_prefix_size + compressed_suffix_size;
		//
		// size_t total_original = {0};
		// for (size_t i = 0; i < lenIn.size(); i++) {
		// 	total_original += lenIn[i];
		// }
		//
		// std::cout << "✅ ✅ ✅ Compressed " << lenIn.size() << " strings ✅ ✅ ✅\n";
		// std::cout << "Original   size: " << total_original << " bytes\n";
		// std::cout << "Compressed size: " << total_compressed << " bytes\n";
		// std::cout << "Compression ratio: " << total_original/static_cast<double>(total_compressed) << "\n\n";

		// Must be deallocated!
		delete prefixCompressionResult;
		delete suffixCompressionResult;


		// // // Cleanup
		// std::cout << "Cleanup";
		// fsst_destroy(encoder);

		// get the next chunk, start loop again
		data_chunk = result->Fetch();
	}
	return 0;
}
