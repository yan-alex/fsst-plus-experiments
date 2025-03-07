#include "duckdb.hpp"
#include "fsst.h"
#include <iostream>
#include <iomanip>
#include <ranges>
#include "main.h"
#include "cleaving.h"
#include "cleaving.cpp"
#include "basic_fsst.h"

#include <sys/stat.h>


using namespace duckdb;


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

FSSTCompressionResult fsst_compress(const std::vector<size_t>& lenIn, std::vector<const unsigned char *>& strIn) {
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
	fsst_compress(
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

	return FSSTCompressionResult{encoder, lenOut, strOut};
}
size_t calc_encoded_strings_size(const FSSTCompressionResult& prefix_sompression_result) {
	size_t result = 0;
	size_t size = prefix_sompression_result.encoded_strings_length.size();
	for (size_t i = 0; i < size; ++i) {
		result += prefix_sompression_result.encoded_strings_length[i];
	}

	// TODO: Should encoder be added?
	// // Correctly calculate decoder size by serialization
	// unsigned char buffer[FSST_MAXHEADER];
	// size_t serialized_size = fsst_export(prefix_sompression_result.encoder, buffer);
	// result += serialized_size;

	return result;
}


// Currently assumes each CompressedBlock has its own symbol table
// returns total_compressed_size
size_t compress(std::vector<size_t>& prefixLenIn, std::vector<const unsigned char *>& prefixStrIn,std::vector<size_t>& suffixLenIn, std::vector<const unsigned char *>& suffixStrIn, const std::vector<SimilarityChunk>& similarity_chunks, FSSTPlusCompressionResult& fsst_plus_compression_result) {
	const size_t BLOCK_DATA_CAPACITY = UINT16_MAX; // ~64KB capacity

	FSSTCompressionResult prefix_result = fsst_compress(prefixLenIn, prefixStrIn);
	size_t encoded_prefixes_size = calc_encoded_strings_size(prefix_result);

	FSSTCompressionResult suffix_result = fsst_compress(suffixLenIn, suffixStrIn);
	size_t encoded_suffixes_size = calc_encoded_strings_size(suffix_result);

	if (encoded_prefixes_size + encoded_suffixes_size > BLOCK_DATA_CAPACITY) {
		std::cerr << "Block Data Capacity Exceeded\n"; //TODO: Throws an error for now, should handle it gracefully in the future
		throw std::logic_error("Block Data Capacity Exceeded");
	}
	// Gradually fill compressed block up to 64kb from prefix data area, and create a new compressed block when it's full.
	CompressedBlock current_block;
	size_t current_block_size = 0;

	current_block.prefix_data_area = new uint8_t[BLOCK_DATA_CAPACITY]; // TODO: Is this a C-style array? Is there a better way to do this?
	current_block.suffix_data_area = current_block.prefix_data_area + encoded_prefixes_size;

	// This is where we will write to
	uint8_t* prefix_writer = current_block.prefix_data_area;
	uint8_t* suffix_writer = current_block.suffix_data_area;


	for (size_t i = 0; i < similarity_chunks.size(); i++) {
		const SimilarityChunk& chunk = similarity_chunks[i];
		const size_t stop_index = i == similarity_chunks.size() - 1 ? suffix_result.encoded_strings_length.size() : similarity_chunks[i+1].start_index;
		if (chunk.prefix_length == 0) {

			//Write suffix
			*suffix_writer++ = 0; // prefix_length = 0
			for (size_t j = chunk.start_index; j < stop_index; j++) {
				for (size_t k = 0; k < suffix_result.encoded_strings_length[j]; k++) {
					*suffix_writer++ = suffix_result.encoded_strings[j][k];
				}
			}
		} else {
			//Write prefix
			if (prefix_writer + chunk.prefix_length < current_block.suffix_data_area) { // If fits
				for (size_t k = 0; k < prefix_result.encoded_strings_length[i]; k++) {
					*prefix_writer++ = prefix_result.encoded_strings[i][k];
				}
			}

			//Write suffix
			*suffix_writer++ = chunk.prefix_length; // prefix_length = 0

			uint16_t jumpback_offset = suffix_writer - (prefix_writer - 1); // jumpback offset. minus one because prefix_writer incremented after the write
			memcpy(suffix_writer, &jumpback_offset, sizeof(jumpback_offset));
			suffix_writer += sizeof(jumpback_offset);

			for (size_t j = chunk.start_index; j < stop_index; j++) {
				for (size_t k = 0; k < suffixLenIn[j]; k++) {
					*suffix_writer++ = suffixStrIn[j][k];
				}
			}
		}
	}

	size_t total_compressed_size = (prefix_writer - current_block.prefix_data_area) + (suffix_writer - current_block.suffix_data_area); //TODO: include symbol table?
	total_compressed_size += calc_symbol_table_size(prefix_result.encoder);
	total_compressed_size += calc_symbol_table_size(suffix_result.encoder);
	return total_compressed_size;
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


void print_compression_stats(size_t total_strings_amount, size_t total_string_size, size_t total_compressed_string_size) {
	// Print compression stats
	std::cout << "✅ ✅ ✅ Compressed " << total_strings_amount << " strings ✅ ✅ ✅\n";
	std::cout << "Original   size: " << total_string_size << " bytes\n";
	std::cout << "Compressed size: " << total_compressed_string_size << " bytes\n";
	std::cout << "Compression ratio: " << (double)(total_string_size) / total_compressed_string_size << "\n\n";
}

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	constexpr size_t total_strings = 3000;
	// Now call run_basic_fsst from basic_fsst.cpp
	run_basic_fsst(con, "/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/clickbenchurl.parquet", total_strings);
	
	// Now continue with main's own processing
	const auto result = con.Query("SELECT Url FROM read_parquet('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/clickbenchurl.parquet') LIMIT " + std::to_string(total_strings) + ";");
	auto data_chunk = result->Fetch();

	size_t total_strings_amount = {0};
	size_t total_string_size = {0};
	size_t total_compressed_string_size = {0};

	std::cout <<
		"===============================================\n"<<
		"==========START FSST PLUS COMPRESSION==========\n"<<
		"===============================================\n";
	while (data_chunk) {
		const size_t n = data_chunk->size();
		std::cout << "🔷 " << n << " strings in DataChunk 🔷 \n";

		std::vector<std::string> original_strings;
		original_strings.reserve(n);

		std::vector<size_t> lenIn;
		lenIn.reserve(n);

		std::vector<const unsigned char*> strIn;
		strIn.reserve(n);
		
		// Populate lenIn and strIn
		extract_strings_from_data_chunk(data_chunk, original_strings, lenIn, strIn);
		
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
		for (size_t i = 0; i < n; i += config::cleaving_run_n) {
			std::cout << "Current Cleaving Run coverage: " << i << ":" << i + config::cleaving_run_n - 1 << std::endl;

			truncated_sort(lenIn, strIn, i);

			const std::vector<SimilarityChunk> cleaving_run_similarity_chunks = form_similarity_chunks(lenIn, strIn, i);
			similarity_chunks.insert(similarity_chunks.end(),
									 cleaving_run_similarity_chunks.begin(),
									 cleaving_run_similarity_chunks.end());
		}
		cleave(lenIn, strIn, similarity_chunks, prefixLenIn, prefixStrIn, suffixLenIn, suffixStrIn);

		FSSTPlusCompressionResult compression_result;
		compression_result.run_start_offsets = {nullptr}; // Placeholder

		total_compressed_string_size += compress(prefixLenIn, prefixStrIn, suffixLenIn, suffixStrIn, similarity_chunks, compression_result);
		total_strings_amount += lenIn.size();
		for (size_t string_length : lenIn) {
			total_string_size += string_length;
		}

		// get the next chunk, continue loop
		data_chunk = result->Fetch();
	}
	print_compression_stats(total_strings_amount, total_string_size, total_compressed_string_size);

	// // Cleanup
	// std::cout << "Cleanup";
	// fsst_destroy(encoder);
	return 0;
}
