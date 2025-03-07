#include "config.h"
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "fsst_plus.h"
#include "cleaving.h"
#include "basic_fsst.h"

#include "duckdb_utils.h"

namespace config {
	constexpr size_t cleaving_run_n = 128; // number of elements per cleaving run. 	// TODO: Should be determined dynamically based on the string size. If the string is >32kb it can dreadfully compress to 64kb so we can't do jumpback. In that case cleaving_run_n = 1
	constexpr size_t max_prefix_size = 120; // how far into the string to scan for a prefix. (max prefix size)
	constexpr bool print_sorted_corpus = false;
	constexpr bool print_split_points = false; // prints compressed corpus displaying split points
}

size_t calc_encoded_strings_size(const FSSTCompressionResult& prefix_sompression_result) {
	size_t result = 0;
	size_t size = prefix_sompression_result.encoded_strings_length.size();
	for (size_t i = 0; i < size; ++i) {
		result += prefix_sompression_result.encoded_strings_length[i];
	}
	return result;
}

// Currently assumes each CompressedBlock has its own symbol table
// returns total_compressed_size
size_t compress(std::vector<size_t>& prefixLenIn, std::vector<const unsigned char *>& prefixStrIn,std::vector<size_t>& suffixLenIn, std::vector<const unsigned char *>& suffixStrIn, const std::vector<SimilarityChunk>& similarity_chunks, FSSTPlusCompressionResult& fsst_plus_compression_result) {
	const size_t BLOCK_DATA_CAPACITY = UINT16_MAX; // ~64KB capacity

	FSSTCompressionResult prefix_result = fsst_compress(prefixLenIn, prefixStrIn); //TODO: make a variant to use the same symbol table for prefix and suffix
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



int main() {
	DuckDB db(nullptr);
	Connection con(db);
	constexpr size_t total_strings = 2048;

	const string query = "SELECT Url FROM read_parquet('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/clickbenchurl.parquet') LIMIT " + std::to_string(total_strings) + ";";
	// ======= RUN BASIC FSST TO COMPARE =======
	run_basic_fsst(con, query);

	// Now continue with main's own processing
	const auto result = con.Query(query);
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
