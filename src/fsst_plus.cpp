#include "config.h"
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "fsst_plus.h"
#include "cleaving.h"
#include "basic_fsst.h"

#include "duckdb_utils.h"

namespace config {
	// constexpr size_t total_strings = 2600; // # of input strings
	// constexpr size_t compressed_block_data_capacity = UINT16_MAX; // ~64KB capacity
	constexpr size_t total_strings = 100; // just to test
	constexpr size_t compressed_block_data_capacity = 1000; // just to test

	constexpr size_t compressed_block_granularity = 128; // number of elements per cleaving run. // TODO: Should be determined dynamically based on the string size. If the string is >32kb it can dreadfully compress to 64kb so we can't do jumpback. In that case compressed_block_granularity = 1. And btw, can't it be 129 if it fits? why not?
	constexpr size_t max_prefix_size = 120; // how far into the string to scan for a prefix. (max prefix size)
	constexpr size_t amount_strings_per_symbol_table = 120000; // 120000 = a duckdb row group
	constexpr bool print_sorted_corpus = false;
	constexpr bool print_split_points = false; // prints compressed corpus displaying split points
}

size_t calc_encoded_strings_size(const FSSTCompressionResult& compression_result) {
	size_t result = 0;
	size_t size = compression_result.encoded_strings_length.size();
	for (size_t i = 0; i < size; ++i) {
		result += compression_result.encoded_strings_length[i];
	}
	return result;
}

size_t find_similarity_chunk_corresponding_to_index(const size_t& target_index, const std::vector<SimilarityChunk>& similarity_chunks) {
	// Binary search
	size_t l =  0, r = similarity_chunks.size()-1;
	while (l <= r) {
		const size_t m = l + (r - l) / 2;

		if (similarity_chunks[m].start_index < target_index && similarity_chunks[m + 1].start_index  < target_index) {
			l = m + 1;
			continue;
		}
		if (target_index < similarity_chunks[m].start_index ) {
			r = m - 1;
			continue;
		}
		return m;
	}
	std::cerr << "Couldn't find_similarity_chunk_corresponding_to_index: "<< target_index <<"\n";
	throw std::logic_error("ERROR on find_similarity_chunk_corresponding_to_index()");
}

void do_allocation(const CheckpointInfo &checkpoint_info, CompressedBlock &current_block, size_t prefix_byte_size, size_t suffix_byte_size, const CheckpointInfo& new_checkpoint_info) {
	// TODO: Is this a C-style array? Is there a better way to do this?
	size_t bytes_to_allocate = prefix_byte_size + suffix_byte_size;
	current_block.prefix_data_area = new uint8_t[bytes_to_allocate]; // Allocate all space needed only once, on prefix_data_area (instead of allocating twice, which might not be contiguous data areas)
	current_block.suffix_data_area = current_block.prefix_data_area + prefix_byte_size; // advance the suffix pointer to the end of prefix

	std::cout << "🟧 New Compressed Block allocated, range " << checkpoint_info.checkpoint_string_index << ":" << new_checkpoint_info.checkpoint_string_index-1 << ", byte size: "<< bytes_to_allocate <<" 🟧\n";
}

size_t calculate_compressed_string_byte_size(size_t encoded_string_size, const SimilarityChunk &chunk) {
	size_t compressed_string_byte_size = encoded_string_size + 8;
	if (chunk.prefix_length != 0) {
		compressed_string_byte_size += 16; // 16 for jumpback offset
	}
	return compressed_string_byte_size;
}

CheckpointInfo allocate_compressed_block(CheckpointInfo checkpoint_info,
                                         const std::vector<SimilarityChunk> &similarity_chunks,
                                         const FSSTCompressionResult &prefix_result,
                                         const FSSTCompressionResult &suffix_result,
                                         CompressedBlock &current_block) {
	size_t prefix_byte_size = 0;
	size_t suffix_byte_size = 0;
	CheckpointInfo new_checkpoint_info = {checkpoint_info.checkpoint_string_index, checkpoint_info.checkpoint_similarity_chunk_index};

	for (size_t i = checkpoint_info.checkpoint_similarity_chunk_index; i < similarity_chunks.size(); ++i) { // start at the similarity chunk we left off on

		const SimilarityChunk& chunk = similarity_chunks[i];
		const size_t encoded_prefix_length = prefix_result.encoded_strings_length[i]; // because chunk.prefix_length is DECOMPRESSED!
		size_t compressed_first_suffix_byte_size = calculate_compressed_string_byte_size(suffix_result.encoded_strings_length[checkpoint_info.checkpoint_string_index], chunk);

		// prefix and first suffix DOESN'T FIT, break.
		if (prefix_byte_size+suffix_byte_size + encoded_prefix_length + compressed_first_suffix_byte_size >
			config::compressed_block_data_capacity) {
			if (i == checkpoint_info.checkpoint_similarity_chunk_index) {
					std::cerr << "STRING TOO LONG! First encoded prefix + encoded suffix byte lengths exceed Block Data Capacity\n"; // Should we just have a hard limit on 64 kb per string? How to avoid this error? Split string on multiple blocks?
					throw std::logic_error("Block Data Capacity Exceeded");
			}
			do_allocation(checkpoint_info, current_block, prefix_byte_size, suffix_byte_size, new_checkpoint_info);
			return new_checkpoint_info;
		}
		prefix_byte_size += encoded_prefix_length;
		new_checkpoint_info.checkpoint_similarity_chunk_index = i;

		// And now add chunk's suffixes
		const size_t stop_index = i == similarity_chunks.size() - 1 ? suffix_result.encoded_strings_length.size() : similarity_chunks[i+1].start_index;

		for (size_t j = chunk.start_index ; j < stop_index; j++) {
			size_t compressed_string_byte_size = calculate_compressed_string_byte_size(suffix_result.encoded_strings_length[j], chunk);

			// Suffix DOESN'T FIT, break
			if (prefix_byte_size+suffix_byte_size + compressed_string_byte_size > config::compressed_block_data_capacity) {
				do_allocation(checkpoint_info, current_block, prefix_byte_size, suffix_byte_size, new_checkpoint_info);
				return new_checkpoint_info;
			}
			suffix_byte_size += compressed_string_byte_size;
			new_checkpoint_info.checkpoint_string_index++;
		}
	}

	do_allocation(checkpoint_info, current_block, prefix_byte_size, suffix_byte_size, new_checkpoint_info);
	return new_checkpoint_info;
}

// Currently assumes each CompressedBlock has its own symbol table
// returns total_compressed_size
size_t compress(std::vector<size_t>& prefixLenIn, std::vector<const unsigned char *>& prefixStrIn,std::vector<size_t>& suffixLenIn, std::vector<const unsigned char *>& suffixStrIn, const std::vector<SimilarityChunk>& similarity_chunks, FSSTPlusCompressionResult& fsst_plus_compression_result) {


	FSSTCompressionResult prefix_result = fsst_compress(prefixLenIn, prefixStrIn);
	// size_t encoded_prefixes_byte_size = calc_encoded_strings_size(prefix_result);
	FSSTCompressionResult suffix_result = fsst_compress(suffixLenIn, suffixStrIn);
	// size_t encoded_suffixes_byte_size = calc_encoded_strings_size(suffix_result);

	CheckpointInfo checkpoint_info = {0, 0};
	size_t total_compressed_byte_size = {0};

	while (checkpoint_info.checkpoint_string_index < suffix_result.encoded_strings_length.size()) {

		// Gradually fill compressed block up to 64kb from prefix data area, and create a new compressed block when it's full.
		CompressedBlock current_block;
		// Calculate up to what index we should store data. (up to 64 kb)
		CheckpointInfo new_checkpoint_info = allocate_compressed_block(checkpoint_info, similarity_chunks, prefix_result , suffix_result, current_block);


		// This is where we will write to
		uint8_t* prefix_writer = current_block.prefix_data_area;
		uint8_t* suffix_writer = current_block.suffix_data_area;

		for (size_t i = checkpoint_info.checkpoint_similarity_chunk_index; i < new_checkpoint_info.checkpoint_similarity_chunk_index; i++) {
			const SimilarityChunk& chunk = similarity_chunks[i];
			const size_t stop_index = i == similarity_chunks.size() - 1 ? suffix_result.encoded_strings_length.size() : similarity_chunks[i+1].start_index;

			if (chunk.prefix_length == 0) {
				//Write suffix
				*suffix_writer++ = 0; // prefix_length = 0
				for (size_t j = checkpoint_info.checkpoint_string_index; j < stop_index; j++) {
					for (size_t k = 0; k < suffix_result.encoded_strings_length[j]; k++) {
						*suffix_writer++ = suffix_result.encoded_strings[j][k];
					}
				}
			} else {
				//Write prefix
				size_t compressed_prefix_length = prefix_result.encoded_strings_length[i]; // because chunk.prefix_length is DECOMPRESSED!
				if (prefix_writer + compressed_prefix_length >= current_block.suffix_data_area) { // If doesn't fit throw error
					std::cerr << "Prefix doesnt fit in prefix area\n"; // Should we just have a hard limit on 64 kb per string? How to avoid this error? Split string on multiple blocks?
					throw std::logic_error("Block Data Capacity Exceeded");
				}

				for (size_t k = 0; k < compressed_prefix_length; k++) {
					*prefix_writer++ = prefix_result.encoded_strings[i][k];
				}
				*suffix_writer++ = compressed_prefix_length; // prefix_length = 0 -- Why is this line needed btw?

				//Write suffix
				uint16_t jumpback_offset = suffix_writer - (prefix_writer - 1); // jumpback offset. minus one because prefix_writer incremented after the write
				memcpy(suffix_writer, &jumpback_offset, sizeof(jumpback_offset));
				suffix_writer += sizeof(jumpback_offset);

				for (size_t j = checkpoint_info.checkpoint_string_index; j < stop_index; j++) {
					for (size_t k = 0; k < suffixLenIn[j]; k++) {
						*suffix_writer++ = suffixStrIn[j][k];
					}
				}
			}
		}

		total_compressed_byte_size += (prefix_writer - current_block.prefix_data_area) + (suffix_writer - current_block.suffix_data_area); //TODO: include symbol table? Where is symbol table stored in the datastructure?
		total_compressed_byte_size += calc_symbol_table_size(prefix_result.encoder);
		total_compressed_byte_size += calc_symbol_table_size(suffix_result.encoder);
		checkpoint_info = new_checkpoint_info;
	}
	return total_compressed_byte_size;
}



int main() {
	DuckDB db(nullptr);
	Connection con(db);

	const string query = "SELECT Url FROM read_parquet('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/clickbenchurl.parquet') LIMIT " + std::to_string(config::total_strings) + ";";

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

	const size_t n = std::min(config::amount_strings_per_symbol_table, config::total_strings);

	std::cout << "🔷 " << n << " strings for this symbol table 🔷 \n";
	while (data_chunk) {
		std::vector<std::string> original_strings;
		original_strings.reserve(n);

		std::vector<size_t> lenIn;
		lenIn.reserve(n);

		std::vector<const unsigned char*> strIn;
		strIn.reserve(n);

		while (data_chunk && lenIn.size() < n) {
			const size_t data_chunk_size = data_chunk->size();
			std::cout << "> " << data_chunk_size << " strings in DataChunk\n";

			// Populate lenIn and strIn
			extract_strings_from_data_chunk(data_chunk, original_strings, lenIn, strIn);
			// fill up to n (150.000 or the queried amount)
			data_chunk = result->Fetch();
		}

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
		for (size_t i = 0; i < n; i += config::compressed_block_granularity) {
			const size_t cleaving_run_n = std::min(lenIn.size() - i, config::compressed_block_granularity);

			std::cout << "Current Cleaving Run coverage: " << i << ":" << i + cleaving_run_n - 1 << std::endl;

			truncated_sort(lenIn, strIn, i, cleaving_run_n);

			const std::vector<SimilarityChunk> cleaving_run_similarity_chunks = form_similarity_chunks(lenIn, strIn, i, cleaving_run_n);
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

		print_compression_stats(total_strings_amount, total_string_size, total_compressed_string_size);
		std::cout << "TODO: Save compressed data to the database.\n\n";
		// get the next chunk, continue loop
		data_chunk = result->Fetch();
	}

	// // Cleanup
	// std::cout << "Cleanup";
	// fsst_destroy(encoder);
	return 0;
}
