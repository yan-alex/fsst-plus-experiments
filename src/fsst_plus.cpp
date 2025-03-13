#include "config.h"
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "fsst_plus.h"
#include "cleaving.h"
#include "basic_fsst.h"

#include "duckdb_utils.h"
#include <string>

namespace config {
	constexpr size_t total_strings = 1000; // # of input strings
	constexpr size_t compressed_block_byte_capacity = UINT16_MAX; // ~64KB capacity
	constexpr bool print_sorted_corpus = false;
	constexpr bool print_split_points = true; // prints compressed corpus displaying split points
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
		if (similarity_chunks[m].start_index < target_index && similarity_chunks[m + 1].start_index  <= target_index) {
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


size_t calculate_compressed_suffix_byte_size(size_t encoded_suffix_size, const SimilarityChunk &chunk) {
	size_t compressed_string_byte_size = encoded_suffix_size + 8;
	if (chunk.prefix_length != 0) {
		compressed_string_byte_size += 16; // 16 for jumpback offset
	}
	return compressed_string_byte_size;
}

void allocate_maximum(uint8_t*& compression_result_data, FSSTCompressionResult prefix_compression_result, FSSTCompressionResult suffix_compression_result, std::vector<SimilarityChunk> similarity_chunks) {
	size_t result = {0};

	size_t ns = suffix_compression_result.encoded_strings.size();
	size_t ng = std::ceil(static_cast<double>(ns)/128);
	size_t all_group_overhead = ng * (1+1+128*2);
	result += all_group_overhead;
	result += calc_encoded_strings_size(prefix_compression_result);
	result += calc_encoded_strings_size(suffix_compression_result);
	result += ns*3;

	compression_result_data = new uint8_t[result];
}

void decompress(uint8_t * data, const fsst_decoder_t & fsst_decoder_prefix, const fsst_decoder_t & fsst_decoder_suffix) {
	const auto n_strings = Load<uint8_t>(data);
	data += sizeof(uint8_t);

	constexpr uint16_t BUFFER_SIZE = 100000;
	auto* result = new unsigned char[BUFFER_SIZE];

	uint16_t string_offsets[n_strings];

	// todo: we can't decompress the last string at the moment
	for (int i = 0; i < (n_strings  - 1)*sizeof(uint16_t); i += sizeof(uint16_t)) {
		const auto curr_string_offest = data + i;
		const auto offset =  Load<uint16_t>(curr_string_offest);
		const auto next_offset =  Load<uint16_t>(curr_string_offest +sizeof(uint16_t));
		const uint16_t full_suffix_length = next_offset + sizeof(uint16_t) - offset; // Add +sizeof(uint16_t) because next offset starts from itself

		uint8_t* start_suffix = curr_string_offest + offset ;
		// correct start suffix:  0x000000012181a840
		// wrong   start suffix:  0x000000012181a7b0

		const auto prefix_length = Load<uint8_t>(start_suffix);
		start_suffix += sizeof(uint8_t);
		if (prefix_length == 0) {
			// suffix only
			const size_t decompressed_size = fsst_decompress(&fsst_decoder_suffix, full_suffix_length - sizeof(uint8_t), start_suffix, BUFFER_SIZE, result);
			std::cout << i/2 << " decompressed: ";
			for (int j = 0; j <decompressed_size; j++) {
				std::cout << result[j];
			}
			std::cout << "\n";
		} else {
			const auto jumpback_offset = Load<uint16_t>(start_suffix);
			start_suffix+=sizeof(uint16_t);
			const auto start_prefix = start_suffix-jumpback_offset - sizeof(uint8_t) - sizeof(uint16_t);

			// Step 1) Decompress prefix
			const size_t decompressed_prefix_size = fsst_decompress(&fsst_decoder_prefix, prefix_length, start_prefix, BUFFER_SIZE, result);
			std::cout << i/2 << " decompressed: ";
			for (int j = 0; j <decompressed_prefix_size; j++) {
				std::cout << result[j];
			}
			// Step 2) Decompress suffix
			const size_t decompressed_suffix_size = fsst_decompress(&fsst_decoder_suffix, full_suffix_length - sizeof(uint8_t) - sizeof(uint16_t),
				// start_suffix+sizeof(uint16_t), // fixes it
				start_suffix,
				BUFFER_SIZE, result);
			for (int j = 0; j <decompressed_suffix_size; j++) {
				std::cout << result[j];
			}
			std::cout << "\n";
		}
	}
};

void calculate_block_size(std::vector<SimilarityChunk> similarity_chunks, FSSTCompressionResult prefix_compression_result, FSSTCompressionResult suffix_compression_result, BlockMetadata &b) {
	// Each blocks starts with the num_strings (uint8_t) and the base offset (uint8_t)
	size_t initial_block_size = sizeof(uint8_t) + sizeof(uint8_t);
	b.block_size += initial_block_size;

	while (b.suffix_n_in_block < config::compressed_block_granularity) {
		const size_t suffix_index = b.suffix_area_start_index + b.suffix_n_in_block;
		size_t prefix_index_for_suffix = find_similarity_chunk_corresponding_to_index(suffix_index, similarity_chunks);
		const SimilarityChunk &chunk = similarity_chunks[prefix_index_for_suffix];
		const bool suffix_has_prefix = chunk.prefix_length != 0;

		if (prefix_index_for_suffix != b.prefix_last_index_added) {
			// we add a new prefix to our block. The block grows only by the size of the encoded prefix
			const size_t prefix_size =  prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
			const size_t block_size_with_prefix = b.block_size + prefix_size;

			// exit loop if we can't fit prefix
			if (block_size_with_prefix >= config::compressed_block_byte_capacity) {
				break;
			}

			std::cout << "Add Prefix " << b.prefix_n_in_block << " Length=" << prefix_size << '\n';

			// increase the number of prefixes for this block, add the size to the block size
			b.prefix_offsets_from_first_prefix[b.prefix_n_in_block] = b.prefix_area_size;
			b.prefix_n_in_block += 1;
			b.prefix_area_size += prefix_size;
			b.block_size += prefix_size;


			// make sure we don't add it again
			b.prefix_last_index_added = prefix_index_for_suffix;
		}

		// calculate the potential size of the suffix
		size_t suffix_total_size = 0;
		// 1) We also have the encoded suffix
		suffix_total_size += suffix_compression_result.encoded_strings_length[suffix_index];
		// 2) We will always add the size of the prefix for this suffix, 0 if there is no prefix
		suffix_total_size += sizeof(uint8_t);
		// 3) Prefix length != zero: We have to add the jumpback (uint16_t)
		if (suffix_has_prefix) {
			suffix_total_size += sizeof(uint16_t);
		}
		// 4) This means we have to add a new string offset in the block-header (uint16_t)
		constexpr size_t suffix_block_header_size = sizeof(uint16_t);

		// exit loop if we can't fit suffix
		if (b.block_size + suffix_total_size + suffix_block_header_size >= config::compressed_block_byte_capacity) {
			break;
		}
		// store the string offset
		b.suffix_offsets_from_first_suffix[b.suffix_n_in_block] = b.suffix_offset_current;
		b.suffix_encoded_prefix_lengths[b.suffix_n_in_block] = prefix_compression_result.encoded_strings_length[prefix_index_for_suffix]; // the ENCODED prefix length
		b.suffix_prefix_index[b.suffix_n_in_block] = b.prefix_n_in_block - 1; // as we already increased prefix_n_in_block before
		b.suffix_offset_current += suffix_total_size;

		// we can add the suffix
		b.block_size += suffix_total_size;
		b.suffix_n_in_block += 1;
	}

	std::cout << "N Strings: " << b.suffix_n_in_block << " N Prefixes: " << b.prefix_n_in_block << " Block size: " <<b.block_size << " Pre size: " <<b.prefix_area_size << std::endl;
}

void write_block(FSSTPlusCompressionResult compression_result, FSSTCompressionResult prefix_compression_result, FSSTCompressionResult suffix_compression_result, const BlockMetadata b) {
	uint8_t* current_data_ptr = compression_result.data;

	// A) WRITE THE HEADER

	// A 1) Write the number of strings as an uint_8
	Store<uint8_t>(b.suffix_n_in_block, current_data_ptr);
	current_data_ptr += sizeof(uint8_t);
	// A 2) Write the string_offsets[]
	for (size_t i = 0; i < b.suffix_n_in_block; i++) {
		uint16_t offset_array_size_to_go = (b.suffix_n_in_block-i) * sizeof(uint16_t);
		uint16_t string_offset = b.prefix_area_size + offset_array_size_to_go + b.suffix_offsets_from_first_suffix[i];
		Store<uint16_t>(string_offset, current_data_ptr);
		current_data_ptr += sizeof(uint16_t);
	}

	// B) WRITE THE PREFIX AREA
	for (size_t i = 0; i < b.prefix_n_in_block; i++) {
		const size_t prefix_index = b.prefix_area_start_index + i;
		const size_t prefix_length = prefix_compression_result.encoded_strings_length[prefix_index];

		std::cout << "Write Prefix " << i << " Length=" << prefix_length << '\n';
		const unsigned char * prefix_start = prefix_compression_result.encoded_strings[prefix_index];
		memcpy(current_data_ptr, prefix_start, prefix_length);
		current_data_ptr += prefix_length;
	}

	// C) WRITE SUFFIX AREA
	for (size_t i = 0; i < b.suffix_n_in_block; i ++){
		const size_t suffix_index = b.suffix_area_start_index + i;

		uint8_t prefix_index_for_suffix = b.suffix_prefix_index[suffix_index];
		uint8_t suffix_prefix_length = b.suffix_encoded_prefix_lengths[suffix_index];
		const bool suffix_has_prefix = suffix_prefix_length != 0;

		// write the length of the prefix, can be zero
		Store<uint8_t>(suffix_prefix_length, current_data_ptr);
		current_data_ptr += sizeof(uint8_t);

		// if there is a prefix, calculate offset and store it
		if (suffix_has_prefix) {
			size_t prefix_offset_from_first_prefix = b.prefix_offsets_from_first_prefix[prefix_index_for_suffix];
			size_t suffix_offset_from_first_suffix = b.suffix_offsets_from_first_suffix[suffix_index];
			uint16_t prefix_jumpback_offset = (b.prefix_area_size - prefix_offset_from_first_prefix) + suffix_offset_from_first_suffix;

			Store<uint16_t>(prefix_jumpback_offset, current_data_ptr);
			current_data_ptr += sizeof(uint16_t);
		}

		// write the suffix
		const size_t suffix_length = suffix_compression_result.encoded_strings_length[suffix_index];
		const unsigned char * suffix_start = suffix_compression_result.encoded_strings[suffix_index];
		memcpy(current_data_ptr, suffix_start, suffix_length);
		current_data_ptr += suffix_length;
	}
	std::cout << b.block_size << "\n";
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

	std::vector<std::string> original_strings;
	original_strings.reserve(n);

	std::vector<size_t> lenIn;
	lenIn.reserve(n);
	std::vector<const unsigned char*> strIn;
	strIn.reserve(n);

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

	std::cout << "🔷 " << n << " strings for this symbol table 🔷 \n";
	while (data_chunk) {
		const size_t data_chunk_size = data_chunk->size();

		std::cout << "> " << data_chunk_size << " strings in DataChunk\n";

		// Populate lenIn and strIn
		extract_strings_from_data_chunk(data_chunk, original_strings, lenIn, strIn);

		data_chunk = result->Fetch();
	}

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

	size_t total_string_count = suffixLenIn.size();
	FSSTCompressionResult prefix_compression_result = fsst_compress(prefixLenIn, prefixStrIn);
	// size_t encoded_prefixes_byte_size = calc_encoded_strings_size(prefix_result);
	FSSTCompressionResult suffix_compression_result = fsst_compress(suffixLenIn, suffixStrIn);
	// size_t encoded_suffixes_byte_size = calc_encoded_strings_size(suffix_result);

	allocate_maximum(compression_result.data, prefix_compression_result, suffix_compression_result, similarity_chunks);


	BlockMetadata b;
	// *** calculate the size for the next block to insert ***
	calculate_block_size(similarity_chunks, prefix_compression_result, suffix_compression_result, b);


	// *** WRITE THE BLOCK NOW WHERE WE NOW HOW MANY STRINGS TO PUT IN ***
	write_block(compression_result, prefix_compression_result, suffix_compression_result, b);

	// decompress to check
	decompress(compression_result.data, fsst_decoder(prefix_compression_result.encoder), fsst_decoder(suffix_compression_result.encoder));


	// compression_result.run_start_offsets = {nullptr}; // Placeholder
	// total_compressed_string_size += compress(prefixLenIn, prefixStrIn, suffixLenIn, suffixStrIn, similarity_chunks, compression_result);

	total_strings_amount += lenIn.size();
	for (size_t string_length : lenIn) {
		total_string_size += string_length;
	}

	print_compression_stats(total_strings_amount, total_string_size, total_compressed_string_size);
	std::cout << "TODO: Save compressed data to the database.\n\n";
	// get the next chunk, continue loop

	// // Cleanup
	// std::cout << "Cleanup";
	// fsst_destroy(encoder);
	return 0;
}
