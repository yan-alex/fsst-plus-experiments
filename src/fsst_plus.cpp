#include "config.h"
#include "duckdb.hpp"
#include <iostream>
#include <ranges>
#include "fsst_plus.h"
#include "cleaving.h"
#include "basic_fsst.h"

#include "duckdb_utils.h"

namespace config {
	constexpr size_t total_strings = 1000; // # of input strings
	constexpr size_t compressed_block_byte_capacity = UINT16_MAX; // ~64KB capacity
	// constexpr size_t compressed_block_byte_capacity = 2000; // just to test

	constexpr size_t compressed_block_granularity = 128; // number of elements per cleaving run. // TODO: Should be determined dynamically based on the string size. If the string is >32kb it can dreadfully compress to 64kb so we can't do jumpback. In that case compressed_block_granularity = 1. And btw, can't it be 129 if it fits? why not?
	constexpr size_t max_prefix_size = 120; // how far into the string to scan for a prefix. (max prefix size)
	constexpr size_t amount_strings_per_symbol_table = 120000; // 120000 = a duckdb row group
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

void do_allocation(CompressedBlock &current_block, size_t prefix_byte_size, size_t suffix_byte_size) {
	// TODO: Is this a C-style array? Is there a better way to do this?
	size_t bytes_to_allocate = prefix_byte_size + suffix_byte_size;
	current_block.prefix_data_area = new uint8_t[bytes_to_allocate]; // Allocate all space needed only once, on prefix_data_area (instead of allocating twice, which might not be contiguous data areas)
	current_block.suffix_data_area = current_block.prefix_data_area + prefix_byte_size; // advance the suffix pointer to the end of prefix

}

size_t calculate_compressed_suffix_byte_size(size_t encoded_suffix_size, const SimilarityChunk &chunk) {
	size_t compressed_string_byte_size = encoded_suffix_size + 8;
	if (chunk.prefix_length != 0) {
		compressed_string_byte_size += 16; // 16 for jumpback offset
	}
	return compressed_string_byte_size;
}

// CheckpointInfo allocate_compressed_block(CheckpointInfo checkpoint_info,
//                                          const std::vector<SimilarityChunk> &similarity_chunks,
//                                          const FSSTCompressionResult &prefix_result,
//                                          const FSSTCompressionResult &suffix_result,
//                                          CompressedBlock &current_block) {
// 	size_t prefix_byte_size = 0;
// 	size_t suffix_byte_size = 0;
// 	CheckpointInfo new_checkpoint_info = {checkpoint_info.checkpoint_string_index, checkpoint_info.checkpoint_similarity_chunk_index};
//
// 	for (size_t i = checkpoint_info.checkpoint_similarity_chunk_index; i < similarity_chunks.size(); ++i) { // start at the similarity chunk we left off on
//
// 		const SimilarityChunk& chunk = similarity_chunks[i];
// 		const size_t encoded_prefix_length = prefix_result.encoded_strings_length[i]; // because chunk.prefix_length is DECOMPRESSED!
// 		size_t compressed_first_suffix_byte_size = calculate_compressed_string_byte_size(suffix_result.encoded_strings_length[checkpoint_info.checkpoint_string_index], chunk);
//
// 		// prefix and first suffix DOESN'T FIT, break.
// 		if (prefix_byte_size+suffix_byte_size + encoded_prefix_length + compressed_first_suffix_byte_size >
// 			config::compressed_block_data_capacity) {
// 			if (i == checkpoint_info.checkpoint_similarity_chunk_index) {
// 					std::cerr << "STRING TOO LONG! First encoded prefix + encoded suffix byte lengths exceed Block Data Capacity\n"; // Should we just have a hard limit on 64 kb per string? How to avoid this error? Split string on multiple blocks?
// 					throw std::logic_error("Block Data Capacity Exceeded");
// 			}
// 			do_allocation(checkpoint_info, current_block, prefix_byte_size, suffix_byte_size, new_checkpoint_info);
// 			return new_checkpoint_info;
// 		}
// 		prefix_byte_size += encoded_prefix_length;
// 		new_checkpoint_info.checkpoint_similarity_chunk_index = i;
//
// 		// And now add chunk's suffixes
// 		const size_t stop_index = i == similarity_chunks.size() - 1 ? suffix_result.encoded_strings_length.size() : similarity_chunks[i+1].start_index;
//
// 		for (size_t j = chunk.start_index ; j < stop_index; j++) {
// 			size_t compressed_string_byte_size = calculate_compressed_string_byte_size(suffix_result.encoded_strings_length[j], chunk);
//
// 			// Suffix DOESN'T FIT, break
// 			if (prefix_byte_size+suffix_byte_size + compressed_string_byte_size > config::compressed_block_data_capacity) {
// 				do_allocation(checkpoint_info, current_block, prefix_byte_size, suffix_byte_size, new_checkpoint_info);
// 				return new_checkpoint_info;
// 			}
// 			suffix_byte_size += compressed_string_byte_size;
// 			new_checkpoint_info.checkpoint_string_index++;
// 		}
// 	}
//
// 	do_allocation(checkpoint_info, current_block, prefix_byte_size, suffix_byte_size, new_checkpoint_info);
// 	return new_checkpoint_info;
// }

std::pair<size_t, size_t> calc_to_add(
	const size_t index_string,
	CheckpointInfo checkpoint_info,
	const SimilarityChunk &chunk,
	const FSSTCompressionResult &prefix_result,
	const FSSTCompressionResult &suffix_result
	) {
	size_t compressed_prefix_byte_size = 0;
	size_t compressed_suffix_byte_size = 0;

	if (
		// case that its first string of compressed block (write prefix and suffix)
		index_string == checkpoint_info.checkpoint_string_index
		||
		// case that its first string of a similarity chunk (write prefix and suffix)
		index_string == chunk.start_index
		) {
		compressed_prefix_byte_size += prefix_result.encoded_strings_length[index_string];
		compressed_suffix_byte_size += calculate_compressed_suffix_byte_size(suffix_result.encoded_strings_length[index_string], chunk); // Adds jumpback offset if chunk has prefix length
	} else {
		// otherwise write suffix
		compressed_suffix_byte_size += calculate_compressed_suffix_byte_size(suffix_result.encoded_strings_length[index_string], chunk); // Adds jumpback offset if chunk has prefix length
	}
	return std::pair<size_t, size_t>(compressed_prefix_byte_size, compressed_suffix_byte_size);

}

CheckpointInfo allocate_compressed_block(CheckpointInfo checkpoint_info,
                                         const std::vector<SimilarityChunk> &similarity_chunks,
                                         const FSSTCompressionResult &prefix_result,
                                         const FSSTCompressionResult &suffix_result,
                                         CompressedBlock &current_block) {
    size_t total_string_count = suffix_result.encoded_strings_length.size();
    // The total combined byte size (prefix + suffix) that has been accumulated so far.
    size_t total_byte_size = 0;
    // This will hold the prefix size for the string that triggers a new block.
    size_t next_block_prefix_offset = 0;

    for (size_t i = checkpoint_info.checkpoint_string_index; i < total_string_count; i++) {
        // Find the similarity chunk corresponding to the current index.
        size_t index_chunk = find_similarity_chunk_corresponding_to_index(i, similarity_chunks);
        // Compute the bytes to add for the current string.
        auto [curr_prefix_size, curr_suffix_size] = calc_to_add(
            i,
            checkpoint_info,
            similarity_chunks[index_chunk],
            prefix_result,
            suffix_result
        );

        // If adding this string's bytes does not exceed the capacity, accumulate them.
        if (total_byte_size + curr_prefix_size + curr_suffix_size < config::compressed_block_byte_capacity) {
            total_byte_size += curr_prefix_size + curr_suffix_size;
            next_block_prefix_offset = curr_prefix_size; // Save the current prefix size for later use in allocation.
        } else {
            // Otherwise, the new string would overflow. So allocate the block with the
            // data from checkpoint_info.checkpoint_string_index up to i-1.
            current_block.prefix_data_area = new uint8_t[total_byte_size];
            // In this branch we use the prefix size computed for the current (failing) string
            // to mark the offset for the writing pointer for the next compressed block.
            current_block.suffix_data_area = current_block.prefix_data_area + curr_prefix_size;
            std::cout << "🟧 New Compressed Block allocated, range " << checkpoint_info.checkpoint_string_index
                      << ":" << i - 1 << ", byte size: " << total_byte_size << " 🟧\n";
            return CheckpointInfo{i, index_chunk};
        }
    }

    // If we went through all strings without overflowing the limit, allocate the final block.
    current_block.prefix_data_area = new uint8_t[total_byte_size];
    // Here we use the prefix size from the last successfully added item.
    current_block.suffix_data_area = current_block.prefix_data_area + next_block_prefix_offset;
    std::cout << "🟧 New Compressed Block allocated, range " << checkpoint_info.checkpoint_string_index
              << ":" << total_string_count - 1 << ", byte size: " << total_byte_size << " 🟧\n";

    // Return an overflowed pointer indicating that we're done.
    return CheckpointInfo{total_string_count, similarity_chunks.size()};
}

// Currently assumes each CompressedBlock has its own symbol table
// returns total_compressed_size
// size_t compress(std::vector<size_t>& prefixLenIn, std::vector<const unsigned char *>& prefixStrIn,std::vector<size_t>& suffixLenIn, std::vector<const unsigned char *>& suffixStrIn, const std::vector<SimilarityChunk>& similarity_chunks, FSSTPlusCompressionResult& fsst_plus_compression_result) {
//
//
//
// 	CheckpointInfo checkpoint_info = {0, 0};
// 	size_t total_compressed_byte_size = {0};
//
// 	while (checkpoint_info.checkpoint_string_index < total_string_count) {
//
// 		// Gradually fill compressed block up to 64kb from prefix data area, and create a new compressed block when it's full.
// 		CompressedBlock current_block;
// 		// Calculate up to what index we should store data. (up to 64 kb)
// 		CheckpointInfo new_checkpoint_info = allocate_compressed_block(checkpoint_info, similarity_chunks, prefix_compression_result , suffix_compression_result, current_block);
//
//
// 		// This is where we will write to
// 		uint8_t* prefix_writer = current_block.prefix_data_area;
// 		uint8_t* suffix_writer = current_block.suffix_data_area;
//
//
// 		// For each similarity chunk that fits (last one might fit for half)
// 		for (size_t i = checkpoint_info.checkpoint_similarity_chunk_index;
// 			i < new_checkpoint_info.checkpoint_similarity_chunk_index; i++) {
//
// 			// Handle case where last chunk fits for half
// 			const size_t stop_index =
// 					i == similarity_chunks.size() - 1
// 						? suffix_compression_result.encoded_strings_length.size()
// 						: std::min(similarity_chunks[i + 1].start_index, new_checkpoint_info.checkpoint_string_index);
//
// 			size_t compressed_prefix_length = prefix_compression_result.encoded_strings_length[i]; // because chunk.prefix_length is DECOMPRESSED!
//
// 			if (compressed_prefix_length == 0) {
// 				//Write suffix
// 				for (size_t j = checkpoint_info.checkpoint_string_index; j < stop_index; j++) {
// 					*suffix_writer++ = static_cast<uint8_t>(compressed_prefix_length); // writes 0
//
// 					for (size_t k = 0; k < suffix_compression_result.encoded_strings_length[j]; k++) {
// 						*suffix_writer++ = suffix_compression_result.encoded_strings[j][k];
// 					}
// 				}
// 			} else {
// 				//Write prefix
// 				if (prefix_writer + compressed_prefix_length >= current_block.suffix_data_area) { // If doesn't fit throw error
// 					std::cerr << "Prefix doesnt fit in prefix area\n"; // Should we just have a hard limit on 64 kb per string? How to avoid this error? Split string on multiple blocks?
// 					throw std::logic_error("Block Data Capacity Exceeded");
// 				}
//
// 				for (size_t k = 0; k < compressed_prefix_length; k++) {
// 					*prefix_writer++ = prefix_compression_result.encoded_strings[i][k];
// 				}
//
// 				//Write suffix
// 				*suffix_writer++ = static_cast<uint8_t>(compressed_prefix_length); // prefix_length overhead
// 				uint16_t jumpback_offset = suffix_writer - (prefix_writer - 1); // jumpback offset overhead. minus one because prefix_writer incremented after the write
// 				memcpy(suffix_writer, &jumpback_offset, sizeof(jumpback_offset));
// 				suffix_writer += sizeof(jumpback_offset);
//
// 				for (size_t j = checkpoint_info.checkpoint_string_index; j < stop_index; j++) {
// 					for (size_t k = 0; k < suffix_compression_result.encoded_strings_length[j]; k++) {
// 						*suffix_writer++ = suffix_compression_result.encoded_strings[j][k];
// 					}
// 				}
// 			}
// 		}
//
// 		fsst_plus_compression_result.compressed_blocks.push_back(current_block);
//
// 		total_compressed_byte_size += (prefix_writer - current_block.prefix_data_area) + (suffix_writer - current_block.suffix_data_area);
// 		total_compressed_byte_size += calc_symbol_table_size(prefix_compression_result.encoder);
// 		total_compressed_byte_size += calc_symbol_table_size(suffix_compression_result.encoder);
// 		checkpoint_info = new_checkpoint_info;
// 	}
// 	return total_compressed_byte_size;
// }

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
			std::cout << "RESULT: ";
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
			std::cout << "RESULT: ";
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


	// *** calculate the size for the next block to insert ***

	size_t suffix_area_start_index = {0}; // start index for this block into all suffixes (stored in suffix_compression_result)
	size_t prefix_area_start_index = {0}; // start index for this block into all prefixes (stored in prefix_compression_result)

	size_t suffix_n_in_block = 0; // number of strings inside the block
	uint16_t suffix_offset_current = 0;
	uint16_t suffix_offsets_from_first_suffix[config::compressed_block_granularity];
	uint8_t suffix_encoded_prefix_lengths[config::compressed_block_granularity]; // the length of the prefix for suffix i
	uint8_t suffix_prefix_index[config::compressed_block_granularity]; // the index of the prefix for suffix i

	size_t prefix_n_in_block = 0; // number of strings inside the block
	size_t prefix_last_index_added = UINT64_MAX;
	size_t prefix_area_size = 0;
	uint16_t prefix_offsets_from_first_prefix[config::compressed_block_granularity];

	size_t block_size = {0};

	// Each blocks starts with the num_strings (uint8_t) and the base offset (uint8_t)
	size_t initial_block_size = sizeof(uint8_t) + sizeof(uint8_t);
	block_size += initial_block_size;

	while (suffix_n_in_block < config::compressed_block_granularity) {
		const size_t suffix_index = suffix_area_start_index + suffix_n_in_block;
		size_t prefix_index_for_suffix = find_similarity_chunk_corresponding_to_index(suffix_index, similarity_chunks);
		const SimilarityChunk &chunk = similarity_chunks[prefix_index_for_suffix];
		const bool suffix_has_prefix = chunk.prefix_length != 0;

		if (prefix_index_for_suffix != prefix_last_index_added) {
			// we add a new prefix to our block. The block grows only by the size of the encoded prefix
			const size_t prefix_size =  prefix_compression_result.encoded_strings_length[prefix_index_for_suffix];
			const size_t block_size_with_prefix = block_size + prefix_size;

			// exit loop if we can't fit prefix
			if (block_size_with_prefix >= config::compressed_block_byte_capacity) {
				break;
			}

			std::cout << "Add Prefix " << prefix_n_in_block << " Length=" << prefix_size << '\n';

			// increase the number of prefixes for this block, add the size to the block size
			prefix_offsets_from_first_prefix[prefix_n_in_block] = prefix_area_size;
			prefix_n_in_block += 1;
			prefix_area_size += prefix_size;
			block_size += prefix_size;


			// make sure we don't add it again
			prefix_last_index_added = prefix_index_for_suffix;
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
		if (block_size + suffix_total_size + suffix_block_header_size >= config::compressed_block_byte_capacity) {
			break;
		}
		// store the string offset
		suffix_offsets_from_first_suffix[suffix_n_in_block] = suffix_offset_current;
		suffix_encoded_prefix_lengths[suffix_n_in_block] = prefix_compression_result.encoded_strings_length[prefix_index_for_suffix]; // the ENCODED prefix length
		suffix_prefix_index[suffix_n_in_block] = prefix_n_in_block - 1; // as we already increased prefix_n_in_block before
		suffix_offset_current += suffix_total_size;

		// we can add the suffix
		block_size += suffix_total_size;
		suffix_n_in_block += 1;
	}

	std::cout << "N Strings: " << suffix_n_in_block << " N Prefixes: " << prefix_n_in_block << " Block size: " <<block_size << " Pre size: " <<prefix_area_size << std::endl;

	// *** WRITE THE GROUP NOW WHERE WE NOW HOW MANY STRINGS TO PUT IN ***
	uint8_t* current_data_ptr = compression_result.data;

	// A) WRITE THE HEADER

	// A 1) Write the number of strings as an uint_8
	Store<uint8_t>(suffix_n_in_block, current_data_ptr);
	current_data_ptr += sizeof(uint8_t);
	// A 2) Write the string_offsets[]
	for (size_t i = 0; i < suffix_n_in_block; i++) {
		uint16_t offset_array_size_to_go = (suffix_n_in_block-i) * sizeof(uint16_t);
		uint16_t string_offset = prefix_area_size + offset_array_size_to_go + suffix_offsets_from_first_suffix[i];
		Store<uint16_t>(string_offset, current_data_ptr);
		current_data_ptr += sizeof(uint16_t);
	}

	// B) WRITE THE PREFIX AREA
	for (size_t i = 0; i < prefix_n_in_block; i++) {
		const size_t prefix_index = prefix_area_start_index + i;
		const size_t prefix_length = prefix_compression_result.encoded_strings_length[prefix_index];

		std::cout << "Write Prefix " << i << " Length=" << prefix_length << '\n';
		const unsigned char * prefix_start = prefix_compression_result.encoded_strings[prefix_index];
		memcpy(current_data_ptr, prefix_start, prefix_length);
		current_data_ptr += prefix_length;
	}

	// C) WRITE SUFFIX AREA
	for (size_t i = 0; i < suffix_n_in_block; i ++){
		const size_t suffix_index = suffix_area_start_index + i;

		uint8_t prefix_index_for_suffix = suffix_prefix_index[suffix_index];
		uint8_t suffix_prefix_length = suffix_encoded_prefix_lengths[suffix_index];
		const bool suffix_has_prefix = suffix_prefix_length != 0;

		// write the length of the prefix, can be zero
		Store<uint8_t>(suffix_prefix_length, current_data_ptr);
		current_data_ptr += sizeof(uint8_t);

		// if there is a prefix, calculate offset and store it
		if (suffix_has_prefix) {
			size_t prefix_offset_from_first_prefix = prefix_offsets_from_first_prefix[prefix_index_for_suffix];
			size_t suffix_offset_from_first_suffix = suffix_offsets_from_first_suffix[suffix_index];
			uint16_t prefix_jumpback_offset = (prefix_area_size - prefix_offset_from_first_prefix) + suffix_offset_from_first_suffix;

			Store<uint16_t>(prefix_jumpback_offset, current_data_ptr);
			current_data_ptr += sizeof(uint16_t);
		}

		// write the suffix
		const size_t suffix_length = suffix_compression_result.encoded_strings_length[suffix_index];
		const unsigned char * suffix_start = suffix_compression_result.encoded_strings[suffix_index];
		memcpy(current_data_ptr, suffix_start, suffix_length);
		current_data_ptr += suffix_length;
	}
	std::cout << block_size << "\n";

	//DECOMPRESS TO CHECK
	decompress(compression_result.data, fsst_decoder(prefix_compression_result.encoder), fsst_decoder(suffix_compression_result.encoder));


	// compression_result.run_start_offsets = {nullptr}; // Placeholder
	//
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
