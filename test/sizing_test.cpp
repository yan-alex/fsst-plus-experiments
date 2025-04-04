#include <catch2/catch_test_macros.hpp>
#include "block_sizer.h" // Includes block_types.h implicitly which defines BlockSizingMetadata and config
#include "block_types.h" // Explicit include just in case for clarity or if block_sizer changes
namespace config {
    constexpr size_t total_strings = 100000; // # of input strings
    constexpr size_t block_byte_capacity = UINT16_MAX; // ~64KB capacity
}

TEST_CASE("CanFitInBlock()", "[block_sizer]") {
    constexpr size_t capacity = config::block_byte_capacity;
    constexpr size_t block_header_suffix_offset = sizeof(uint16_t); // Space needed for one suffix offset

    SECTION("Block is empty") {
        BlockSizingMetadata bsm; // block_size defaults to 0
        REQUIRE(bsm.block_size == 0);
        REQUIRE(CanFitInBlock(bsm, 0)); // Adding 0 to empty block
        REQUIRE(CanFitInBlock(bsm, 100)); // Adding 100 to empty block
        // Max additional size that fits in an empty block
        REQUIRE(CanFitInBlock(bsm, capacity - block_header_suffix_offset - 1));
        // Exactly full (considering header offset) doesn't fit due to '<' comparison
        REQUIRE_FALSE(CanFitInBlock(bsm, capacity - block_header_suffix_offset));
        // Exceeding capacity
        REQUIRE_FALSE(CanFitInBlock(bsm, capacity));
    }

    SECTION("Block is partially filled") {
        BlockSizingMetadata bsm;
        bsm.block_size = capacity / 2;
        REQUIRE(CanFitInBlock(bsm, 0)); // Adding 0
        REQUIRE(CanFitInBlock(bsm, 100)); // Adding 100
        // Max additional size that fits: capacity - bsm.block_size - header_offset_size - 1
        REQUIRE(CanFitInBlock(bsm, capacity - bsm.block_size - block_header_suffix_offset - 1));
        // Exactly full doesn't fit: capacity - bsm.block_size - header_offset_size
        REQUIRE_FALSE(CanFitInBlock(bsm, capacity - bsm.block_size - block_header_suffix_offset));
        // Exceeding capacity
        REQUIRE_FALSE(CanFitInBlock(bsm, capacity - bsm.block_size));
    }

    SECTION("Block is almost full") {
        BlockSizingMetadata bsm;
        // Leave 10 bytes before considering the header offset
        bsm.block_size = capacity - block_header_suffix_offset - 10;
        REQUIRE(CanFitInBlock(bsm, 0)); // Adding 0
        REQUIRE(CanFitInBlock(bsm, 9));  // Fits (total usage = capacity - 1)
        REQUIRE_FALSE(CanFitInBlock(bsm, 10)); // Doesn't fit (total usage = capacity)
        REQUIRE_FALSE(CanFitInBlock(bsm, 11)); // Doesn't fit (total usage > capacity)
    }

     SECTION("Block size calculation edge cases near capacity") {
        BlockSizingMetadata bsm;

        // Edge case: current size leaves exactly enough space for header offset + 0 additional bytes
        bsm.block_size = capacity - block_header_suffix_offset;
        REQUIRE_FALSE(CanFitInBlock(bsm, 0)); // Adding 0 makes total usage = capacity, which fails '<'
        REQUIRE_FALSE(CanFitInBlock(bsm, 1)); // Adding 1 exceeds capacity

        // Edge case: current size leaves just less than needed for header offset
        bsm.block_size = capacity - block_header_suffix_offset + 1;
        REQUIRE_FALSE(CanFitInBlock(bsm, 0));

        // Edge case: block is already full or overflown
         bsm.block_size = capacity;
         REQUIRE_FALSE(CanFitInBlock(bsm, 0));

         bsm.block_size = capacity + 100;
         REQUIRE_FALSE(CanFitInBlock(bsm, 0));
    }
}

TEST_CASE("CalculateSuffixPlusHeaderSize()", "[block_sizer]") {
    // Mock FSSTCompressionResult with different encoded string lengths
    FSSTCompressionResult mock_compression_result;
    mock_compression_result.encoded_string_lengths = {5, 10, 15, 20, 25};
    
    SECTION("Suffix without prefix") {
        // Create a similarity chunk without a prefix (prefix_length = 0)
        std::vector<SimilarityChunk> mock_chunks = {
            {0, 0}  // start_index = 0, prefix_length = 0
        };
        
        // Calculate expected size: suffix_encoded_length + prefix_length_byte (no jumpback)
        constexpr size_t prefix_length_byte = sizeof(uint8_t);
        constexpr size_t jumpback_size = 0;  // No prefix, so no jumpback

        // Test for each encoded string length
        for (size_t i = 0; i < mock_compression_result.encoded_string_lengths.size(); i++) {
            size_t expected_size = mock_compression_result.encoded_string_lengths[i] + prefix_length_byte + jumpback_size;
            size_t actual_size = CalculateSuffixPlusHeaderSize(mock_compression_result, mock_chunks, i);
            REQUIRE(actual_size == expected_size);
        }
    }
    
    SECTION("Suffix with prefix") {
        // Create a similarity chunk with a prefix (prefix_length > 0)
        std::vector<SimilarityChunk> mock_chunks = {
            {0, 5}  // start_index = 0, prefix_length = 5
        };
        
        // Calculate expected size: suffix_encoded_length + prefix_length_byte + jumpback_size
        constexpr size_t prefix_length_byte = sizeof(uint8_t);
        constexpr size_t jumpback_size = sizeof(uint16_t);  // With prefix, so has jumpback
        
        // Test for each encoded string length
        for (size_t i = 0; i < mock_compression_result.encoded_string_lengths.size(); i++) {
            size_t expected_size = mock_compression_result.encoded_string_lengths[i] + prefix_length_byte + jumpback_size;
            size_t actual_size = CalculateSuffixPlusHeaderSize(mock_compression_result, mock_chunks, i);
            REQUIRE(actual_size == expected_size);
        }
    }

}

TEST_CASE("Basic tests CalculateBlockSizeAndPopulateWritingMetadata()", "[block_sizer]") {
    SECTION("Basic test") {
        FSSTCompressionResult prefix_compression_result;
        FSSTCompressionResult suffix_compression_result;
        std::vector<SimilarityChunk> similarity_chunks;
        BlockWritingMetadata wm;

        suffix_compression_result.encoded_string_lengths = {10, 10, 10, 10, 10};
        suffix_compression_result.encoded_string_ptrs.resize(5);

        prefix_compression_result.encoded_string_lengths = {10, 10, 10, 10, 10};
        prefix_compression_result.encoded_string_ptrs.resize(5);

        similarity_chunks = {
            {0, 10},
            {1, 10},
            {2, 10},
            {3, 10},
            {4, 10},
        };
        size_t block_size = CalculateBlockSizeAndPopulateWritingMetadata(
            similarity_chunks,
            prefix_compression_result,
            suffix_compression_result,
            wm,
            0
        );
        size_t expected_size = sizeof(uint8_t) + // num_strings
                               sizeof(uint16_t) * 5 + // suffix data area offsets
                               5*10 + // encoded prefixes
                               5* (sizeof(uint8_t) + sizeof(uint16_t) + 10); // suffix data areas

        REQUIRE(block_size == expected_size);

        REQUIRE(wm.number_of_prefixes == 5);
        REQUIRE(wm.number_of_suffixes == 5);
        REQUIRE(wm.prefix_area_size == 5*10);
        REQUIRE(wm.suffix_area_size == 5* (sizeof(uint8_t) + sizeof(uint16_t) + 10));
        
        REQUIRE(wm.prefix_offsets_from_first_prefix[0] == 0);
        REQUIRE(wm.prefix_offsets_from_first_prefix[1] == 10);
        REQUIRE(wm.prefix_offsets_from_first_prefix[2] == 20);
        REQUIRE(wm.prefix_offsets_from_first_prefix[3] == 30);
        REQUIRE(wm.prefix_offsets_from_first_prefix[4] == 40);

        REQUIRE(wm.suffix_offsets_from_first_suffix[0] == 0);
        REQUIRE(wm.suffix_offsets_from_first_suffix[1] == sizeof(uint8_t) + sizeof(uint16_t) + 10);
        REQUIRE(wm.suffix_offsets_from_first_suffix[2] == 2*(sizeof(uint8_t) + sizeof(uint16_t) + 10));
        REQUIRE(wm.suffix_offsets_from_first_suffix[3] == 3*(sizeof(uint8_t) + sizeof(uint16_t) + 10));
        REQUIRE(wm.suffix_offsets_from_first_suffix[4] == 4*(sizeof(uint8_t) + sizeof(uint16_t) + 10));
    }
}

TEST_CASE("Large scale CalculateBlockSizeAndPopulateWritingMetadata()", "[block_sizer]") {
    SECTION("Test with 1000 strings, block only takes 128") {
        // Create compression results for a large number of strings
        constexpr size_t num_strings = 1000;
        FSSTCompressionResult prefix_compression_result;
        FSSTCompressionResult suffix_compression_result;
        std::vector<SimilarityChunk> similarity_chunks;
        BlockWritingMetadata wm;

        // Set up encoded strings with varying lengths to test block capacity limits
        prefix_compression_result.encoded_string_ptrs.resize(num_strings);
        suffix_compression_result.encoded_string_lengths.resize(num_strings);
        suffix_compression_result.encoded_string_ptrs.resize(num_strings);

        // Create patterns in the data to test different scenarios
        for (size_t i = 0; i < num_strings; i++) {
            // Create strings with varying sizes (5-15 bytes)
            suffix_compression_result.encoded_string_lengths[i] = 5 + (i % 11);
            
            // Create similarity chunks with different patterns
            // Every 100 strings share the same prefix
            if (i % 100 == 0) {
                similarity_chunks.push_back({i, 8}) ;
                prefix_compression_result.encoded_string_lengths.push_back( 5 + (i % 11));
            }
        }

        // Calculate block size and populate writing metadata
        size_t block_size = CalculateBlockSizeAndPopulateWritingMetadata(
            similarity_chunks,
            prefix_compression_result,
            suffix_compression_result,
            wm,
            0
        );

        REQUIRE(block_size < config::block_byte_capacity);

        REQUIRE(wm.number_of_suffixes == 128);

        REQUIRE(wm.number_of_prefixes == 2);

        // Verify prefix and suffix area sizes are consistent with the encoded string lengths
        size_t expected_prefix_area_size = (5 + (0 % 11)) + 5 + (100 % 11);

        REQUIRE(wm.prefix_area_size == expected_prefix_area_size);

        size_t expected_suffix_area_size = 0;
        for (size_t i = 0; i < 128; i++) {
            expected_suffix_area_size += (sizeof(uint8_t) + suffix_compression_result.encoded_string_lengths[i]);
        }
        expected_suffix_area_size += 128*sizeof(uint16_t); //prefix jumpbacks

        REQUIRE(wm.suffix_area_size == expected_suffix_area_size);
    }
}
