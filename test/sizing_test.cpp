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