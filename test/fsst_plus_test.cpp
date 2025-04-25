// #include <catch2/catch_test_macros.hpp>
// #include "../src/fsst_plus.h"
// #include "../src/config.h"
// #include "block_sizer.h"
// #include "block_types.h"
//
// namespace test {
//     constexpr size_t block_granularity = 128;
//     constexpr size_t small_block_granularity = 8;
// }
//
//
// TEST_CASE("SizeEverything() Many blocks test", "[block_sizer]") {
//
//
//     SECTION("Test with 100000 strings and small block granularity") {
//         // Params
//         constexpr size_t num_strings = 100000;
//         constexpr size_t prefix_size = 3;
//         constexpr size_t suffix_size = 5;
//
//         FSSTCompressionResult prefix_compression_result;
//         FSSTCompressionResult suffix_compression_result;
//         std::vector<SimilarityChunk> similarity_chunks;
//
//         // Set up encoded strings: Sizes don't need to be large for this test
//         prefix_compression_result.encoded_string_ptrs.resize(num_strings);
//         suffix_compression_result.encoded_string_lengths.resize(num_strings);
//         suffix_compression_result.encoded_string_ptrs.resize(num_strings);
//
//         // Create simple patterns: Make prefixes change every small_block_granularity / 2 strings
//         for (size_t i = 0; i < num_strings; i++) {
//             suffix_compression_result.encoded_string_lengths[i] = suffix_size;
//
//             // Create similarity chunks with frequent prefix changes
//             if (i % (test::small_block_granularity / 2) == 0) {
//                 similarity_chunks.push_back({i, (uint8_t)(2 + (i % prefix_size))}); // Small prefix length (2-4 bytes)
//                 prefix_compression_result.encoded_string_lengths.push_back(prefix_size); // Small prefix size
//             }
//         }
//
//         // Call SizeEverything
//         FSSTPlusSizingResult sizing_result = SizeEverything(
//             num_strings,
//             similarity_chunks,
//             prefix_compression_result,
//             suffix_compression_result,
//             test::small_block_granularity // Use small granularity
//         );
//
//         // Assert a large number of blocks were created
//         // Theoretical max: num_strings / small_block_granularity = 100000 / 8 = 12500
//         // We expect slightly fewer due to block capacity limits, but still many.
//         REQUIRE(sizing_result.wms.size() > 10000);
//         REQUIRE(sizing_result.block_sizes_pfx_summed.size() > 10000);
//         REQUIRE(sizing_result.wms.size() == sizing_result.block_sizes_pfx_summed.size());
//
//         // Assert individual block sizes are within limits and check metadata
//         size_t previous_sum = 0;
//         for (size_t i = 0; i < sizing_result.block_sizes_pfx_summed.size(); ++i) {
//             size_t summed_size = sizing_result.block_sizes_pfx_summed[i];
//             const auto& wm = sizing_result.wms[i];
//
//             size_t current_block_size = summed_size - previous_sum;
//             REQUIRE(current_block_size <= config::block_byte_capacity);
//
//             // Validate BlockWritingMetadata fields
//             // Most blocks should be full due to small sizes
//             // Allow for the last block potentially being smaller
//             bool is_last_block = (i == sizing_result.wms.size() - 1);
//
//             if (!is_last_block) {
//                 REQUIRE(wm.number_of_suffixes == test::small_block_granularity);
//                 // Since prefixes change every 4 strings, a block of 8 suffixes should contain 2 prefixes
//                 REQUIRE(wm.number_of_prefixes == 2);
//                 REQUIRE(wm.prefix_area_size == 2 * prefix_size); // 2 prefixes * prefix_size
//                 // Each suffix takes suffix_size + 1 (prefix_len byte) + 2 (jumpback) = 8 bytes
//                 REQUIRE(wm.suffix_area_size == (suffix_size + 1 + 2) * test::small_block_granularity);
//             } else {
//                 // Last block might have fewer suffixes
//                 REQUIRE(wm.number_of_suffixes <= test::small_block_granularity);
//                 REQUIRE(wm.number_of_prefixes <= 2); // Could be 1 or 2
//                 REQUIRE(wm.prefix_area_size <= 2 * prefix_size);
//                 REQUIRE(wm.suffix_area_size == (suffix_size + 1 + 2) * test::small_block_granularity);
//             }
//
//             // Basic sanity check for offsets vectors (ensure sizes match number of elements)
//             // Note: block_granularity is the *capacity*, not necessarily the number of elements
//             REQUIRE(wm.prefix_offsets_from_first_prefix.size() >= wm.number_of_prefixes);
//             REQUIRE(wm.suffix_offsets_from_first_suffix.size() >= wm.number_of_suffixes);
//             REQUIRE(wm.suffix_encoded_prefix_lengths.size() >= wm.number_of_suffixes);
//             REQUIRE(wm.suffix_prefix_index.size() >= wm.number_of_suffixes);
//
//             previous_sum = summed_size;
//         }
//
//     }
// }
