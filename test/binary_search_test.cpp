// #include <vector>
// // #include <iostream> // No longer needed with Catch2
// // #include <cassert> // No longer needed with Catch2
// #include "cleaving_types.h"
// #include "generic_utils.h"
// #include <catch2/catch_test_macros.hpp>

// // Remove main function
// // int main() {

// TEST_CASE("Test Case 1: Single chunk", "[binary_search]") {
//     std::vector<SimilarityChunk> chunks1 = {{0, 10}};
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(0, chunks1) == 0);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(5, chunks1) == 0);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(99, chunks1) == 0); // Target index greater than start index

//     // Note: Testing target index before the first chunk's start index is tricky.
//     // The current function likely returns the first chunk (index 0) in this case.
//     // If a different behavior is expected (e.g., error or specific index), the test needs adjustment.
//     // std::vector<SimilarityChunk> chunks1b = {{10, 5}};
//     // REQUIRE(FindSimilarityChunkCorrespondingToIndex(5, chunks1b) == ???); // What should this return?
// }

// TEST_CASE("Test Case 2: Multiple chunks, target within various ranges", "[binary_search]") {
//     std::vector<SimilarityChunk> chunks2 = {{0, 10}, {10, 5}, {25, 8}, {30, 12}};
//     // Target exactly at chunk starts
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(0, chunks2) == 0);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(10, chunks2) == 1);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(25, chunks2) == 2);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(30, chunks2) == 3); // Last chunk start
//     // Target within chunks
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(5, chunks2) == 0);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(15, chunks2) == 1);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(28, chunks2) == 2);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(35, chunks2) == 3); // Within last chunk
//     // Target just before next chunk start
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(9, chunks2) == 0);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(24, chunks2) == 1);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(29, chunks2) == 2);
// }

// TEST_CASE("Test Case 3: Large index within the last chunk", "[binary_search]") {
//     std::vector<SimilarityChunk> chunks3 = {{0, 10}, {100, 5}, {200, 8}};
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(500, chunks3) == 2);
// }

// TEST_CASE("Test Case 4: Chunks", "[binary_search]") {
//     std::vector<SimilarityChunk> chunks4 = {{0, 5}, {30, 8}, {50, 12}};
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(10, chunks4) == 0);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(20, chunks4) == 0);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(29, chunks4) == 0);

//     // Targets within the second chunk or its 'boundary'
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(30, chunks4) == 1);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(45, chunks4) == 1);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(49, chunks4) == 1);

//     // Targets within the third (last) chunk or beyond - should return the index of the last chunk (2)
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(50, chunks4) == 2);
//     REQUIRE(FindSimilarityChunkCorrespondingToIndex(100, chunks4) == 2);
// }

// // Remove the final success message and return statement
// // std::cout << "All tests passed successfully.\n";
// // return 0;
// // } // End of removed main function
