#include <vector>
#include <iostream>
#include <cassert>
#include "cleaving_types.h"
#include "generic_utils.h"

int main() {

    {
        // Test Case 1: Single chunk
        std::vector<SimilarityChunk> chunks1 = {{0, 10}};
        assert(FindSimilarityChunkCorrespondingToIndex(0, chunks1) == 0);
        assert(FindSimilarityChunkCorrespondingToIndex(5, chunks1) == 0);
        assert(FindSimilarityChunkCorrespondingToIndex(99, chunks1) == 0); // Target index greater than start index
         // Note: Testing target index before the first chunk's start index
        std::vector<SimilarityChunk> chunks1b = {{10, 5}};

        std::cout << "Test Case 1 Passed: Single chunk." << std::endl;
    }

    {
        // Test Case 2: Multiple chunks, target within various ranges
        std::vector<SimilarityChunk> chunks2 = {{0, 10}, {10, 5}, {25, 8}, {30, 12}};
        // Target exactly at chunk starts
        assert(FindSimilarityChunkCorrespondingToIndex(0, chunks2) == 0);
        assert(FindSimilarityChunkCorrespondingToIndex(10, chunks2) == 1);
        assert(FindSimilarityChunkCorrespondingToIndex(25, chunks2) == 2);
        assert(FindSimilarityChunkCorrespondingToIndex(30, chunks2) == 3); // Last chunk start
        // Target within chunks
        assert(FindSimilarityChunkCorrespondingToIndex(5, chunks2) == 0);
        assert(FindSimilarityChunkCorrespondingToIndex(15, chunks2) == 1);
        assert(FindSimilarityChunkCorrespondingToIndex(28, chunks2) == 2);
        assert(FindSimilarityChunkCorrespondingToIndex(35, chunks2) == 3); // Within last chunk
        // Target just before next chunk start
        assert(FindSimilarityChunkCorrespondingToIndex(9, chunks2) == 0);
        assert(FindSimilarityChunkCorrespondingToIndex(24, chunks2) == 1);
        assert(FindSimilarityChunkCorrespondingToIndex(29, chunks2) == 2);
        std::cout << "Test Case 2 Passed: Multiple chunks, various targets." << std::endl;

    }

    {
        // Test Case 3: Large index within the last chunk
        std::vector<SimilarityChunk> chunks3 = {{0, 10}, {100, 5}, {200, 8}};
        assert(FindSimilarityChunkCorrespondingToIndex(500, chunks3) == 2);
         std::cout << "Test Case 3 Passed: Large index in last chunk." << std::endl;
    }

     {
        // Test Case 4: Chunks with gaps
        std::vector<SimilarityChunk> chunks4 = {{10, 5}, {30, 8}, {50, 12}};
         assert(FindSimilarityChunkCorrespondingToIndex(10, chunks4) == 0);
         assert(FindSimilarityChunkCorrespondingToIndex(20, chunks4) == 0); // In the 'gap' before chunk 1
         assert(FindSimilarityChunkCorrespondingToIndex(29, chunks4) == 0); // Just before chunk 1
         assert(FindSimilarityChunkCorrespondingToIndex(30, chunks4) == 1);
         assert(FindSimilarityChunkCorrespondingToIndex(45, chunks4) == 1); // In the 'gap' before chunk 2
         assert(FindSimilarityChunkCorrespondingToIndex(49, chunks4) == 1); // Just before chunk 2
         assert(FindSimilarityChunkCorrespondingToIndex(50, chunks4) == 2);
         assert(FindSimilarityChunkCorrespondingToIndex(100, chunks4) == 2); // Within last chunk
         std::cout << "Test Case 4 Passed: Chunks with gaps." << std::endl;
    }

    std::cout << "All tests passed successfully.\n";
    return 0;
}
