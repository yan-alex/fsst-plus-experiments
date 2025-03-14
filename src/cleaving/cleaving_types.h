#pragma once

struct SimilarityChunk {
    size_t start_index; // Starts here and goes on until next chunk's index, or until the end of the 128 block
    size_t prefix_length;
};
