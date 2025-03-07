#ifndef CLEAVING_TYPES_H
#define CLEAVING_TYPES_H

struct SimilarityChunk {
    size_t start_index;   // Starts here and goes on until next chunk's index, or until the end of the 128 block
    size_t prefix_length;
};

#endif // CLEAVING_TYPES_H 