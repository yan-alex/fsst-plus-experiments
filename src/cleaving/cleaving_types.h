#pragma once
#include <vector>
#include <cstddef> // for size_t

struct SimilarityChunk {
    size_t start_index; // Starts here and goes on until next chunk's index, or until the end of the 128 block
    size_t prefix_length;
};

// Common base struct for Prefixes and Suffixes
struct StringCollection {
    std::vector<size_t> lengths;
    std::vector<const unsigned char *> strings;

    explicit StringCollection(const size_t n) {
        lengths.reserve(n);
        strings.reserve(n);
    }
};

struct Prefixes : StringCollection {
    explicit Prefixes(const size_t n) : StringCollection(n) {}
};

struct Suffixes : StringCollection {
    explicit Suffixes(const size_t n) : StringCollection(n) {}
};

struct CleavedResult {
    Prefixes prefixes;
    Suffixes suffixes;
    
    CleavedResult();
    explicit CleavedResult(const size_t n): suffixes(Suffixes(n)), prefixes(Prefixes(n)){}
};