#include "fsst.h"
#include <vector>

struct CompressionResult {
    fsst_decoder_t decoder;
    virtual ~CompressionResult() = 0; 
};

// Define the pure virtual destructor outside the class
inline CompressionResult::~CompressionResult() {}

struct PrefixCompressionResult : CompressionResult {
    std::vector<unsigned char*> encoded_prefixes; 
};

struct SuffixData {
    unsigned char prefix_length; // unsigned char = 8 bits
    unsigned short jumpback_offset; // unsigned short = 16 bits
    std::vector<unsigned char*> encoded_suffix; 
};

struct SuffixCompressionResult : CompressionResult {
    std::vector<SuffixData> data; 
};

struct SimilarityChunk {
    size_t start_index; // Goes until next chunk's index
    size_t prefix_length;
};