#include "fsst/fsst.h"
#include <vector>

struct CompressionResult {
    fsst_encoder_t* encoder;
    virtual ~CompressionResult() = 0; 
};

// Define the pure virtual destructor outside the class
inline CompressionResult::~CompressionResult() {}

struct PrefixCompressionResult : CompressionResult {
    std::vector<unsigned char*> encoded_prefixes; 
};

struct SuffixData {
    uint8_t prefix_length; // uint8_t = unsigned char = 8 bits
    unsigned char* encoded_suffix;
};

struct SuffixDataWithPrefix: SuffixData {
    unsigned short jumpback_offset; // unsigned short = 16 bits
    SuffixDataWithPrefix() = default;

    // Constructor declaration
    SuffixDataWithPrefix(unsigned char prefix_length, unsigned short jumpback_offset, unsigned char* encoded_suffix);
};

// Inline definition of SuffixDataWithPrefix constructor
inline SuffixDataWithPrefix::SuffixDataWithPrefix(unsigned char prefix_length, unsigned short jumpback_offset, unsigned char* encoded_suffix)
: SuffixData{ prefix_length, encoded_suffix }, jumpback_offset(jumpback_offset)
{}

struct SuffixCompressionResult : CompressionResult {
    std::vector<SuffixData> data; 
};

struct SimilarityChunk {
    size_t start_index; // Starts here and goes on until next chunk's index, or until the end of the 128 block
    size_t prefix_length;
};

std::vector<SimilarityChunk> form_similarity_chunks(
    std::vector<size_t>& lenIn,
    std::vector<const unsigned char*>& strIn,
    const size_t start_index);

struct CleavedInputs {
    std::vector<size_t> prefixLenIn;
    std::vector<const unsigned char*> prefixStrIn;
    std::vector<size_t> suffixLenIn;
    std::vector<const unsigned char*> suffixStrIn;
};

struct RunHeader {
    uint8_t base_offset; // 8 bits
    uint8_t num_strings;// 8 bits
    std::vector<uint16_t> string_offsets;
};