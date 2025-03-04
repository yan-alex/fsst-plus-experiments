#include <fsst.h>
#include <ranges>

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


struct RunHeader {
    uint8_t base_offset; // 8 bits
    uint8_t num_strings;// 8 bits
    std::vector<uint16_t> string_offsets;
};