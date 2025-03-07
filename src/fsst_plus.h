
struct SuffixData {
    uint8_t prefix_length; // uint8_t = unsigned char = 8 bits
    unsigned char* encoded_suffix;

    bool hasPrefix() const {
        return prefix_length > 0;
    }
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


struct RunHeader {
    uint8_t base_offset; // 8 bits
    uint8_t num_strings;// 8 bits
    std::vector<uint16_t> string_offsets;
};

struct CompressedBlock {
    RunHeader header;
    uint8_t* prefix_data_area;
    uint8_t* suffix_data_area;
};

struct FSSTPlusCompressionResult {
    uint64_t* run_start_offsets;
    std::vector<CompressedBlock> compressed_blocks;
};

void print_compression_stats(size_t total_strings_amount, size_t total_string_size, size_t total_compressed_string_size);