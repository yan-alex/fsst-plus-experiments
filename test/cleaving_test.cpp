#include <vector>
#include <string>
#include <iostream>
#include <cassert>
#include "cleaving.h"
#include <catch2/catch_test_macros.hpp>

// Function to compare expected chunks with actual chunks
// void validate_chunks(...) -> No longer needed with Catch2 assertions

// int main() { ... } -> Replaced by Catch2 TEST_CASE blocks

TEST_CASE("Test Case 1: Empty Input", "[cleaving]") {
    std::vector<size_t> lenIn = {};
    std::vector<const unsigned char*> strIn = {};
    size_t start_index = 0;

    std::vector<SimilarityChunk> expected_chunks = {};

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    // No chunks to compare elements for
}

TEST_CASE("Test Case 2: Single String", "[cleaving]") {
    std::string s = "http://example.com";
    std::vector<size_t> lenIn = { s.size() };
    std::vector<const unsigned char*> strIn = { reinterpret_cast<const unsigned char*>(s.c_str()) };
    size_t start_index = 0;

    size_t prefix_length = 16; // Not used in expected
    SimilarityChunk expected_chunk = { 0, 0 };
    std::vector<SimilarityChunk> expected_chunks = { expected_chunk };

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 3: Identical Strings", "[cleaving]") {
    std::string s = "http://identical.com/path";
    std::vector<size_t> lenIn = { s.size(), s.size(), s.size() };
    std::vector<const unsigned char*> strIn = {
        reinterpret_cast<const unsigned char*>(s.c_str()),
        reinterpret_cast<const unsigned char*>(s.c_str()),
        reinterpret_cast<const unsigned char*>(s.c_str())
    };
    size_t start_index = 0;

    size_t prefix_length = 24; // Length of the string, aligned to 8 -> Used in expected
    SimilarityChunk expected_chunk = { 0, 24 };
    std::vector<SimilarityChunk> expected_chunks = { expected_chunk };

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 4: No Common Prefixes", "[cleaving]") {
    std::vector<std::string> strings = {
        "apple",
        "banana",
        "cherry",
        "date"
    };
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    std::vector<SimilarityChunk> expected_chunks = {
        {0, 0}
    };

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 5: Maximum Prefix Length", "[cleaving]") {
    std::string base = std::string(130, 'a'); // 130 'a's
    std::vector<std::string> strings = {
        base + "1",
        base + "2",
        base + "3"
    };
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    SimilarityChunk expected_chunk = {0, 120};
    std::vector<SimilarityChunk> expected_chunks = {expected_chunk};

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 6: Minimum and Maximum Length Strings", "[cleaving]") {
    std::vector<std::string> strings = {
        "", // Empty string
        "a", // Single character
        "abcdefghijklmnopqrstuvwxyz", // 26 characters
        std::string(100, 'b') // 100 'b's
    };
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    std::vector<SimilarityChunk> expected_chunks = {
        {0, 0}
    };

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 7: Strings with Prefixes in Multiples of 8", "[cleaving]") {
    std::vector<std::string> strings = {
        "prefix08_suffix1",
        "prefix08_suffix2",
        "prefix16_common_suffix3",
        "prefix16_common_suffix4",
        "prefix24_common_long_suffix5",
        "prefix24_common_long_suffix6"
    };
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    std::vector<SimilarityChunk> expected_chunks = {
        {0, 8}, {2, 16}, {4, 24}
    };

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 8: Strings with Varying Prefix Lengths", "[cleaving]") {
    std::vector<std::string> strings = {
        "common_prefix_abcdefgh1",
        "common_prefix_abcdefgh2",
        "different_string_xyz",
        "common_prefix_abcxyz1",
        "common_prefix_abcxyz2",
        "another_different_string"
    };
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    std::vector<SimilarityChunk> expected_chunks = {
        {0, 16}, {2, 0}, {3, 16}, {5, 0}
    };

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 9: Start Index Offset", "[cleaving]") {
    std::vector<std::string> all_strings = {
        "string000",
        "string001",
        "string002",
        "string003",
        "string004",
        "string005"
    };
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : all_strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 2; // Start processing from "string2"
    size_t num_elements = strIn.size() - start_index; // Process remaining elements

    std::vector<SimilarityChunk> expected_chunks = {
        {2, 8}
    };

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, num_elements);

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 10: Maximum Number of Strings (128 Strings)", "[cleaving]") {
    std::vector<std::string> strings;
    for (size_t i = 0; i < 130; ++i) { // 130 strings
        strings.push_back("common_prefix_" + std::to_string(i));
    }
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    // Expected prefix length is 8 ('common_prefix_' is 14 characters, aligned down to 8)
    // The logic might form multiple chunks based on how digits change the bytes
    std::vector<SimilarityChunk> expected_chunks = {{0, 8}, {100, 16}, {110, 16}, {120, 16}}; // Adjusted expectation based on original test

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 11: Strings with Common Prefix Exactly at 120 Characters", "[cleaving]") {
    std::string common_prefix = std::string(120, 'x');
    std::vector<std::string> strings = {
        common_prefix + "suffix1",
        common_prefix + "suffix2"
    };
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    SimilarityChunk expected_chunk = {0, 120};
    std::vector<SimilarityChunk> expected_chunks = {expected_chunk};

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 12: Strings with Non-ASCII Characters", "[cleaving]") {
    std::vector<std::string> strings = {
        u8"префикс_общий_1", // Russian characters
        u8"префикс_общий_2",
        u8"другой_строка"
    };
    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size()); // UTF-8 encoding, size() gives the byte length
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    size_t lcp = 0;
    const char* s1 = strings[0].c_str();
    const char* s2 = strings[1].c_str();
    size_t max_len = std::min(strings[0].size(), strings[1].size());
    while (lcp < max_len && s1[lcp] == s2[lcp]) {
        ++lcp;
    }
    size_t prefix_length = lcp - (lcp % 8); // Align down to multiple of 8

    std::vector<SimilarityChunk> expected_chunks = {
        {0, prefix_length},
        {2, 0}
    };

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}


TEST_CASE("Test Case 15: Chose a more specific prefix instead of a general one", "[cleaving]") {
    std::vector<std::string> strings;
    strings.push_back("FSSTPLUSAAAAAAAA");
    strings.push_back("FSSTPLUSAAAAAAAA");
    strings.push_back("FSSTPLUSBBBBBBBB");
    strings.push_back("FSSTPLUSBBBBBBBB");
    strings.push_back("FSSTPLUSCCCCCCCC");

    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    std::vector<SimilarityChunk> expected_chunks = {{0, 16}, {2, 16}, {4, 0}};

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

TEST_CASE("Test Case 13: Chose a more general prefix instead of specific ones", "[cleaving]") {
     std::vector<std::string> strings;
    strings.push_back("FSSTPLUSAAAAAAAA");
    strings.push_back("FSSTPLUSAAAAAAAA");
    strings.push_back("FSSTPLUSBBBBBBBB");
    strings.push_back("FSSTPLUSBBBBBBBB");
    strings.push_back("FSSTPLUSCCCCCCCC");
    strings.push_back("FSSTPLUSDDDDDDDD");

    std::vector<size_t> lenIn;
    std::vector<const unsigned char*> strIn;
    for (const auto& s : strings) {
        lenIn.push_back(s.size());
        strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
    }
    size_t start_index = 0;

    std::vector<SimilarityChunk> expected_chunks = {{0, 8}};

    auto actual_chunks = FormSimilarityChunks(lenIn, strIn, start_index, strIn.size());

    REQUIRE(actual_chunks.size() == expected_chunks.size());
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        REQUIRE(actual_chunks[i].start_index == expected_chunks[i].start_index);
        REQUIRE(actual_chunks[i].prefix_length == expected_chunks[i].prefix_length);
    }
}

// Remove the final success message and return statement
// std::cout << "All tests passed successfully.
// ";
// return 0;
