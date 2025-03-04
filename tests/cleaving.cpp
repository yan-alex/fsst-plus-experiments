#include <vector>
#include <string>
#include <iostream>
#include <cassert>
#include "../main.h"


// Function to compare expected chunks with actual chunks
void validate_chunks(
    const std::string& test_name,
    const std::vector<SimilarityChunk>& actual_chunks,
    const std::vector<SimilarityChunk>& expected_chunks,
    const std::vector<size_t>& lenIn,
    const size_t start_index)
{
    std::cout << "Validating " << test_name << "...\n";
    // Check if the number of chunks is the same
    assert(actual_chunks.size() == expected_chunks.size() && "Number of chunks does not match expected.");

    // Check each chunk's start index and prefix length
    for (size_t i = 0; i < expected_chunks.size(); ++i) {
        const SimilarityChunk& actual = actual_chunks[i];
        const SimilarityChunk& expected = expected_chunks[i];

        assert(actual.start_index == expected.start_index && "Chunk start index does not match expected.");
        assert(actual.prefix_length == expected.prefix_length && "Chunk prefix length does not match expected.");
    }

    std::cout << test_name << " passed.\n\n";
}

int main() {
    {
        // Test Case 1: Empty Input
        std::string test_name = "Test Case 1: Empty Input";
        std::vector<size_t> lenIn = {};
        std::vector<const unsigned char*> strIn = {};
        size_t start_index = 0;

        std::vector<SimilarityChunk> expected_chunks = {};

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 2: Single String - should have no prefix
        std::string test_name = "Test Case 2: Single String - should have no prefix";
        std::string s = "http://example.com";
        std::vector<size_t> lenIn = { s.size() };
        std::vector<const unsigned char*> strIn = { reinterpret_cast<const unsigned char*>(s.c_str()) };
        size_t start_index = 0;

        size_t prefix_length = 16;
        SimilarityChunk expected_chunk = { 0, 0 };
        std::vector<SimilarityChunk> expected_chunks = { expected_chunk };

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 3: Identical Strings
        std::string test_name = "Test Case 3: Identical Strings";
        std::string s = "http://identical.com/path";
        std::vector<size_t> lenIn = { s.size(), s.size(), s.size() };
        std::vector<const unsigned char*> strIn = {
            reinterpret_cast<const unsigned char*>(s.c_str()),
            reinterpret_cast<const unsigned char*>(s.c_str()),
            reinterpret_cast<const unsigned char*>(s.c_str())
        };
        size_t start_index = 0;

        size_t prefix_length = 24; // Length of the string, aligned to 8
        SimilarityChunk expected_chunk = { 0, 24 };
        std::vector<SimilarityChunk> expected_chunks = { expected_chunk };

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 4: No Common Prefixes
        std::string test_name = "Test Case 4: No Common Prefixes";
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

        // Expected chunks: each string is its own chunk with prefix length 0
        std::vector<SimilarityChunk> expected_chunks = {
            {0, 0}
        };

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 5: Maximum Prefix Length
        std::string test_name = "Test Case 5: Maximum Prefix Length";
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

        // Expected prefix length is 120 (maximum allowed)
        SimilarityChunk expected_chunk = {0, 120};
        std::vector<SimilarityChunk> expected_chunks = {expected_chunk};

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 6: Minimum and Maximum Length Strings
        std::string test_name = "Test Case 6: Minimum and Maximum Length Strings";
        std::vector<std::string> strings = {
            "", // Empty string
            "a", // Single character
            "abcdefghijklmnopqrstuvwxyz", // 26 characters
            std::string(200, 'b') // 200 'b's
        };
        std::vector<size_t> lenIn;
        std::vector<const unsigned char*> strIn;
        for (const auto& s : strings) {
            lenIn.push_back(s.size());
            strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
        }
        size_t start_index = 0;

        // Expected chunks: since the strings have no common prefixes, each is its own chunk
        std::vector<SimilarityChunk> expected_chunks = {
            {0, 0}
        };

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 7: Strings with Prefixes in Multiples of 8
        std::string test_name = "Test Case 7: Strings with Prefixes in Multiples of 8";
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

        // Expected chunks:
        // First two strings have a common prefix of 8
        // Next two strings have a common prefix of 16
        // Last two strings have a common prefix of 24
        std::vector<SimilarityChunk> expected_chunks = {
            {0, 8}, {2, 16}, {4, 24}
        };

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 8: Strings with Varying Prefix Lengths
        std::string test_name = "Test Case 8: Strings with Varying Prefix Lengths";
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

        // Expected chunks:
        // First two strings share a prefix of 16
        // Third string is different
        // Fourth and fifth share a prefix of 16
        // Sixth string is different
        std::vector<SimilarityChunk> expected_chunks = {
            {0, 16}, {2, 0}, {3, 16}, {5, 0}
        };

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);
        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 9: Start Index Offset
        std::string test_name = "Test Case 9: Start Index Offset";
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

        // Expected chunks
        std::vector<SimilarityChunk> expected_chunks = {
            {2, 8}
        };

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);
        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 10: Maximum Number of Strings (128 Strings)
        std::string test_name = "Test Case 10: Maximum Number of Strings (128 Strings)";
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

        // Only the first 128 strings are processed
        // Expected prefix length is 16 ('common_prefix_' is 14 characters)
        SimilarityChunk expected_chunk = {0, 8}; // Because 'common_prefix_' is 14, aligned to 8 is 8
        std::vector<SimilarityChunk> expected_chunks = {{0, 8}, {100, 16}, {110, 16}, {120, 16}};

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);
        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 11: Strings with Common Prefix Exactly at 120 Characters
        std::string test_name = "Test Case 11: Strings with Common Prefix Exactly at 120 Characters";
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

        // Expected prefix length is 120
        SimilarityChunk expected_chunk = {0, 120};
        std::vector<SimilarityChunk> expected_chunks = {expected_chunk};

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        // Test Case 12: Strings with Non-ASCII Characters
        std::string test_name = "Test Case 12: Strings with Non-ASCII Characters";
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

        // The first two strings share a prefix
        // Let's calculate the common prefix in bytes
        size_t lcp = 0;
        const char* s1 = strings[0].c_str();
        const char* s2 = strings[1].c_str();
        size_t max_lcp = std::min(strings[0].size(), strings[1].size());
        while (lcp < max_lcp && s1[lcp] == s2[lcp]) {
            ++lcp;
        }
        size_t prefix_length = lcp - (lcp % 8); // Align to multiple of 8

        std::vector<SimilarityChunk> expected_chunks = {
            {0, prefix_length},
            {2, 0}
        };

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);

        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {
        std::string test_name = "Test Case 15: Chose a more specific prefix instead of a general one";
        std::vector<std::string> strings;
        strings.push_back("FSSTPLUSAAAAAAAA");
        strings.push_back("FSSTPLUSAAAAAAAA");
        strings.push_back("FSSTPLUSBBBBBBBB");
        strings.push_back("FSSTPLUSBBBBBBBB");
        strings.push_back("FSSTPLUSCCCCCCCC");
        /*
        * Option A: (63 bytes)
        * FSSTPLUS
        * ___AAAAAAAA
        * ___AAAAAAAA
        * ___BBBBBBBB
        * ___BBBBBBBB
        * ___CCCCCCCC
        *
        * Option B: (61 bytes, correct one)
        * FSSTPLUSAAAAAAAA
        * FSSTPLUSBBBBBBBB
        * ___
        * ___
        * ___
        * ___
        * _FSSTPLUSCCCCCCCC
        */

        std::vector<size_t> lenIn;
        std::vector<const unsigned char*> strIn;
        for (const auto& s : strings) {
            lenIn.push_back(s.size());
            strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
        }
        size_t start_index = 0;


        std::vector<SimilarityChunk> expected_chunks = {{0, 16},{2, 16}, {4, 0}};

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);
        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    {

        std::string test_name = "Test Case 13: Chose a more general prefix instead of specific ones";
        std::vector<std::string> strings;
        strings.push_back("FSSTPLUSAAAAAAAA");
        strings.push_back("FSSTPLUSAAAAAAAA");
        strings.push_back("FSSTPLUSBBBBBBBB");
        strings.push_back("FSSTPLUSBBBBBBBB");
        strings.push_back("FSSTPLUSCCCCCCCC");
        strings.push_back("FSSTPLUSDDDDDDDD");
        /*
         * Option A: (74 bytes), correct one
         * FSSTPLUS
         * ___AAAAAAAA
         * ___AAAAAAAA
         * ___BBBBBBBB
         * ___BBBBBBBB
         * ___CCCCCCCC
         * ___DDDDDDDD
         *
         * Option B: (78 bytes)
         * FSSTPLUSAAAAAAAA
         * FSSTPLUSBBBBBBBB
         * ___
         * ___
         * ___
         * ___
         * _FSSTPLUSCCCCCCCC
         * _FSSTPLUSDDDDDDDD
         */


        std::vector<size_t> lenIn;
        std::vector<const unsigned char*> strIn;
        for (const auto& s : strings) {
            lenIn.push_back(s.size());
            strIn.push_back(reinterpret_cast<const unsigned char*>(s.c_str()));
        }
        size_t start_index = 0;


        std::vector<SimilarityChunk> expected_chunks = {{0, 8}};

        auto actual_chunks = form_similarity_chunks(lenIn, strIn, start_index);
        // Validate
        validate_chunks(test_name, actual_chunks, expected_chunks, lenIn, start_index);
    }

    std::cout << "All tests passed successfully.\n";
    return 0;
}
