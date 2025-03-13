//
// Created by Yan Lanna Alexandre on 07/03/2025.
//
#pragma once
#include "duckdb.hpp"


using namespace duckdb;

inline void extract_strings_from_data_chunk(const unique_ptr<DataChunk> &data_chunk,
                                            std::vector<std::string> &original_strings, std::vector<size_t> &lenIn,
                                            std::vector<const unsigned char *> &strIn) {
    auto &vector = data_chunk->data[0];
    auto vector_data = FlatVector::GetData<string_t>(vector);

    // Populate lenIn and strIn
    for (size_t i = 0; i < data_chunk->size(); i++) {
        std::string str = vector_data[i].GetString();

        original_strings.push_back(str);
        const std::string &stored_str = original_strings.back();
        // Creates a reference to the string that was just added to the vector.
        lenIn.push_back(stored_str.size());
        strIn.push_back(reinterpret_cast<const unsigned char *>(stored_str.c_str()));
        // c_str() returns a pointer to the internal character array, which is a temporary array owned by the string object.
    }
}
