#include "duckdb.hpp"
#include "fsst.h"
#include <iostream>

using namespace duckdb;

int main() {
	DuckDB db(nullptr);
	Connection con(db);
	const auto result = con.Query("SELECT \"Case Number\" FROM read_csv('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/Chicago Crimes 2012-2017.csv', strict_mode=False);");

	// Vectors to store string lengths and pointers
	std::vector<size_t> lenIn;
	std::vector<const unsigned char*> strIn;

	auto next_chunk = result->Fetch();
	while (next_chunk) {
		// to get the number of values within the chunk, we can use the size() method of the chunk
		const auto chunk_size = next_chunk->size();

		auto &vector = next_chunk->data[0];
		auto vector_data = FlatVector::GetData<string_t>(vector);


		// Populate lenIn and strIn
		for (size_t i = 0; i < chunk_size; i++) {
			string_t str = vector_data[i];
			lenIn.push_back(str.GetSize());
			strIn.push_back(reinterpret_cast<const unsigned char*>(str.GetDataUnsafe()));
		}
		break;
		// get the next chunk, start loop again
		next_chunk = result->Fetch();
	}

	// Create FSST encoder
	int zeroTerminated = 0; // DuckDB strings are not zero-terminated
	fsst_encoder_t* encoder = fsst_create(
		lenIn.size(),        // Number of strings
		lenIn.data(),        // Array of string lengths
		strIn.data(),        // Array of string pointers
		zeroTerminated       // Not zero-terminated
	);

	// (Optional: Use encoder for compression here)
	std::cout << "I am alive";

	// Cleanup
	fsst_destroy(encoder);


}
