#include "duckdb.hpp"
#include <iostream>

using namespace duckdb;

int main() {

	// to install, run "git submodule update --init --recursive" to get the submodules
	// then run "cd duckdb && make" to build duckdb

	DuckDB db(nullptr);

	Connection con(db);

	con.Query("CREATE TABLE data(i int32, v VARCHAR);");
	con.Query("INSERT INTO data VALUES (42, 'hello'), (43, 'world'), (44, 'duckdb');");
	const auto result = con.Query("SELECT * FROM data;");
	auto next_chunk = result->Fetch();

	while (next_chunk) {

		// to get the number of values within the chunk, we can use the size() method of the chunk
		const auto chunk_size = next_chunk->size();

		// get the first chunk via a reference. A reference is indicated by the &. The reference means
		// that the variable first_vector is not a copy of the data, but a reference to the data, similar to a pointer.
		auto &first_vector = next_chunk->data[0];
		// get the data of the first chunk. Here it is now important what is the type specification. In
		// create table integers(i int32) we have defined the column i as INTEGER, which is a 32-bit signed integer.
		auto first_vector_data = FlatVector::GetData<int32_t>(first_vector);

		// now we can iterate over the data of the first chunk
		for (size_t i = 0; i < chunk_size; i++) {
			// print the value of the first chunk
			std::cout << "Vector 1, Row " << i << ": " << first_vector_data[i] << std::endl;
		}

		// the second vector is a string, so we need to use the string data type. the string_t type
		// is a duckdb internal type which has some extra optimizations.
		auto &second_vector = next_chunk->data[1];
		auto second_vector_data = FlatVector::GetData<string_t>(second_vector);

		for (size_t i = 0; i < chunk_size; i++) {
			// get a cpp string from the duckdb string_t
			std::string str = second_vector_data[i].GetString();
			// print the value of the second chunk
			std::cout << "Vector 2, Row " << i << ": " << str << std::endl;
		}

		// get the next chunk, start loop again
		next_chunk = result->Fetch();
	}

	// example 2: read example/otp.csv, repleace the path with your own path
	const auto csv_result = con.Query("SELECT * FROM read_csv_auto('/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments/example_data/otp.csv') LIMIT 10;");
	auto csv_next_chunk = csv_result->Fetch();
	while (csv_next_chunk) {
		csv_next_chunk->Print();
		csv_next_chunk = csv_result->Fetch();
	}
}
