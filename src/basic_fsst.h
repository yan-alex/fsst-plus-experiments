#ifndef BASIC_FSST_H
#define BASIC_FSST_H

#include "duckdb.hpp"
#include <string>

// Declaration for the function that runs basic FSST compression and prints its results, using the provided DuckDB connection, parquet file path, and limit.
void run_basic_fsst(duckdb::Connection &con, const std::string &parquetPath, size_t limit);
fsst_encoder_t* create_encoder(const std::vector<size_t>& lenIn, std::vector<const unsigned char*>& strIn);
size_t calc_symbol_table_size(fsst_encoder_t* encoder);
#endif // BASIC_FSST_H 