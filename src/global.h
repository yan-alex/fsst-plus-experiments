#pragma once
#include <cstddef>

namespace global {
    size_t global_index = 0;

    string dataset = "";
    string column = "";
    string algo = "";

    size_t amount_of_rows = 0;
    double run_time_ms = 0;
    double compression_factor = 0;
}
