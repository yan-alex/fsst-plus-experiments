#pragma once
#include <cstddef>
#include <string>

namespace global {
    size_t global_index = 0;

    std::string dataset = "";
    std::string column = "";
    std::string algo = "";

    size_t amount_of_rows = 0;
    double run_time_ms = 0;
    double compression_factor = 0;
}
