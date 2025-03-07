#ifndef CONFIG_H
#define CONFIG_H
#include <ranges>

namespace config {
    extern const size_t cleaving_run_n;      // number of elements per cleaving run.
    extern const size_t max_prefix_size;      // maximum prefix size to scan.
    extern const bool   print_sorted_corpus;  // whether to print sorted strings.
    extern const bool   print_split_points;   // whether to print split points.
}

#endif // CONFIG_H 