#include "config.h"
#include "env.h"
#include "duckdb.hpp"
#include <iostream>
#include <fstream>
#include <ranges>
#include "fsst_plus.h"
#include "cleaving.h"
#include "basic_fsst.h"
#include "duckdb_utils.h"
#include <string>
#include "block_writer.h"
#include "block_decompressor.h"
#include "cleaving_types.h"
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <queue>
#include <condition_variable>

namespace config {
    constexpr size_t total_strings = 100000; // # of input strings
    constexpr bool print_sorted_corpus = false;
    constexpr bool print_split_points = false; // prints compressed corpus displaying split points
    constexpr bool print_similarity_chunks = false;
    constexpr bool print_decompressed_corpus = false;
}


uint8_t *FindBlockStart(uint8_t *block_start_offsets, const int i) {
    uint8_t *offset_ptr = block_start_offsets + (i * sizeof(uint32_t));
    const uint32_t offset = Load<uint32_t>(offset_ptr);
    uint8_t *block_start =  offset_ptr + offset;
    return block_start;
}

void DecompressAll(uint8_t *global_header, const fsst_decoder_t &prefix_decoder,
const fsst_decoder_t &suffix_decoder,
const std::vector<size_t> &lengths_original,
const std::vector<const unsigned char *> &string_ptrs_original,
Metadata &metadata
) {
    metadata.global_index = 0; // Reset global index before decompression
    uint16_t num_blocks = Load<uint16_t>(global_header);
    uint8_t *block_start_offsets = global_header + sizeof(uint16_t);
    for (int i = 0; i < num_blocks; ++i) {
        const uint8_t *block_start = FindBlockStart(block_start_offsets, i);
        /*
         * Block stop is next block's start. Note that this also works for the last block, so no over-read,
         * as we save an "extra" data_end_offset, pointing to where the last block stops. This is needed to
         * calculate the length
         */
        const uint8_t *block_stop = FindBlockStart(block_start_offsets, i + 1);

        DecompressBlock(block_start, prefix_decoder, suffix_decoder, block_stop, lengths_original, string_ptrs_original, metadata);
    }
    std::cout << "Decompression verified\n";
}


std::vector<SimilarityChunk> FormBlockwiseSimilarityChunks(const size_t &n, StringCollection &input, const size_t &block_granularity) {
    std::vector<SimilarityChunk> similarity_chunks;
    similarity_chunks.reserve(n);

    // Figure out the optimal split points (similarity chunks)
    for (size_t i = 0; i < n; i += block_granularity) {
        const size_t cleaving_run_n = std::min(input.lengths.size() - i, block_granularity);

        // std::cout << "Current Cleaving Run coverage: " << i << ":" << i + cleaving_run_n - 1 << std::endl;

        TruncatedSort(input.lengths, input.string_ptrs, i, cleaving_run_n);

        const std::vector<SimilarityChunk> cleaving_run_similarity_chunks = FormSimilarityChunks(
            input.lengths, input.string_ptrs, i, cleaving_run_n);
        similarity_chunks.insert(similarity_chunks.end(),
                                 cleaving_run_similarity_chunks.begin(),
                                 cleaving_run_similarity_chunks.end());
    }
    return similarity_chunks;
}



FSSTPlusCompressionResult FSSTPlusCompress(const size_t n, std::vector<SimilarityChunk> similarity_chunks, CleavedResult cleaved_result, const size_t &block_granularity) {
    FSSTPlusCompressionResult compression_result{};

    FSSTCompressionResult prefix_compression_result = FSSTCompress(cleaved_result.prefixes);
    compression_result.prefix_encoder = prefix_compression_result.encoder;

    FSSTCompressionResult suffix_compression_result = FSSTCompress(cleaved_result.suffixes);
    compression_result.suffix_encoder = suffix_compression_result.encoder;

    // Allocate the maximum size possible for the corpus
    size_t max_size = CalcMaxFSSTPlusDataSize(prefix_compression_result,suffix_compression_result);
    compression_result.data_start = new uint8_t[max_size];

    // std::cout << "Data should start at: " << static_cast<void*>(compression_result.data_start) << '\n';
    // std::cout << "and end at: " << static_cast<void*>(compression_result.data_start + max_size) << '\n';

    /*
     *  >>> WRITE GLOBAL HEADER <<<
     *
     * To write num_blocks, we must know how many blocks we have. But first let's
     * calculate the size of each block (giving us also the number of blocks),
     * allowing us to write block_start_offsets[] and data_end_offset also.
     */

     FSSTPlusSizingResult sizing_result = SizeEverything(n, similarity_chunks, prefix_compression_result, suffix_compression_result, block_granularity);


    uint8_t* global_header_ptr = compression_result.data_start;

    // Now we can write!

    // A) write num_blocks
    size_t n_blocks = sizing_result.block_sizes_pfx_summed.size();
    Store<uint16_t>(n_blocks ,global_header_ptr);
    global_header_ptr+=sizeof(uint16_t);

    // B) write block_start_offsets[]
    for (size_t i = 0; i < n_blocks; i++) {
        size_t offsets_to_go = (n_blocks - i); // count itself, so that the "base" begins at the offset's end
        size_t global_header_size_ahead =
                offsets_to_go * sizeof(uint32_t)
                + sizeof(uint32_t); // data_end_offset size
        const size_t total_block_size_ahead =  i == 0 ? 0 : sizing_result.block_sizes_pfx_summed[i-1];

        Store<uint32_t>(global_header_size_ahead + total_block_size_ahead, global_header_ptr);
        global_header_ptr +=sizeof(uint32_t);
    }

    // C) write data_end_offset
    Store<uint32_t>(sizing_result.block_sizes_pfx_summed.back() + sizeof(uint32_t) // count itself, so that the "base" begins at the offset's end
                    ,global_header_ptr);
    global_header_ptr +=sizeof(uint32_t);

    uint8_t* next_block_start_ptr = global_header_ptr;

    //  >>> WRITE BLOCKS <<<
    for (size_t i = 0; i < sizing_result.wms.size(); i++) {
        // use metadata to write correctly

        // std::cout << "wm.prefix_area_size: " << sizing_result.wms[i].prefix_area_size << "\n";

        // std::cout << "\n🧱 Block " << std::setw(3) << i << " start: " << static_cast<void*>(next_block_start_ptr) << '\n';
        next_block_start_ptr = WriteBlock(next_block_start_ptr, prefix_compression_result, suffix_compression_result, sizing_result.wms[i]);
    }

    // Cleanup
    free(prefix_compression_result.output_buffer);
    free(suffix_compression_result.output_buffer);

    compression_result.data_end = next_block_start_ptr;
    return compression_result;
}

bool CreateResultsTable(Connection &con) {
    // Begin transaction
    con.Query("BEGIN TRANSACTION");

    // Create a results table to store benchmarks
    const string create_results_table =
            "CREATE TABLE results ("
            "path VARCHAR, "
            "dataset VARCHAR, "
            "col_name VARCHAR, "
            "algo VARCHAR, "
            "amount_of_rows BIGINT, "
            "run_time_ms DOUBLE, "
            "compression_factor DOUBLE, "
            "num_strings BIGINT, "
            "original_size BIGINT"
            ");";

    try {
        con.Query(create_results_table);

        // Commit the transaction to persist the table
        con.Query("COMMIT");
    } catch (std::exception& e) {
        con.Query("ROLLBACK");
        std::cerr << "Failed to create results table: " << e.what() << std::endl;
        return false;
    }

    // Verify the table was created (after commit)
    auto verify_result = con.Query("SHOW TABLES");
    auto verify_chunk = verify_result->Fetch();
    bool found_results_table = false;

    while (verify_chunk) {
        // Only access columns that exist
        size_t num_cols = verify_chunk->data.size();
        if (num_cols == 0) {
            std::cerr << "SHOW TABLES returned no columns" << std::endl;
            break;
        }

        // Use the first column as it contains the table name in this case
        auto table_names = duckdb::FlatVector::GetData<duckdb::string_t>(verify_chunk->data[0]);
        for (size_t i = 0; i < verify_chunk->size(); i++) {
            std::string table_name = table_names[i].GetString();
            std::cout << " - " << table_name << std::endl;
            if (table_name == "results") {
                found_results_table = true;
            }
        }
        verify_chunk = verify_result->Fetch();
    }

    if (!found_results_table) {
        std::cerr << "Results table was not created successfully" << std::endl;
        return false;
    }

    std::cout << "Results table created successfully" << std::endl;

    return found_results_table;
}

vector<string> FindDatasets(Connection &con, const string &data_dir) {
    vector<string> datasets;
    const auto files_result = con.Query("SELECT file FROM glob('" + data_dir + "/**/*.parquet')");
    try {
        auto files_chunk = files_result->Fetch();
        while (files_chunk) {
            auto file_names = duckdb::FlatVector::GetData<duckdb::string_t>(files_chunk->data[0]);
            for (size_t i = 0; i < files_chunk->size(); i++) {
                datasets.push_back(file_names[i].GetString());
            }
            files_chunk = files_result->Fetch();
        }
    } catch (std::exception& e) {
        std::cerr << "Failed to list parquet files: " << e.what() << std::endl;
        throw std::logic_error("Failed to list parquet files");
    }
    return datasets;
}

vector<string> GetColumnNames(const unique_ptr<MaterializedQueryResult> &columns_result) {
    auto columns_chunk = columns_result->Fetch();
    vector<string> column_names;
    while (columns_chunk) {
        auto column_data = duckdb::FlatVector::GetData<duckdb::string_t>(columns_chunk->data[0]);
        for (size_t i = 0; i < columns_chunk->size(); i++) {
            column_names.push_back(column_data[i].GetString());
        }
        columns_chunk = columns_result->Fetch();
    }
    return column_names;
}

bool ColumnIsStringType(Connection &con, const string &column_name) {
    const auto column_type_result = con.Query("SELECT data_type FROM duckdb_columns() WHERE table_name = 'temp_view' AND column_name = '" + column_name + "'");
    const auto column_type_chunk = column_type_result->Fetch();
    if (column_type_chunk) {
        const string type = duckdb::FlatVector::GetData<duckdb::string_t>(column_type_chunk->data[0])[0].GetString();
        if (type.find("VARCHAR") == string::npos && type.find("STRING") == string::npos) {
            std::cerr << "Skipping non-string column: " << column_name << " (type: " << type << ")" << std::endl;
            return false;
        }
    }
    return true;
}

void RunFSSTPlus(Connection &con, const size_t &block_granularity, Metadata &metadata, const size_t &n, StringCollection &input, const size_t &total_string_size) {
    // Start timing
    auto start_time = std::chrono::high_resolution_clock::now();

    const std::vector<SimilarityChunk> similarity_chunks = FormBlockwiseSimilarityChunks(n, input, block_granularity);

    const CleavedResult cleaved_result = Cleave(input.lengths, input.string_ptrs, similarity_chunks, n);
    if (config::print_similarity_chunks) {
        std::cout << "🤓 Similarity Chunks 🤓\n";
        for (int i = 0; i < similarity_chunks.size(); ++i) {
            std::cout
                    // << "i:" << std::setw(6) << i
                    << " start_index: " << std::setw(6)<< similarity_chunks[i].start_index << " prefix_length: " << std::setw(3) <<similarity_chunks[i].prefix_length
                    << " PREFIX: " << cleaved_result.prefixes.string_ptrs[i] << "\n";
        }
    }
    const FSSTPlusCompressionResult compression_result = FSSTPlusCompress(n, similarity_chunks, cleaved_result, block_granularity);

    // End timing
    auto end_time = std::chrono::high_resolution_clock::now();

    // decompress to check all went well
    DecompressAll(compression_result.data_start, fsst_decoder(compression_result.prefix_encoder),
                  fsst_decoder(compression_result.suffix_encoder), input.lengths, input.string_ptrs, metadata);


    metadata.run_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    size_t compressed_size = compression_result.data_end - compression_result.data_start;
    compressed_size += CalcSymbolTableSize(compression_result.prefix_encoder);
    compressed_size += CalcSymbolTableSize(compression_result.suffix_encoder);

    // Subtract savings of bitpacking global header block offsets
    uint16_t n_blocks = compression_result.data_start[0];
    size_t size_of_one_offset = ceil(log2(n_blocks) / 8);
    size_t bitpacked_offsets_size = n_blocks * size_of_one_offset;
    size_t non_bitpacked_offsets_size = n_blocks * sizeof(uint32_t);
    size_t savings = non_bitpacked_offsets_size - bitpacked_offsets_size;
    printf("Savings of bitpacking global header block offsets: %zu bytes\n", savings);
    compressed_size -= savings;
    metadata.compression_factor = static_cast<double>(total_string_size) / static_cast<double>(compressed_size);

    PrintCompressionStats(n, total_string_size, compressed_size);

    // Add results to table
    string insert_query = "INSERT INTO results VALUES ('" +
                          metadata.dataset_folders + "', '" +
                          metadata.dataset + "', '" +
                          metadata.column + "', '" +
                          metadata.algo + "', " +
                          std::to_string(metadata.amount_of_rows) + ", " +
                          std::to_string(metadata.run_time_ms) + ", " +
                          std::to_string(metadata.compression_factor) + ", " +
                          std::to_string(n) + ", " +
                          std::to_string(total_string_size) + ");";

    try {
        con.Query(insert_query);
        std::cout << "Inserted result for " << metadata.dataset << "." << metadata.column << std::endl;
    } catch (std::exception& e) {
        std::cerr << "Failed to insert result: " << e.what() << std::endl;
    }

    // Cleanup
    fsst_destroy(compression_result.prefix_encoder);
    fsst_destroy(compression_result.suffix_encoder);
    delete[] compression_result.data_start;
}

bool process_dataset(Connection &con, const size_t &block_granularity, const string &dataset_path, int thread_id) {
    // Extract dataset name from path
    string dataset_folders = dataset_path.substr(0, dataset_path.find_last_of("/"));
    string substring = dataset_path.substr(dataset_path.find_last_of("/") + 1);
    string dataset_name = substring.substr(0, substring.find_last_of('.'));

    // Create a unique temp view name for this thread
    string temp_view_name = "temp_view_" + std::to_string(thread_id);

    std::cout<<"dataset_path: "<<dataset_path << "\n";
    std::cout<<"dataset_name: "<< dataset_name << "\n";

    // Get columns from dataset
    con.Query("CREATE OR REPLACE VIEW " + temp_view_name + " AS SELECT * FROM read_parquet('" + dataset_path + "')");
    auto columns_result = con.Query("SELECT column_name FROM duckdb_columns() WHERE table_name = '" + temp_view_name + "'");
    vector<string> column_names;

    try {
        column_names = GetColumnNames(columns_result); //TODO: Uncomment
        // column_names = {"fromUseravatarUrlMedium"};
    } catch (std::exception& e) {
        std::cerr << "🚨 Error GetColumnNames() with dataset: " << dataset_name << ": " << e.what() << std::endl;
        std::cerr << "Moving on to the next dataset" << std::endl;
        return false;
    }

    // For each column
    for (const auto& column_name : column_names) {
        std::cout << "\n🟡> Processing dataset: " << dataset_name << ", column: " << column_name << std::endl;
        try {
            // Skip this column if it's not string
            if (!ColumnIsStringType(con, column_name)) {
                std::cerr<<"Refined column is not string time. This should not happen as refinement should deal with that. Skipping column.";
                continue;
                // throw std::logic_error("Refined column is not string time. This should not happen as refinement should deal with that. Terminating");
            }

            // Set global variables for tracking
            Metadata metadata{};
            metadata.dataset_folders = dataset_folders;
            metadata.dataset = dataset_name;
            metadata.column = column_name;

            // Query to get column data
            const string query =
                    "SELECT \"" + column_name + "\" FROM read_parquet('" + dataset_path + "')"
                    "LIMIT " + std::to_string(config::total_strings) + ";";

            const auto result = con.Query(query);
            metadata.amount_of_rows = result->RowCount();

            auto data_chunk = result->Fetch();
            if (!data_chunk || data_chunk->size() == 0) {
                std::cout << "No data for column: " << column_name << std::endl;
                continue;
            }

            const size_t n = std::min(config::amount_strings_per_symbol_table, static_cast<size_t>(result->RowCount()));

            StringCollection input = RetrieveData(result, data_chunk, n); // 100k rows

            size_t total_string_size = {0};
            for (const size_t string_length: input.lengths) {
                total_string_size += string_length;
            }

            // std::cout <<"==========START DICTIONARY COMPRESSION=========\n";
            // metadata.algo = "dictionary";
            // RunDictionaryCompression(con, column_name, dataset_path, n, total_string_size, metadata);

            // std::cout <<"==========START BASIC FSST COMPRESSION=========\n";
            // metadata.algo = "basic_fsst";
            // RunBasicFSST(con, input, total_string_size, metadata);

            std::cout <<"==========START FSST PLUS COMPRESSION==========\n";
            metadata.algo = "fsstplus_twost";
            RunFSSTPlus(con, block_granularity, metadata, n, input, total_string_size);

            // Now run FSST Plus
            std::cout <<"==========START FSST PLUS COMPRESSION==========\n";
            metadata.algo = "fsst_plus";

            RunFSSTPlus(con, block_granularity, metadata, n, input, total_string_size);
        } catch (std::exception& e) {
            std::cerr << "🚨 Error processing column" << dataset_name << "." << column_name << ": " << e.what() << std::endl;
            std::cerr << "Moving on to the next column" << std::endl;
        }
    }
    
    // Clean up the temp view when done
    con.Query("DROP VIEW IF EXISTS " + temp_view_name);
    return true;
}

// Thread-safe queue for distributing datasets to worker threads
class ThreadSafeQueue {
private:
    std::queue<string> queue;
    std::mutex mutex;
    std::condition_variable cv;
    bool done = false;

public:
    void push(const string& item) {
        std::unique_lock<std::mutex> lock(mutex);
        queue.push(item);
        cv.notify_one();
    }

    bool pop(string& item) {
        std::unique_lock<std::mutex> lock(mutex);
        cv.wait(lock, [this] { return !queue.empty() || done; });
        
        if (queue.empty() && done) {
            return false;
        }
        
        item = queue.front();
        queue.pop();
        return true;
    }
    
    void finish() {
        std::unique_lock<std::mutex> lock(mutex);
        done = true;
        cv.notify_all();
    }
};

// Thread worker function
void worker_thread(int thread_id, ThreadSafeQueue& queue, DuckDB& db, const size_t& block_granularity) {
    // Create a connection for this thread
    Connection con(db);
    
    string dataset_path;
    while (queue.pop(dataset_path)) {
        try {
            process_dataset(con, block_granularity, dataset_path, thread_id);
        } catch (std::exception& e) {
            std::cerr << "Thread " << thread_id << " error processing dataset " << dataset_path << ": " << e.what() << std::endl;
        }
    }
}

void save_results(Connection &con) {
    // Make sure we can access the results table
    auto verify_result2 = con.Query("SHOW TABLES");
    auto verify_chunk2 = verify_result2->Fetch();
    bool found_results_table2 = false;

    std::cout << "Available tables before saving:" << std::endl;
    while (verify_chunk2) {
        // Only access columns that exist
        size_t num_cols = verify_chunk2->data.size();
        if (num_cols == 0) {
            std::cerr << "SHOW TABLES returned no columns" << std::endl;
            break;
        }

        // Use the first column as it contains the table name in this case
        auto table_names = duckdb::FlatVector::GetData<duckdb::string_t>(verify_chunk2->data[0]);
        for (size_t i = 0; i < verify_chunk2->size(); i++) {
            std::string table_name = table_names[i].GetString();
            std::cout << " - " << table_name << std::endl;
            if (table_name == "results") {
                found_results_table2 = true;
            }
        }
        verify_chunk2 = verify_result2->Fetch();
    }

    if (!found_results_table2) {
        throw std::runtime_error("Results table not found before saving");
    }

    // Check how many results we have before saving
    auto count_result = con.Query("SELECT COUNT(*) FROM results");
    auto count_chunk = count_result->Fetch();
    size_t result_count = 0;
    if (count_chunk) {
        result_count = duckdb::FlatVector::GetData<int64_t>(count_chunk->data[0])[0];
        std::cout << "Number of benchmark results: " << result_count << std::endl;
    }

    if (result_count == 0) {
        std::cout << "Warning: No results to save!" << std::endl;
        return;
    }

    std::string filename = "resultsv2bitpacked_";
    std::string algo = "fsstplus_twost";
    // Save results to parquet file
    string save_query = "COPY results TO '" + env::project_dir + "/benchmarking/results/" + filename + algo + ".parquet' (FORMAT 'parquet', OVERWRITE TRUE)";
    con.Query(save_query);

    // Verify the file was created
    std::ifstream file_check((env::project_dir + "/benchmarking/results/" + filename + algo + ".parquet").c_str());
    if (file_check.good()) {
        std::cout << "Results saved to " << env::project_dir << "/benchmarking/results/" + filename + algo + ".parquet" << std::endl;
    } else {
        std::cerr << "Warning: Results file not found after save operation" << std::endl;
    }
}

int main() {
    // Define project directory
    // string project_dir = "/export/scratch2/home/yla/fsst-plus-experiments";
    // string project_dir = "~/fsst-plus-experiments/";
    // string project_dir = "/Users/yanlannaalexandre/_DA_REPOS/fsst-plus-experiments";

    // Create a persistent database connection
    string db_path = env::project_dir + "/benchmarking/results/benchmark.db";
    
    // Ensure the data directory exists using system commands
    system(("mkdir -p " + env::project_dir + "/benchmarking/data").c_str());
    
    // Remove any existing database file to start fresh
    system(("rm -f " + db_path).c_str());
    
    DuckDB db(db_path);
    Connection con(db);
    
    if (!CreateResultsTable(con)) return 1;

    string data_dir = env::project_dir + "/benchmarking/data/refined";

    vector<string> datasets = FindDatasets(con, data_dir);
    
    constexpr size_t block_granularity = 128;
    constexpr int num_threads = 192;

    // Create a thread-safe queue for distributing work
    ThreadSafeQueue dataset_queue;
    
    for (const auto& dataset_path : datasets) { //TODO: Uncomment
        dataset_queue.push(dataset_path);
    }

    // dataset_queue.push(env::project_dir + "/benchmarking/data/refined/NextiaJD/freecodecamp_casual_chatroom.parquet");
    // dataset_queue.push(env::project_dir + "/benchmarking/data/refined/NextiaJD/glassdoor.parquet");
    // dataset_queue.push(env::project_dir + "/benchmarking/data/refined/NextiaJD/glassdoor_photos.parquet");
    // dataset_queue.push(env::project_dir + "/benchmarking/data/refined/NextiaJD/github_issues.parquet");
    // dataset_queue.push(env::project_dir + "/benchmarking/data/refined/clickbench.parquet");


    
    // Create worker threads
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back(worker_thread, i, std::ref(dataset_queue), std::ref(db), std::ref(block_granularity));
    }
    
    // Signal that no more items will be added to the queue
    dataset_queue.finish();
    
    // Wait for all threads to complete
    for (auto& thread : threads) {
        thread.join();
    }
    
    // Save results to parquet file
    try {
        save_results(con);
    } catch (std::exception& e) {
        std::cerr << "Error saving results: " << e.what() << std::endl;
    }
    
    return 0;
}
