from pathlib import Path, PurePath
import os
import math
import duckdb
import tempfile
import bz2
import subprocess

# Define custom temp directory on /bigstore
TEMP_DIR = "/bigstore/yan/temp_fsst"
# Create temp directory if it doesn't exist
os.makedirs(TEMP_DIR, exist_ok=True)

def get_compression_method(df, column_name):
    temp_db_path = os.path.join(TEMP_DIR, "duckdb_compression_choice.db")
    con = duckdb.connect(temp_db_path)
    con.execute("PRAGMA force_compression='Auto'")
    temp_df = df.select(column_name)
    con.register("temp_view", temp_df)
    con.execute("CREATE TABLE temp_table AS SELECT * FROM temp_view")
    con.execute("CHECKPOINT")
    con.execute("ANALYZE temp_table")
    storage_info = con.execute("PRAGMA storage_info('temp_table')").fetchall()
    con.execute("DROP TABLE IF EXISTS temp_table")
    con.unregister("temp_view")
    con.close()
  
    print(f"Column info: {storage_info[0]}")
    if storage_info[0][1] == column_name:
        compression_type = storage_info[0][8]
        return compression_type

    return None

def refine_dataset(input_path: PurePath, output_path: PurePath):
    print(f"\n\n ⚗️ ⚗️  Refining {str(input_path)} -> {str(output_path)} ⚗️ ⚗️")
    
    """Process a single data file using the refinement logic"""
    # Determine file type and read accordingly
    file_ext = input_path.suffix.lower()
    is_bz2 = input_path.name.endswith('.csv.bz2')
    temp_decompressed_path = None
    
    try:
        if file_ext == '.parquet':
            df = pl.read_parquet(str(input_path))
            duckdb_reader_clause = f"read_parquet('{str(input_path)}')"
        elif file_ext == '.csv':
            df = pl.read_csv(str(input_path), ignore_errors=True, truncate_ragged_lines=True)
            duckdb_reader_clause = f"read_csv_auto('{str(input_path)}', header=true, ignore_errors=true)"
        elif is_bz2:
            try:
                print(f"🔍 Reading & Decompressing {str(input_path)}")
                # Create a temporary file for the decompressed output
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, dir=TEMP_DIR, suffix='.csv') as temp_f_out:
                    temp_decompressed_path = temp_f_out.name
                    with bz2.open(input_path, "rb") as f_in:
                        temp_f_out.write(f_in.read())
                
                print(f"📊 Reading decompressed file: {temp_decompressed_path}")
                # Add ignore_errors=True to handle parsing issues
                df = pl.read_csv(temp_decompressed_path, ignore_errors=True, truncate_ragged_lines=True) # Read the decompressed CSV with Polars
                # Use the decompressed path for DuckDB as well
                duckdb_reader_clause = f"read_csv_auto('{temp_decompressed_path}', header=true, ignore_errors=true)" 

            except Exception as e:
                # Clean up the temporary file if an error occurs during reading
                if temp_decompressed_path and os.path.exists(temp_decompressed_path):
                    try:
                        os.remove(temp_decompressed_path)
                        print(f"🧹 Cleaned up temporary file due to error: {temp_decompressed_path}")
                    except OSError as cleanup_e:
                        print(f"⚠️ Error cleaning up temporary file {temp_decompressed_path} after error: {cleanup_e}")
                print(f"🚨 Failed to read DataFrame from {str(input_path)}: {e}. Skipping refinement.")
                return False
                raise Exception(f"‼️‼️ Error reading/decompressing {str(input_path)}: {e}")
                
        elif file_ext == '.json': # Keep JSON logic if needed, but mark as potentially unhandled by DuckDB section
            df = pl.read_json(str(input_path))
            duckdb_reader_clause = None # JSON not handled by DuckDB reader logic below
        else:
            print(f"⚠️ Unsupported file format: {file_ext}")
            return False

        if df is None: # Check if df loading failed for any reason
            print(f"🚨 Failed to read DataFrame from {str(input_path)}. Skipping refinement.")
            return False
        
        if not duckdb_reader_clause:
            print(f"⚠️ DuckDB reader clause not set for {str(input_path)} (format: {file_ext}). Skipping DuckDB checks.")
            # Decide how to proceed if DuckDB checks are skipped. Maybe return False or only use Polars info?
            # For now, let's skip the file if we can't use DuckDB.
            return False

        # Text column selection criteria
        text_columns = []
        
        # Create temporary DuckDB database for dictionary size calculation
        # temp_db_path = os.path.join(TEMP_DIR, f"temp_db_{os.path.basename(input_path)}_{os.getpid()}.db")
        con = duckdb.connect()
        
        for col in df.columns:
            try:
                if (df[col].dtype != pl.String and # Not a string column
                    df[col].null_count() >= df.height / 2 and # More than 50% non-null
                    df[col].str.len_chars().mean() <= 5): # Avg length <= 5
                    print(f"Skipping column '{col}")
                    continue
            except Exception as e:
                print(f"🚨⏭️ ERROR, SKIPPING. Error processing column: col:'{col}', error: {e}")
                continue

            # Check if the column is actually a string type in DuckDB to prevent type conversion issues
            check_type_query = f"SELECT typeof(\"{col}\") FROM {duckdb_reader_clause} LIMIT 1"
            col_type = con.execute(check_type_query).fetchone()[0].lower()
            if not ('varchar' in col_type or 'string' in col_type or 'text' in col_type):
                print(f"Skipping column '{col}' with type '{col_type}' - not a string column")
                continue
            
            # Register the data in DuckDB and rename the column to match the query
            con.execute(f"CREATE OR REPLACE TABLE dictionary_sizer_table AS SELECT \"{col}\" AS THISCOL FROM {duckdb_reader_clause}")
            
            # Calculate dictionary encoding statistics
            dict_query = """
            SELECT length(string_agg(DISTINCT THISCOL)) as dict_size,  -- Size of dictionary (bytes needed to store all unique strings)
                    COUNT(DISTINCT THISCOL) as dist,  -- Number of distinct values
                    CASE WHEN COUNT(DISTINCT THISCOL) = 0 THEN 0 
                         ELSE ceil(log2(COUNT(DISTINCT THISCOL)) / 8) 
                    END as size_of_code,  -- Bytes needed per dictionary reference (log2 of cardinality)
                    
                    CASE WHEN COUNT(DISTINCT THISCOL) = 0 THEN 0 
                         ELSE COUNT(THISCOL) * ceil(log2(COUNT(DISTINCT THISCOL)) / 8) 
                    END as codes_size,  -- Total size of all dictionary references
                    
                    CASE WHEN COUNT(DISTINCT THISCOL) = 0 
                        THEN CAST(length(string_agg(DISTINCT THISCOL)) as BIGINT) 
                        ELSE CAST(length(string_agg(DISTINCT THISCOL)) + COUNT(THISCOL) * ceil(log2(COUNT(DISTINCT THISCOL)) / 8) as BIGINT) 
                    END as total_compressed_size,  -- Dictionary size + references size
                    
                    format_bytes(CASE WHEN COUNT(DISTINCT THISCOL) = 0 
                        THEN CAST(length(string_agg(DISTINCT THISCOL)) as BIGINT) 
                        ELSE CAST(length(string_agg(DISTINCT THISCOL)) + COUNT(THISCOL) * ceil(log2(COUNT(DISTINCT THISCOL)) / 8) as BIGINT) 
                    END) as formatted  -- Human-readable format of total compressed size
            FROM dictionary_sizer_table
            """
            dict_result = con.execute(dict_query).fetchall()[0]
            dict_size = dict_result[4]  # total_compressed_size
            if dict_size is None:
                print(f"⚠️ No dictionary size for column '{col}'. Skipping.")
                continue
            
            # Run FSST compression using our C++ program
            # try:
            print(f"Running FSST compression on column '{col}'")
            # Determine the input path for the C++ program
            cpp_input_path = temp_decompressed_path if is_bz2 and temp_decompressed_path else str(input_path)
            # Ensure input_path is passed to the C++ program, not the temp path
            cmd = ["/export/scratch2/home/yla/fsst-plus-experiments/build/compress_w_basic_fsst", cpp_input_path, col]
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            # Parse FSST compressed size from output
            fsst_size = None
            if result.stdout is not None:
                for line in result.stdout.split('\n'):
                    if line.startswith("FSST_COMPRESSED_SIZE="):
                        fsst_size = int(line.split('=')[1])
                        break
            else:
                print(f"‼️‼️ No stdout from FSST compression for column '{col}'. Error: {result.stderr}")
            
            # Compare sizes and decide whether to include the column
            if fsst_size is not None:
                if fsst_size <= dict_size * 2:
                    text_columns.append(col)
                    print(f"👍 Column '{col}' passes filter: FSST size ({fsst_size}) is not 2x bigger than dictionary size ({dict_size}). Including.")
                else:
                    print(f"👎 Column '{col}' filtered out: FSST size ({fsst_size}) is 2x bigger than dictionary size ({dict_size}). Excluding.")
            else:
                print(f"🚨⏭️ ERROR, SKIPPING. Could not determine FSST size for column '{col}'. Output was:\n {result.stderr}")
                continue
                raise Exception(f"‼️‼️ Could not determine FSST size for column '{col}'. Output was:\n {result.stderr}")

            # # Clean up temp file
            # os.remove(temp_parquet)
        
        # Close DuckDB connection and remove temp database
        con.close()
        # if os.path.exists(temp_db_path):
        #     os.remove(temp_db_path)
        
        # Only write the file if there are text columns that meet our criteria
        if text_columns:
            # Ensure the parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            df_text = df.select(text_columns)
            df_text.write_parquet(str(output_path))
            print(f"✅ Processed {str(input_path)} -> {str(output_path)}, with columns: {text_columns}")

            return True
        else:
            print(f"⏭️ No columns found in {str(input_path)} that match the criteria. Skipping file.")
            return False

    finally:
        # Clean up the temporary decompressed file if it was created
        if temp_decompressed_path and os.path.exists(temp_decompressed_path):
            try:
                os.remove(temp_decompressed_path)
                print(f"🧹 Cleaned up temporary file: {temp_decompressed_path}")
            except OSError as e:
                print(f"⚠️ Error cleaning up temporary file {temp_decompressed_path}: {e}")

def process_raw_directory(raw_dir: str = "../../data/raw", output_dir: str = "../../data/refined"):
    """Process all Parquet files in the raw directory and its subdirectories"""
    raw_path = Path(raw_dir)
    output_path = Path(output_dir)
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Process all supported data files recursively
    processed_count = 0
    error_count = 0
    
    # Find all matching files with supported extensions
    supported_files = []
    supported_files.extend(raw_path.glob("**/*.parquet"))
    supported_files.extend(raw_path.glob("**/*.csv"))
    supported_files.extend(raw_path.glob("**/*.csv.bz2"))
    
    # supported_files.extend(raw_path.glob("**/*.json"))
    
    for data_file in supported_files:
        # Preserve directory structure
        relative_path = data_file.relative_to(raw_path)
        # Always save as parquet regardless of input format
        output_file = output_path / relative_path.parent / (relative_path.stem.replace('.csv', '') + '.parquet')
        
        # Skip if the output file already exists
        if output_file.exists():
            print(f"✅ Already refined {str(data_file)}, file already exists at {str(output_file)}")
            continue
        
        found_columns = refine_dataset(data_file, output_file)
        if found_columns:
            processed_count += 1
        else:
            error_count += 1
    
    print(f" 🌐 Macrodata Refinement Complete 🌐 \n {processed_count} files matched the criteria, {error_count} files didn't have compatible columns")

if __name__ == "__main__":
    process_raw_directory() 