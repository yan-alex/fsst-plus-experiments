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

def refine_dataset(input_path: PurePath, output_path: PurePath):
    print(f"\n\n ⚗️ ⚗️  Refining {str(input_path)} -> {str(output_path)} ⚗️ ⚗️")
    
    """Process a single data file using the refinement logic"""
    # Determine file type and read accordingly
    file_ext = input_path.suffix.lower()
    is_bz2 = input_path.name.endswith('.csv.bz2')
    temp_decompressed_path = None
    
    # Create temporary DuckDB database for dictionary size calculation
    con = duckdb.connect()
    
    try:
        relation = None;
        if file_ext == '.parquet':
            relation = con.read_parquet(str(input_path));
        elif file_ext == '.csv':
            relation = con.from_csv_auto(str(input_path), header=True, ignore_errors=True);
        elif is_bz2:
            try:
                print(f"🔍 Reading & Decompressing {str(input_path)}")
                # Create a temporary file for the decompressed output
                with tempfile.NamedTemporaryFile(mode='wb', delete=False, dir=TEMP_DIR, suffix='.csv') as temp_f_out:
                    temp_decompressed_path = temp_f_out.name
                    with bz2.open(input_path, "rb") as f_in:
                        temp_f_out.write(f_in.read())
                
                print(f"📊 Reading decompressed file: {temp_decompressed_path}")
                
                # Use the decompressed path for DuckDB
                relation = con.from_csv_auto(temp_decompressed_path, header=True, ignore_errors=True);

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
        else:
            print(f"⚠️ Unsupported file format: {file_ext}")
            return False
        
        if relation is None:
            print(f"⚠️ Failed to read DataFrame from {str(input_path)} (format: {file_ext}). Skipping refinement.")
            return False
        
        # Text column selection criteria
        columns = relation.columns
        types = relation.types # DuckDB data types
        col_types_map = dict(zip(columns, types))
        
        
        # Get total row count once
        count_result = relation.aggregate('count(*)').fetchone()
        if count_result is None:
            raise ValueError("Could not get row count from relation.")
        total_rows = count_result[0]

        if total_rows == 0:
            print("Relation is empty, no columns to filter.")
            return False

        text_columns = []
        for col in columns:
            col_type = col_types_map[col]
            col_type_str = str(col_type).upper()

            non_null_count_result = relation.aggregate(f'count("{col}")').fetchone()
            non_null_count = non_null_count_result[0]
            
            avg_len_query = f'avg(length(CAST("{col}" AS VARCHAR)))'
            avg_len_result = relation.aggregate(avg_len_query).fetchone()
            avg_len = avg_len_result[0] if avg_len_result and avg_len_result[0] is not None else 0
            
            if (col_type_str != 'VARCHAR' or # Not a string column
                non_null_count < math.ceil(total_rows / 2.0) or # More than 50% non-null
                avg_len <= 5): # Avg length <= 5
                print(f"Skipping column '{col}")
                continue
        
            # Select the specific column from the existing relation and rename it
            col_relation = relation.select(f'"{col}" AS THISCOL')
            # Create the table from this new, single-column relation
            con.execute("CREATE OR REPLACE TABLE dictionary_sizer_table AS SELECT * FROM col_relation")
            
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
            print(f"Running FSST compression on column '{col}'")
            cpp_input_path = temp_decompressed_path if is_bz2 and temp_decompressed_path else str(input_path)
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
        
        # Only write the file if there are text columns that meet our criteria
        if text_columns:
            # Ensure the parent directory exists
            output_path.parent.mkdir(parents=True, exist_ok=True)

            # Construct comma-separated string for column selection
            # Quote column names to handle potential special characters
            select_expr = ", ".join([f'\"{col}\"' for col in text_columns])
            
            # Select the final columns using the explicit expression
            refined_data = relation.select(select_expr)
            
            refined_data.write_parquet(str(output_path))
            print(f"✅ Processed {str(input_path)} -> {str(output_path)}, with columns: {text_columns}")
            return True
        else:
            print(f"⏭️ No columns found in {str(input_path)} that match the criteria. Skipping file.")
            return False
      
    finally:
        con.close()
        
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
    # supported_files.extend(raw_path.glob("**/*.csv"))
    # supported_files.extend(raw_path.glob("**/*.csv.bz2"))
    
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