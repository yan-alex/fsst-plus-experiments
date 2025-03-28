import polars as pl
from pathlib import Path, PurePath
import os
import math
import duckdb
import tempfile
import bz2
from read_nextia_datasets import read_nextia_dataset

def get_compression_method(df, column_name):
    temp_db_path = "duckdb_compression_choice.db"
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
    print(f"\nRefining {str(input_path)} -> {str(output_path)}")
    
    """Process a single data file using the refinement logic"""
    # Determine file type and read accordingly
    file_ext = input_path.suffix.lower()
    if file_ext == '.parquet':
        df = pl.read_parquet(str(input_path))
    elif file_ext == '.csv':
        df = pl.read_csv(str(input_path))
    elif input_path.name.endswith('.csv.bz2'):
        try:
            print(f"🔍 Reading {str(input_path)}")
            df = read_nextia_dataset(str(input_path))
            if df is None:
                print(f"🚨 Failed to read {str(input_path)} using read_nextia_dataset. Skipping refinement.")
                return False
        except Exception as e:
            print(f"❌ Error reading {str(input_path)}: {e}")
            return False
        
        # with bz2.open(str(input_path), 'rt') as f:
        #     df = pl.read_csv(f)
    elif file_ext == '.json':
        df = pl.read_json(str(input_path))
    else:
        print(f"⚠️ Unsupported file format: {file_ext}")
        return False

    # Text column selection criteria
    text_columns = []
    for col in df.columns:
        # num_rows = df.height
        # dict_encoding_est_size = num_unique * avg_len + (num_rows * (math.log(num_unique, 2)/8))
        
        if (df[col].dtype == pl.String and
            df[col].null_count() < df.height / 2 and  # More than 50% non-null
            df[col].str.len_chars().mean() > 5 and # Avg length > 5
            df[col].n_unique() > 1000): 
            
            # # Check if DuckDB would use FSST compression for this column
            # compression_type = get_compression_method(df, col)
            
            # if compression_type and "FSST" in compression_type.upper():
            #     text_columns.append(col)
            #     print(f"Column '{col}' would be FSST-encoded. Adding to selection.")
            # else:
            #     print(f"Column '{col}' would NOT be FSST-encoded (would be {compression_type}). Skipping.")
            
            text_columns.append(col)
            
            
    
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