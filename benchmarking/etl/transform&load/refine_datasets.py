import polars as pl
from pathlib import Path
import os

def refine_dataset(input_path: str, output_path: str):
    print(f"Refining {input_path} -> {output_path}");
    
    """Process a single Parquet file using the refinement logic"""
    df = pl.read_parquet(input_path)
    
    # Text column selection criteria
    text_columns = [
        col for col in df.columns
        if (df[col].dtype == pl.String and
            df[col].null_count() < df.height / 2 and  # More than 50% non-null
            df[col].str.len_chars().mean() > 5)       # Avg length > 5
    ]
    
    # Only write the file if there are text columns that meet our criteria
    if text_columns:
        df_text = df.select(text_columns)
        df_text.write_parquet(output_path)
        return True
    else:
        # If no text columns found, include the first column to ensure a valid Parquet file
        if len(df.columns) > 0:
            df_min = df.select([df.columns[0]])
            df_min.write_parquet(output_path)
            print(f"Warning: No suitable text columns found in {input_path}. Including only the first column.")
            return True
        else:
            print(f"Error: No columns found in {input_path}. Skipping file.")
            return False

def process_raw_directory(raw_dir: str = "/export/scratch2/home/yla/fsst-plus-experiments/data/raw", output_dir: str = "/export/scratch2/home/yla/fsst-plus-experiments/data/refined"):
    """Process all Parquet files in the raw directory and its subdirectories"""
    raw_path = Path(raw_dir)
    output_path = Path(output_dir)
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Process all Parquet files recursively
    processed_count = 0
    skipped_count = 0
    
    for parquet_file in raw_path.glob("**/*.parquet"):
        # Preserve directory structure
        relative_path = parquet_file.relative_to(raw_path)
        output_file = output_path / relative_path
        
        # Ensure the parent directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        success = refine_dataset(str(parquet_file), str(output_file))
        if success:
            print(f"Processed {parquet_file} -> {output_file}")
            processed_count += 1
        else:
            print(f"Skipped {parquet_file}")
            skipped_count += 1
    
    print(f" 🌐 Macrodata Refinement Complete 🌐 \n {processed_count} files processed, {skipped_count} files skipped")

if __name__ == "__main__":
    process_raw_directory() 