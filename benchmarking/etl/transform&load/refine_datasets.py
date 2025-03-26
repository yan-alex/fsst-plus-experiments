import polars as pl
from pathlib import Path, PurePath
import os

def refine_dataset(input_path: PurePath, output_path: PurePath):
    print(f"\nRefining {str(input_path)} -> {str(output_path)}");
    
    """Process a single Parquet file using the refinement logic"""
    df = pl.read_parquet(str(input_path))
    
    # Text column selection craiteria
    text_columns = [
        col for col in df.columns
        if (df[col].dtype == pl.String and
            df[col].null_count() < df.height / 2 and  # More than 50% non-null
            df[col].str.len_chars().mean() > 5)       # Avg length > 5
    ]
    
    # Only write the file if there are text columns that meet our criteria
    if text_columns:
        # Ensure the parent directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)

        df_text = df.select(text_columns)
        df_text.write_parquet(str(output_path))
        print(f"✅ Processed {str(input_path)} -> {str(output_path)}, with columns: {text_columns}")

        return True
    else:
        print(f"❌ No columns found in {str(input_path)} that match the criteria. Skipping file.")
        return False

def process_raw_directory(raw_dir: str = "../../data/raw", output_dir: str = "../../data/refined"):
    """Process all Parquet files in the raw directory and its subdirectories"""
    raw_path = Path(raw_dir)
    output_path = Path(output_dir)
    
    # Create output directory if it doesn't exist
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Process all Parquet files recursively
    processed_count = 0
    error_count = 0
    
    for parquet_file in raw_path.glob("**/*.parquet"):
        # Preserve directory structure
        relative_path = parquet_file.relative_to(raw_path)
        output_file = output_path / relative_path
        

        found_columns = refine_dataset(parquet_file, output_file)
        if found_columns:
            processed_count += 1
        else:
            error_count += 1
    
    print(f" 🌐 Macrodata Refinement Complete 🌐 \n {processed_count} files matched the criteria, {error_count} files didn't have compatible columns")

if __name__ == "__main__":
    process_raw_directory() 