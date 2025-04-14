import bz2
import io
import polars as pl
from pathlib import Path
import sys

def read_nextia_dataset(file_path):
    """
    Read a NextiaJD compressed CSV dataset using Polars
    
    Args:
        file_path (str): Path to the .csv.bz2 file
        
    Returns:
        polars.DataFrame: The loaded dataframe
    """
    # Convert to Path object for easier handling
    file_path = Path(file_path)
    
    print(f"Reading {file_path.name}...")
    
    try:
        # Read the compressed file
        with bz2.BZ2File(file_path, 'rb') as bz_file:
            # Read the content
            content = bz_file.read()
            
            # Decode with error handling
            text_content = content.decode('utf-8', errors='replace')
            
            # Create a StringIO object
            csv_file = io.StringIO(text_content)
            
            # Read with polars with options to handle issues
            df = pl.read_csv(
                csv_file,
                infer_schema_length=10000,
                ignore_errors=True,
                truncate_ragged_lines=True,
                has_header=True,
                low_memory=False,
                try_parse_dates=True,
                null_values=["", "NA", "NULL", "null"]
            )
            
            print(f"Successfully read {file_path.name}")
            print(f"Shape: {df.shape}")
            
            return df
            
    except Exception as e:
        print(f"Error reading {file_path.name}: {e}")
        return None

# Example usage
if __name__ == "__main__":

    file_path = sys.argv[1]

    
    # Read the dataset
    df = read_nextia_dataset(file_path)
    
    if df is not None:
        # Display information about the dataframe
        print("\nColumns:")
        print(df.columns)
        
        print("\nFirst few rows:")
        print(df.head())
        
        print("\nData types:")
        print(df.dtypes)
        
        # Optional: Save to parquet for faster future access
        output_path = Path(file_path).with_suffix('.parquet')
        print(f"\nSaving to {output_path}...")
        df.write_parquet(output_path)
        print(f"Saved to {output_path}") 