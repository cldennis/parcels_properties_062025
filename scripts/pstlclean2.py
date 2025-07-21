import duckdb
import os
import re
import glob
import multiprocessing
from concurrent.futures import ProcessPoolExecutor

# Define cleaning functions (same as before)
def remove_commas(address):
    return re.sub(r'[,]+', ' ', address)

def expand_abbreviations(address):
    replacements = {
        r'\bST\b': 'STREET',
        r'\bRD\b': 'ROAD',
        r'\bAVE\b': 'AVENUE',
        r'\bAV\b': 'AVENUE',
        r'\bDR\b': 'DRIVE',
        r'\bN\b': 'NORTH',
        r'\bS\b': 'SOUTH',
        r'\bE\b': 'EAST',
        r'\bW\b': 'WEST',
        r'\bHWY\b': 'HIGHWAY',
        r'\bLN\b': 'LANE',
        r'\bCT\b': 'COURT',
        r'\bCIR\b': 'CIRCLE',
        r'\bSTE\b': 'SUITE',
        r'\bPL\b': 'PLACE',
        r'\bBLVD\b': 'BOULEVARD',
        r'\bTR\b': 'TRAIL'
    }
    for pattern, replacement in replacements.items():
        address = re.sub(pattern, replacement, address, flags=re.IGNORECASE)
    return address


def standardize_whitespace(address):
    return re.sub(r'\s+', ' ', address).strip()


def extract_zip(address):
    parts = address.split()
    if parts and re.match(r'\d{5}(-\d{4})?', parts[-1]):
        parts[-1] = parts[-1][:5]  # Keep only the first 5 digits
    return ' '.join(parts)


def remove_special_characters(address):
    return re.sub(r'[^a-zA-Z0-9\s]', '', address)


def remove_all_spaces(address):
    return re.sub(r'\s+', '', address)


def to_uppercase(address):
    return address.upper()


# Worker function to process a single state
def process_state(state, input_folder, output_folder, data_dir):
    try:
        # Each process creates its own DuckDB connection and initializes spatial extension.
        con = duckdb.connect(database=":memory:")
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        # Limit threads per process to 1 to avoid nested parallelism
        con.execute("PRAGMA threads = 1;")
        con.execute("PRAGMA memory_limit='100GB';")
        # Optionally, use a state-specific temp directory to avoid clashes
        temp_dir = os.path.join(data_dir, f"duckdb_temp_{state}")
        os.makedirs(temp_dir, exist_ok=True)
        con.execute(f"PRAGMA temp_directory='{temp_dir}';")
        con.execute("PRAGMA max_temp_directory_size='500GB';")

        input_parquet = os.path.join(input_folder, f"concatpstl_{state}.parquet")
        output_parquet = os.path.join(output_folder, f"cleanedpstl_{state}.parquet")

        print(f"🔹 Processing state: {state}")

        # Load full dataset (not just pstladress)
        df = duckdb.query(f"SELECT * FROM read_parquet('{input_parquet}')").fetchdf()

        # Apply cleaning functions to 'pstladress'
        # Note: Using chained .apply calls as in the original code.
        df['pstlclean'] = (
            df['pstladress']
            .fillna('')
            .apply(remove_commas)
            .apply(expand_abbreviations)
            .apply(standardize_whitespace)
            .apply(extract_zip)
            .apply(remove_special_characters)
            .apply(remove_all_spaces)
            .apply(to_uppercase)
        )

        # Save cleaned data as Parquet, preserving all original columns
        df.to_parquet(output_parquet, index=False)
        con.close()
        print(f"📁 Saved cleaned data to {output_parquet} for state {state}")
        return state
    except Exception as e:
        print(f"Error processing state {state}: {e}")
        raise

def process_state_wrapper(state):
    return process_state(state, input_folder, output_folder, data_dir)

if __name__ == '__main__':
    # Get environment variables and define directories
    region = os.getenv("REGION")
    data_dir = os.getenv("DATA_DIR")
    input_folder = f"{data_dir}/parquet/{region}/parquets_concat"
    output_folder = f"{data_dir}/parquet/{region}/parquets_cleaned"

    os.makedirs(output_folder, exist_ok=True)

    # Create your list of states from parquet_files as before.
    parquet_files = glob.glob(os.path.join(input_folder, "concatpstl_*.parquet"))
    states = [os.path.basename(f).replace("concatpstl_", "").replace(".parquet", "") for f in parquet_files]

    if not states:
        raise ValueError(f"❌ No Parquet state files found in {input_folder}")

    print(f"🔹 Found Parquet files for states: {states}")

    from concurrent.futures import ProcessPoolExecutor
    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
        results = list(executor.map(process_state_wrapper, states))

    print("🎉 Processing complete! Processed states:", results)
