import duckdb
import os
import glob
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from functools import partial

def process_match_pairs(state, cleaned_pstl_dir, output_dir, data_dir):
    # 1) New connection per process
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    con.execute("PRAGMA threads = 1;")  # one thread per worker
    con.execute("PRAGMA memory_limit='100GB';")
    # 2) state‑specific temp dir
    temp_dir = os.path.join(data_dir, f"duckdb_temp_match_{state}")
    os.makedirs(temp_dir, exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{temp_dir}';")
    con.execute("PRAGMA max_temp_directory_size='500GB';")

    # 3) Paths
    input_parquet  = os.path.join(cleaned_pstl_dir, f"cleanedpstl_{state}.parquet")
    output_parquet = os.path.join(output_dir,      f"match_pairs_{state}.parquet")

    # 4) Run your SQL
    con.execute(f"""
        CREATE OR REPLACE TABLE cleaned_pstl AS 
        SELECT fips_id, owner, pstlclean, mailadd, state2,
               ST_GeomFromWKB(geom) AS geom
          FROM read_parquet('{input_parquet}');
    """)
    # owner matches
    con.execute("DROP TABLE IF EXISTS match_pairs_owner;")
    con.execute("""
        CREATE TABLE match_pairs_owner AS
        SELECT a.fips_id AS id1, b.fips_id AS id2
          FROM cleaned_pstl a
          JOIN cleaned_pstl b
            ON a.owner = b.owner
           AND a.owner IS NOT NULL
           AND a.owner <> 'CURRENT OWNER'
           AND a.fips_id < b.fips_id
         WHERE a.state2 = ? AND b.state2 = ?
           AND ST_DWithin(a.geom, b.geom, 100);
    """, [state, state])
    # address matches
    con.execute("DROP TABLE IF EXISTS match_pairs_address;")
    con.execute("""
        CREATE TABLE match_pairs_address AS
        SELECT a.fips_id AS id1, b.fips_id AS id2
          FROM cleaned_pstl a
          JOIN cleaned_pstl b
            ON a.pstlclean = b.pstlclean
           AND a.pstlclean <> ''
           AND b.pstlclean <> ''
           AND a.mailadd  <> ''
           AND b.mailadd  <> ''
           AND a.fips_id < b.fips_id
         WHERE a.state2 = ? AND b.state2 = ?
           AND ST_DWithin(a.geom, b.geom, 100);
    """, [state, state])
    # union & write
    con.execute("DROP TABLE IF EXISTS match_pairs_p;")
    con.execute("CREATE TABLE match_pairs_p AS SELECT * FROM match_pairs_owner;")
    con.execute("INSERT INTO match_pairs_p SELECT * FROM match_pairs_address;")
    con.execute(f"COPY match_pairs_p TO '{output_parquet}' (FORMAT 'parquet');")

    count = con.execute("SELECT COUNT(*) FROM match_pairs_p").fetchone()[0]
    print(f" {state}: wrote {count} pairs → {output_parquet}")
    con.close()
    return state

if __name__ == '__main__':
    region = os.getenv("REGION")
    data_dir = os.getenv("DATA_DIR")

    cleaned_pstl_dir = f"{data_dir}/parquet/{region}/parquets_cleaned"
    output_dir       = f"{data_dir}/parquet/{region}/{region}_match_pairs"
    os.makedirs(output_dir, exist_ok=True)

    # all states
    parquet_files = glob.glob(os.path.join(cleaned_pstl_dir, "cleanedpstl_*.parquet"))
    all_states = [os.path.basename(f).split("_")[1].split(".")[0] for f in parquet_files]

    # skip already done
    done = {os.path.basename(f).split("_")[2].split(".")[0]
            for f in glob.glob(os.path.join(output_dir, "match_pairs_*.parquet"))}
    to_run = [s for s in all_states if s not in done]

    if not to_run:
        print(" All states processed!")
        exit(0)

    print(f" Parallelizing match_pairs for: {to_run}")
    # bind the extra args
    worker = partial(process_match_pairs,
                     cleaned_pstl_dir=cleaned_pstl_dir,
                     output_dir=output_dir,
                     data_dir=data_dir)

    with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as exe:
        results = list(exe.map(worker, to_run))

    print(" Done:", results)
