import duckdb
from pathlib import Path

# === CONFIGURATION ===
BASE_DIR = Path("/home/christina/Desktop/property-matching/regrid_2025/parquet/UPDATE_SEPTEMBER/parquet")
REGIONS  = ["midwest", "south", "northeast", "west"]

# connect to in‑memory DuckDB
con = duckdb.connect()

# adjust threads to your CPU count for faster parquet scans
con.execute("PRAGMA threads = 8;")

for region in REGIONS:
    props_dir  = BASE_DIR / region / f"{region}_propsholds_final"
    census_dir = BASE_DIR / region / f"{region}_census"

    if not props_dir.exists() or not census_dir.exists():
        print(f"Skipping {region}: folder missing")
        continue

    for props_fp in props_dir.glob("propsholds_final_*.parquet"):
        state = props_fp.stem.split("_")[-1]
        urban_fp = census_dir / f"{region}_{state}_urban_flag.parquet"

        if not urban_fp.exists():
            print(f"  [!] no urban flag for {region}/{state}")
            continue

        out_fp = props_fp.with_name(f"{props_fp.stem}_urban.parquet")
        print(f"  • Processing {region}/{state}…")

        # single COPY … SELECT does the join and write in one go
        con.execute(f"""
            COPY (
                SELECT 
                  p.*, 
                  u.in_urban
                FROM parquet_scan('{props_fp.as_posix()}') AS p
                LEFT JOIN parquet_scan('{urban_fp.as_posix()}') AS u
                  USING (fips_id)
            ) 
            TO '{out_fp.as_posix()}' (FORMAT PARQUET);
        """)
        print(f"    → wrote {out_fp.name}")

con.close()
