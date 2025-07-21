#!/usr/bin/env python3
import duckdb
from pathlib import Path

# ——— CONFIGURATION ———
BASE_DIR = Path("/home/christina/Desktop/property-matching/regrid_2025/parquet")
REGIONS  = ["midwest", "south", "northeast", "west"]

# connect to an in‑memory DuckDB
con = duckdb.connect()

# use all your cores for the parquet scan
con.execute("PRAGMA threads = 8;")

for region in REGIONS:
    in_glob = BASE_DIR / region / f"{region}_propsholds_final" / "*.parquet"
    out_dir = BASE_DIR / region / f"{region}_prop_shapes"
    out_dir.mkdir(exist_ok=True)  # create if missing
    out_fp  = out_dir / "props_urban.parquet"

    print(f"[{region}] scanning → {in_glob}")
    con.execute(f"""
        COPY (
          SELECT
            propid,
            AVG(in_urban) AS avg_inurban
          FROM parquet_scan('{in_glob.as_posix()}')
          GROUP BY propid
        )
        TO '{out_fp.as_posix()}' (FORMAT PARQUET);
    """)
    print(f"[{region}] wrote → {out_fp.relative_to(BASE_DIR)}")

con.close()
