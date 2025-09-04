#!/usr/bin/env python3
import duckdb
from pathlib import Path

# ——— CONFIGURATION ———
BASE_DIR = Path("/home/christina/Desktop/property-matching/regrid_2025/parquet/UPDATE_SEPTEMBER/parquet")
REGIONS  = ["midwest", "south", "northeast", "west"]

# Connect to DuckDB (in‑memory)
con = duckdb.connect()
con.execute("PRAGMA threads = 8;")

for region in REGIONS:
    shapes_dir = BASE_DIR / region / f"{region}_prop_shapes"
    if not shapes_dir.exists():
        print(f"[{region}] – missing folder, skipping")
        continue

    urban_fp = shapes_dir / "props_urban.parquet"
    if not urban_fp.exists():
        print(f"[{region}] – no props_urban.parquet, skipping")
        continue

    # adjust this pattern if you know the exact shapes filename
    all_parquets = list(shapes_dir.glob("*.parquet"))
    target_fps = [p for p in all_parquets if p.name != urban_fp.name]

    if not target_fps:
        print(f"[{region}] – no other .parquet to join, skipping")
        continue

    for shapes_fp in target_fps:
        out_fp = shapes_dir / f"{shapes_fp.stem}_with_urban.parquet"
        print(f"[{region}] joining → {shapes_fp.name}")

        con.execute(f"""
            COPY (
              SELECT
                s.*,
                u.avg_inurban
              FROM
                parquet_scan('{shapes_fp.as_posix()}')    AS s
              LEFT JOIN
                parquet_scan('{urban_fp.as_posix()}')    AS u
              USING (propid)
            )
            TO '{out_fp.as_posix()}' (FORMAT PARQUET);
        """)
        print(f"      wrote → {out_fp.name}")

con.close()
