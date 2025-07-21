#!/usr/bin/env python3
import geopandas as gpd
from pathlib import Path

# 1. Paths
shp_fp   = Path("/home/christina/Desktop/data/Census_UrbanAreas/tl_2024_us_uac20.shp")
out_fp   = Path("/home/christina/Desktop/data/Census_UrbanAreas/urban_5070.parquet")

# 2. Read & reproject
urban = gpd.read_file(shp_fp)
urban = urban.to_crs(epsg=5070)

# 3. Serialize geometry as WKB
urban["geom"] = urban.geometry.apply(lambda g: g.wkb)

# 4. Drop everything but the WKB column (and whatever ID you might want)
urban = urban[["geom"]]

# 5. Write to Parquet
urban.to_parquet(out_fp, engine="pyarrow", index=False)

print(f"✅ Wrote {out_fp}")

