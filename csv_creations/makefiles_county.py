import duckdb

which = 'holdid'

region      = 'west'
base_dir    = f'/home/christina/Desktop/property-matching/regrid_2025/parquet/{region}'
prop_shapes = f'{base_dir}/{region}_prop_shapes/prop_shapes_{region}_with_urban.parquet'
output_csv  = f'/home/christina/Desktop/property-matching/regrid_2025/methods_figures/data/{region}_county_summary_{which}.csv'


# 1) Open an in‐memory DuckDB
conn = duckdb.connect()

# 2) Build and run the combined query
sql = f"""
COPY (
  WITH
    -- per‐property averages
    prop_avg AS (
      SELECT
        {which},
        county,
        state2,
        AVG(zip_match) AS avg_zip_match_per_{which},
        AVG(in_urban)  AS avg_in_urban_per_{which},
        COUNT(fips_id) AS parcels_per_{which}
      FROM '{base_dir}/{region}_propsholds_final/*.parquet'
      GROUP BY {which}, county, state2
    ),

    -- roll up those to county level
    county_stats AS (
      SELECT
        county,
        state2,
        AVG(avg_zip_match_per_{which}) AS avg_zip_match_by_county,
        AVG(avg_in_urban_per_{which})  AS avg_in_urban_by_county,
        AVG(parcels_per_{which})       AS avg_parcels_prop_by_county
      FROM prop_avg
      GROUP BY county, state2
    ),

    -- per‐property areas
    area_table AS (
      SELECT {which}, area_acres
      FROM '{prop_shapes}'
    ),

    -- join back to get county‐level area stats
    county_area AS (
      SELECT
        p.county,
        p.state2,
        COUNT(*)            AS prop_count,
        AVG(a.area_acres)   AS avg_area_acres
      FROM '{base_dir}/{region}_propsholds_final/*.parquet' AS p
      JOIN area_table AS a USING ({which})
      GROUP BY p.county, p.state2
    )

  -- final join of the two county tables
  SELECT
    cs.county,
    cs.state2,
    ROUND(cs.avg_zip_match_by_county, 3)     AS avg_zip_match_by_county,
    ROUND(cs.avg_in_urban_by_county, 3)      AS avg_in_urban_by_county,
    ROUND(cs.avg_parcels_prop_by_county, 3)  AS avg_parcels_prop_by_county,
    ca.prop_count,
    ROUND(ca.avg_area_acres, 3)              AS avg_area_acres
  FROM county_stats AS cs
  JOIN county_area  AS ca
    ON cs.county = ca.county
   AND cs.state2 = ca.state2
  ORDER BY cs.state2, cs.county
)
TO '{output_csv}'
( FORMAT CSV, HEADER TRUE );
"""

conn.execute(sql)
print(f"Wrote combined county summary to {output_csv}")
conn.close()
