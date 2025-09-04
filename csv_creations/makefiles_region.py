import duckdb
#
# which = 'propid'
# region       = "northeast"
# base_dir     = f"/home/christina/Desktop/property-matching/regrid_2025/parquet/{region}"
# prop_shapes  = f"{base_dir}/{region}_prop_shapes/prop_shapes_{region}_with_urban.parquet"
# hold_shapes  = f"{base_dir}/{region}_prop_shapes/prop_shapes_{region}_with_urban.parquet"
# props_glob   = f"{base_dir}/{region}_propsholds_final/*.parquet"
# output_csv   = f"/home/christina/Desktop/property-matching/regrid_2025/methods_figures/data/{region}_combined_regional_summary_{which}.csv"
#
# conn = duckdb.connect()
# conn.execute(f"""
# COPY (
#   WITH
#     shape_avg AS (
#       SELECT AVG(area_acres) AS avg_area_acres
#       FROM parquet_scan('{prop_shapes}')
#     ),
#     prop_avg AS (
#       SELECT
#         AVG(zip_match) AS avg_zip_match_per_{which},
#         AVG(in_urban)  AS avg_in_urban_per_{which},
#         COUNT(fips_id) AS parcels_per_{which},
#       FROM '{props_glob}'
#       GROUP BY {which}
#     )
#   SELECT
#     '{region}'                             AS region,
#     ROUND(ANY_VALUE(shape_avg.avg_area_acres), 3)   AS avg_area_acres,
#     ROUND(AVG(prop_avg.avg_zip_match_per_{which}),   3) AS avg_zip_match,
#     ROUND(AVG(prop_avg.avg_in_urban_per_{which}),    3) AS avg_in_urban,
#     ROUND(AVG(prop_avg.parcels_per_{which}),         3) AS avg_parcels_per_{which}
#   FROM shape_avg
#   CROSS JOIN prop_avg
# )
# TO '{output_csv}'
# (FORMAT CSV, HEADER TRUE);
# """)
# print(f"Wrote combined one-line regional summary to {output_csv}")

