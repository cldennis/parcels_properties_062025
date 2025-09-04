#!/usr/bin/env python3
import duckdb
######################## THIS IS THE SCRIPT USED TO MAKE THE COUNTY LEVEL METRIC FILES AREA WEIGHTED FOR ZIP_MATCH
# 1) Connect & load spatial extension (optional, but safe)
conn = duckdb.connect()
conn.execute("INSTALL spatial;")
conn.execute("LOAD spatial;")

# 2) Define your regions and the metrics you want to summarize
regions = ['midwest', 'south', 'northeast', 'west']
metrics = ['propid', 'holdid']

# 3) Loop through each metric and build+execute a UNION of all four regions
for metric in metrics:
    region_queries = []
    for region in regions:
        region_queries.append(f"""
        SELECT
          hi.county,
          hi.state2,

          -- area-weighted county means of your four per-metric summaries
          ROUND(
            SUM(hm.avg_zip_match    * hm.{metric}_area_acres)
            / SUM(hm.{metric}_area_acres),
            3
          ) AS aw_avg_of_avg_zip_match,

          ROUND(
            SUM(hm.avg_in_urban     * hm.{metric}_area_acres)
            / SUM(hm.{metric}_area_acres),
            3
          ) AS aw_avg_of_avg_in_urban,

          ROUND(
            SUM(hm.aw_avg_zip_match  * hm.{metric}_area_acres)
            / SUM(hm.{metric}_area_acres),
            3
          ) AS aw_aw_avg_of_zip_match,

          ROUND(
            SUM(hm.aw_avg_in_urban   * hm.{metric}_area_acres)
            / SUM(hm.{metric}_area_acres),
            3
          ) AS aw_aw_avg_of_urban,

          -- percent of **county‐area** where each metric exceeds thresholds
          ROUND(
            100.0 * SUM(
              CASE WHEN hm.avg_zip_match    > 0.75 THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_avg_zip_gt_075,

          ROUND(
            100.0 * SUM(
              CASE WHEN hm.avg_in_urban     > 0.75 THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_avg_urban_gt_075,

          ROUND(
            100.0 * SUM(
              CASE WHEN hm.aw_avg_zip_match > 0.75 THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_aw_zip_gt_075,

          ROUND(
            100.0 * SUM(
              CASE WHEN hm.aw_avg_in_urban  > 0.75 THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_aw_urban_gt_075,

          -- threshold > 0.5
          ROUND(
            100.0 * SUM(
              CASE WHEN hm.avg_zip_match    > 0.5  THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_avg_zip_gt_0_5,

          ROUND(
            100.0 * SUM(
              CASE WHEN hm.avg_in_urban     > 0.5  THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_avg_urban_gt_0_5,

          ROUND(
            100.0 * SUM(
              CASE WHEN hm.aw_avg_zip_match > 0.5  THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_aw_zip_gt_0_5,

          ROUND(
            100.0 * SUM(
              CASE WHEN hm.aw_avg_in_urban  > 0.5  THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_aw_urban_gt_0_5,

          -- threshold > 0
          ROUND(
            100.0 * SUM(
              CASE WHEN hm.avg_zip_match    > 0    THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_avg_zip_gt_0,

          ROUND(
            100.0 * SUM(
              CASE WHEN hm.aw_avg_zip_match > 0    THEN hm.{metric}_area_acres ELSE 0 END
            ) / SUM(hm.{metric}_area_acres),
            3
          ) AS pct_area_aw_zip_gt_0,

          geo.geoid::TEXT AS geoid

        FROM
          read_parquet(
            '/home/christina/Desktop/property-matching/regrid_2025/'
            || 'methods_figures/parquets_local_urban/'
            || '{region}_local_urban_{metric}_weighted.parquet'
          ) AS hm

        JOIN (
          SELECT DISTINCT {metric}, county, state2
          FROM parquet_scan(
            '/home/christina/Desktop/property-matching/regrid_2025/'
            || 'parquet/{region}/{region}_propsholds_final/*urban.parquet'
          )
        ) AS hi
        USING({metric})

        LEFT JOIN read_csv_auto(
          '/media/christina/ssd_cd/cdennis_2025/data_info/'
          || 'county_area/county_geoid_{region}.csv'
        ) AS geo
        USING(county, state2)

        GROUP BY hi.county, hi.state2, geo.geoid
        """)

    # UNION all four regions into one big query
    union_sql = "\nUNION ALL\n".join(region_queries)

    # Write out a single CSV for this metric
    out_path = (
      '/home/christina/Desktop/property-matching/regrid_2025/'
      'methods_figures/parquets_local_urban/summaries/'
      f'area_weighted_full_{metric}.csv'
    )

    conn.execute(f"""
    COPY (
      {union_sql}
    )
    TO '{out_path}'
    (FORMAT CSV, HEADER TRUE, DELIMITER ',');
    """)

    print(f"Wrote {out_path}")
