import pandas as pd
import pyarrow.parquet as pq
import os
import glob

# === Paths ===
holdings_path = "/home/christina/Desktop/property-matching/regrid_2025/parquet/northeast/northeast_holdings/holdings_info.parquet"
parcels_folder = "/home/christina/Desktop/property-matching/regrid_2025/parquet/northeast/northeast_propsholds_final/"
output_csv = "/home/christina/Desktop/property-matching/regrid_2025/summaries/sampled_northeast_100parcel_holdings.csv"
print(f"✅ Done. loaded file paths {output_csv}")

# === Load holdings and sample 25 with exactly 100 parcels ===
holdings = pd.read_parquet(holdings_path)
print(f"✅ Done. lodaded {holdings} ")
holdings_100 = holdings[holdings['holds_numparcels'] == 100]
print(f"✅ Done. lodaded {holdings_100} ")
sampled_holdings = holdings_100.sample(n=25, random_state=42)
print(f"✅ Done. lodaded {sampled_holdings} ")
sampled_holdids = set(sampled_holdings['holdid'])
print(f"✅ Done. lodaded {sampled_holdids} ")

# === Get list of parcel files ===
parcel_files = glob.glob(os.path.join(parcels_folder, "*.parquet"))
print(f"✅ Done. lodaded {parcel_files} ")

# === Track if header has been written ===
header_written = False
rows_written = 0

for file in parcel_files:
    df = pd.read_parquet(file, columns=['holdid', 'propid', 'fips_id', 'owner', 'mailadd', 'pstlclean', 'state2', 'county', 'city' ])  # include other cols if needed
    df_filtered = df[df['holdid'].isin(sampled_holdids)]
    print(f"✅ Done. lodaded {df} ")

    if not df_filtered.empty:
        df_filtered.to_csv(output_csv, mode='a', index=False, header=not header_written)
        header_written = True
        rows_written += len(df_filtered)

print(f"✅ Done. Wrote {rows_written} parcels to: {output_csv}")

