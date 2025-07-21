import pandas as pd
import numpy as np

region = "south"
# 1. load
df = pd.read_csv(f"/home/christina/Desktop/property-matching/regrid_2025/validation/{region}_neighbors.csv")

# 2. extract the focal‐parcel attributes
#    (those rows where fips_id == focal_id)
focals = (
    df.loc[df.fips_id == df.focal_id,
           ["fips_id","propid","owner","pstlclean"]]
      .rename(columns={
         "fips_id":"focal_id",
         "propid":"focal_propid",
         "owner":"focal_owner",
         "pstlclean":"focal_pstlclean"
      })
)

# 3. merge them back onto the main table by focal_id
df = df.merge(focals, on="focal_id", how="left")

# 4. compute your new flags
df["self"] = np.where(df.fips_id == df.focal_id, "yes", "no")
df["propmatch"]    = np.where(df.propid == df.focal_propid,    "yes", "no")
df["namematch"]    = np.where(df.owner  == df.focal_owner,     "yes", "no")
df["pstlclean_match"] = np.where(
    df.pstlclean == df.focal_pstlclean, "yes", "no"
)

# 5. (optional) drop the helper focal_… columns
df = df.drop(columns=["focal_propid","focal_owner","focal_pstlclean"])

# 6. write it back out
df.to_csv(f"/home/christina/Desktop/property-matching/regrid_2025/validation/property/{region}_neighbors.csv", index=False)
