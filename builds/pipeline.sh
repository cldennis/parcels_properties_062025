#!/bin/bash

uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Base data directory
DATA_DIR="/home/christina/Desktop/property-matching/regrid_2025/parquet/UPDATE_SEPTEMBER"
export DATA_DIR

# List of regions
REGIONS=("midwest" "west" "south")

#REGIONS=("west" "midwest" "south" "northeast")
for REGION in "${REGIONS[@]}"; do
    export REGION
    echo "🧭 Starting pipeline for REGION: $REGION"
#    python3 "scripts/check_distinct.py"
#    python3 "scripts/importparquet.py"
#    python3 "scripts/concatpstl.py"
#    python3 "scripts/pstlclean2.py"
#    python3 "scripts/prop_match2.py"
#    python3 "scripts/prop_groupmatch.py"
#    python3 "scripts/prop_setnullgroupid.py"
### September 4th -- updated from here for all regions
    python3 "scripts/holds_match.py"
    python3 "scripts/jointables.py"
    python3 "scripts/getbatches.py"
    python3 "scripts/holds_union.py"
    python3 "scripts/dispersion.py"
    python3 "scripts/joinzipcode.py"
    python3 "scripts/localzip.py"
#    python3 "scripts/addattributes.py"
    python3 "scripts/countchecks.py"

  echo "✅ Finished REGION: $REGION"
  echo "-------------------------------"
done
