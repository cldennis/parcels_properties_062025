#!/bin/bash

uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt

#python3 "urban_rural/census_parquet.py"
#python3 "urban_rural/makecentroids.py"
#python3 "urban_rural/selecturban.py"
#python3 "urban_rural/joincolumn.py"
#python3 "urban_rural/props_urban.py"
#python3 "urban_rural/join_avgurban.py"

