#!/bin/bash

uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt

#python3 "validation/sample_holds100.py"
#python3 "validation/sample_holds2.py"
#python3 "validation/select_props.py"
#python3 "validation/prop_check.py"
#python3 "validation/holds100summary.py"

