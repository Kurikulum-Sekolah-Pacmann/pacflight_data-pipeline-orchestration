#!/bin/bash

echo "========== Start Orcestration Process =========="

# Virtual Environment Path
VENV_PATH="/home/laode/pacmann/course/data-storage/week-5/pacflight_data-pipeline-orchestration/.venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python script
PYTHON_SCRIPT="/home/laode/pacmann/course/data-storage/week-5/pacflight_data-pipeline-orchestration/elt_main.py"

# Run Python Script 
python3 "$PYTHON_SCRIPT"


echo "========== End of Orcestration Process =========="