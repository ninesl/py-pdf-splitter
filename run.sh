#!/bin/bash

# Usage: ./run.sh START_DAY START_MONTH START_YEAR

START_DAY=$1
START_MONTH=$2
START_YEAR=$3

# Convert input start date to YYYY-MM-DD
START_DATE=$(date -d "$START_YEAR-$START_MONTH-$START_DAY" +%Y-%m-%d)
# Get yesterday's date
END_DATE=$(date -d "yesterday" +%Y-%m-%d)

CURRENT_DATE="$START_DATE"

while [[ "$CURRENT_DATE" < "$END_DATE" || "$CURRENT_DATE" == "$END_DATE" ]]; do
    DAY=$(date -d "$CURRENT_DATE" +%d)
    MONTH=$(date -d "$CURRENT_DATE" +%m)
    YEAR=$(date -d "$CURRENT_DATE" +%Y)
    uv run pdf_parse.py "$DAY" "$MONTH" "$YEAR"
    # Increment date by 1 day
    CURRENT_DATE=$(date -I -d "$CURRENT_DATE + 1 day")
done