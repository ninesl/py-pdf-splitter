#!/bin/bash

# Usage: ./script.sh START_DAY END_DAY MONTH YEAR
# Example: ./script.sh 1 5 05 2025

START_DAY=$1
END_DAY=$2
MONTH=$3
YEAR=$4

for DAY in $(seq -w $START_DAY $END_DAY); do
    make run DAY=$DAY MONTH=$MONTH YEAR=$YEAR
done
