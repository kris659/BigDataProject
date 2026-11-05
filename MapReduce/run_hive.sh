#!/bin/sh

if [ $# -ne 3 ]; then
    echo "Usage: $0 input_dir3 input_dir4 output_dir6" >&2
    exit 1
fi

INPUT_DIR3="$1"
INPUT_DIR4="$2"
OUTPUT_DIR6="$3"

beeline -u jdbc:hive2://localhost:10000/default \
	-n hadoop \
        --hivevar input_dir3="$INPUT_DIR3" \
        --hivevar input_dir4="$INPUT_DIR4" \
        --hivevar output_dir6="$OUTPUT_DIR6" \
        -f hive.sql

exit $?