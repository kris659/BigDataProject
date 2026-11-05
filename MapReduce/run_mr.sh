#!/bin/sh

if [ $# -ne 2 ]; then
    echo "Usage: $0 input_dir1 output_dir3" >&2
    exit 1
fi

INPUT_DIR="$1"
OUTPUT_DIR="$2"

hadoop fs -rm -r -f "$OUTPUT_DIR"

hadoop jar "SalesStatsMapReduce.jar" \
    -D "mapreduce.output.textoutputformat.separator=," \
    "$INPUT_DIR" "$OUTPUT_DIR"
exit $?