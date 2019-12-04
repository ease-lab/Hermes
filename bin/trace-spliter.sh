#!/usr/bin/env bash

INPUT_DIR="/home/s1671850/hermes/traces/system-traces/"
INPUT_FILENAME="simple_trace_w_100000000_k_1000000_a_0.99.txt"
OUTPUT_DIR="/home/s1671850/hermes/traces/current-splited-traces/"
OUTPUT_PREFIX="t_"
OUTPUT_SUFFIX="_a_0.99.txt"

MAX_NUM_NODES=10
MAX_THREADS_PER_NODE=40


CHUNKS=$(expr ${MAX_NUM_NODES} \* ${MAX_THREADS_PER_NODE})
LINES=$(wc -l ${INPUT_DIR}/${INPUT_FILENAME} | cut -d ' ' -f1)

echo "Splitting trace with $LINES lines into $CHUNKS (per-thread) chunks ..."

split -l  $(expr ${LINES} / ${CHUNKS}) \
      -a 4 -d \
      --additional-suffix=${OUTPUT_SUFFIX} \
      ${INPUT_DIR}/${INPUT_FILENAME} \
      ${OUTPUT_DIR}/${OUTPUT_PREFIX}
