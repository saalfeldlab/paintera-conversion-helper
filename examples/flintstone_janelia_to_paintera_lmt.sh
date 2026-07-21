#!/usr/bin/env bash

# Convert a label dataset into a Paintera label-multiset source with a multiscale mipmap pyramid.
INPUT_CONTAINER="${INPUT_CONTAINER:-sample.n5}"
INPUT_DATASET="${INPUT_DATASET:-volumes/labels/s0}"
OUTPUT_CONTAINER="${OUTPUT_CONTAINER:-paintera.n5}"
OUTPUT_DATASET="${OUTPUT_DATASET:-volumes/labels}"

./startup-scripts/flintstone-paintera-convert.sh 5 -- to-paintera \
  --container="$INPUT_CONTAINER" \
  -d "$INPUT_DATASET" \
  --output-container="$OUTPUT_CONTAINER" \
  --target-dataset="$OUTPUT_DATASET" \
    --scale 2 2 2 2 \
    --block-size=32  \
    --type=label \
    --dataset-resolution 18,18,24
