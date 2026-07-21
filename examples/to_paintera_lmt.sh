#!/usr/bin/env bash

# Convert a label dataset into a Paintera label-multiset source with a multiscale mipmap pyramid.
INPUT_CONTAINER="${INPUT_CONTAINER:-sample.n5}"
INPUT_DATASET="${INPUT_DATASET:-volumes/labels/s0}"
OUTPUT_CONTAINER="${OUTPUT_CONTAINER:-paintera.n5}"
OUTPUT_DATASET="${OUTPUT_DATASET:-volumes/labels}"

paintera-convert to-paintera \
  --output-container="$OUTPUT_CONTAINER" \
  --scale 2,2,2 2,2,2 2,2,2 \
  --block-size=64,64,64 \
  --container="$INPUT_CONTAINER" \
    -d "$INPUT_DATASET" \
      --target-dataset="$OUTPUT_DATASET" \
      --type=label
