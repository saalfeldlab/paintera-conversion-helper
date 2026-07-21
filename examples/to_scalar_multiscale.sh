#!/usr/bin/env bash

# Extract a Paintera label source into a multiscale uint64 scalar dataset with a downsampling pyramid.
INPUT_CONTAINER="${INPUT_CONTAINER:-paintera.n5}"
INPUT_DATASET="${INPUT_DATASET:-volumes/labels}"
OUTPUT_CONTAINER="${OUTPUT_CONTAINER:-scalar.n5}"
OUTPUT_DATASET="${OUTPUT_DATASET:-volumes/labels}"

paintera-convert to-scalar \
  -i "$INPUT_CONTAINER" \
  -I "$INPUT_DATASET" \
  -o "$OUTPUT_CONTAINER" \
  -O "$OUTPUT_DATASET" \
  --consider-fragment-segment-assignment \
  --scale 2,2,2 2,2,2 \
  --block-size=64,64,64 \
  --resolution=4,4,40 \
  --xyz-unit=nm
