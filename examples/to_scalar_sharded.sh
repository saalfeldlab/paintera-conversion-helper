#!/usr/bin/env bash
# Extract a Paintera label source into a sharded Zarr3 uint64 scalar dataset with OME-Zarr metadata.
INPUT_CONTAINER="${INPUT_CONTAINER:-paintera.n5}"
INPUT_DATASET="${INPUT_DATASET:-volumes/labels}"
OUTPUT_CONTAINER="${OUTPUT_CONTAINER:-scalar.zarr}"
OUTPUT_DATASET="${OUTPUT_DATASET:-volumes/labels}"

paintera-convert to-scalar \
  -i "$INPUT_CONTAINER" \
  -I "$INPUT_DATASET" \
  -o "$OUTPUT_CONTAINER" \
  -O "$OUTPUT_DATASET" \
  --output-format=ZARR3 \
  --scale 2,2,2 2,2,2 \
  --block-size=64,64,64 \
  --chunks-per-shard=4,4,4 \
  --resolution=4,4,40 \
  --xyz-unit=nm
