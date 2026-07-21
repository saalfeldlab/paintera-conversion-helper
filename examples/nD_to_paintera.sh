#!/usr/bin/env bash

# Reduce an nD input to a 3D Paintera source by slicing the non-spatial axes to fixed positions.
INPUT_CONTAINER="${INPUT_CONTAINER:-sample.zarr}"
INPUT_DATASET="${INPUT_DATASET:-volumes/raw}"
OUTPUT_CONTAINER="${OUTPUT_CONTAINER:-paintera.n5}"
OUTPUT_DATASET="${OUTPUT_DATASET:-volumes/raw}"

paintera-convert to-paintera \
  --output-container="$OUTPUT_CONTAINER" \
  --scale 2,2,2 2,2,2 \
  --block-size=64,64,64 \
  --container="$INPUT_CONTAINER" \
    -d "$INPUT_DATASET" \
      --target-dataset="$OUTPUT_DATASET" \
      --type=raw \
      --slice-positions=x,y,z,0,10
