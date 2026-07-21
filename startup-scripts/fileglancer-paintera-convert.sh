#!/usr/bin/env bash

# Adapter so Fileglancer can drive flintstone-paintera-convert.sh.
#
# Fileglancer appends every form field AFTER the command, but flintstone needs the
# worker node count as its FIRST argument, before `--`. This pulls `--n-nodes N` out
# of the appended args and re-emits it in the position flintstone expects, leaving the
# remaining paintera-convert args in their original order.
#
# usage: fileglancer-paintera-convert.sh <subcommand> --n-nodes N [paintera-convert args...]

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SUBCOMMAND="${1:?missing subcommand (to-paintera|to-scalar)}"
shift

N_NODES=""
PAINTERA_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --n-nodes) N_NODES="$2"; shift 2 ;;
        --n-nodes=*) N_NODES="${1#*=}"; shift ;;
        *) PAINTERA_ARGS+=("$1"); shift ;;
    esac
done

if [[ -z "$N_NODES" ]]; then
    echo "error: --n-nodes is required" 1>&2
    exit 1
fi

exec "$SCRIPT_DIR/flintstone-paintera-convert.sh" \
  "$N_NODES" -- "$SUBCOMMAND" "${PAINTERA_ARGS[@]}"
