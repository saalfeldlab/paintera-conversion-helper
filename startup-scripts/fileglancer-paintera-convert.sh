#!/usr/bin/env bash

# Wrapper around flintstone-paintera-convert.sh with support for use as a Fileglancer app.
#
# It does three things beyond the plain flintstone launch script:
#   1. pulls `--n-nodes N` out of the appended Fileglancer form fields and re-emits it as
#      flintstone's first argument (before `--`), where flintstone expects the node count
#   2. reports the Spark master web UI URL as soon as the master logs it, so it shows up
#      in the Fileglancer job log
#   3. blocks until the conversion actually finishes, so the Fileglancer job's lifetime and
#      exit status track the real cluster work instead of returning right after submission
#
# flintstone itself is fire-and-forget: it bsub-submits a master -> url -> workers -> driver
# -> shutdown LSF chain and returns. We locate the run from flintstone's own output, then
# poll the shared-filesystem logs and the driver job's LSF state.
#
# usage: fileglancer-paintera-convert.sh <subcommand> --n-nodes N [paintera-convert args...]

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SUBCOMMAND="${1:?missing subcommand (to-paintera|to-scalar)}"
shift

# how often to poll, and how long to wait for the web UI URL before giving up on it (the
# conversion keeps being waited on regardless)
POLL_INTERVAL="${FG_POLL_INTERVAL:-30}"
URL_TIMEOUT="${FG_URL_TIMEOUT:-1800}"
# if the driver job never becomes visible to bjobs, bail after this many polls
MAX_UNSEEN_POLLS="${FG_MAX_UNSEEN_POLLS:-20}"

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

USER_NAME="${USER:-$(id -un)}"

SUBMIT_LOG="$(mktemp "${TMPDIR:-/tmp}/fg-paintera-submit.XXXXXX")"
WATCH_PID=""
cleanup() {
    [[ -n "$WATCH_PID" ]] && kill "$WATCH_PID" 2>/dev/null
    rm -f "$SUBMIT_LOG"
}
trap cleanup EXIT

echo "[fileglancer] submitting '$SUBCOMMAND' on $N_NODES worker node(s) via flintstone ..."

# run flintstone (builds the jar if needed, then queues the LSF jobs and returns); show and
# capture its output so we can find the run directory it created
"$SCRIPT_DIR/flintstone-paintera-convert.sh" "$N_NODES" -- "$SUBCOMMAND" "${PAINTERA_ARGS[@]}" 2>&1 | tee "$SUBMIT_LOG"
submit_rc=${PIPESTATUS[0]}
if [[ "$submit_rc" -ne 0 ]]; then
    echo "[fileglancer] flintstone submission failed (exit $submit_rc)" 1>&2
    exit "$submit_rc"
fi

# spark-janelia prints `grep "Bound MasterWebUI to" <run>/logs/01-master.log`; pull that path
MASTER_LOG="$(grep -oE '/[^"[:space:]]+/logs/01-master\.log' "$SUBMIT_LOG" | head -1)"
if [[ -z "$MASTER_LOG" ]]; then
    echo "[fileglancer] no cluster run detected in the flintstone output; nothing to wait for." 1>&2
    exit 0
fi

RUN_DIR="$(dirname "$(dirname "$MASTER_LOG")")"
DRIVER_LOG="$RUN_DIR/logs/04-driver.log"
DRIVER_JOB="spark_${USER_NAME}_$(basename "$RUN_DIR")_dr"
echo "[fileglancer] run directory: $RUN_DIR"
echo "[fileglancer] driver job:    $DRIVER_JOB"

# report the master web UI URL once the master JVM logs it (it boots minutes after submit)
watch_url() {
    local waited=0 url=""
    while (( waited < URL_TIMEOUT )); do
        if [[ -f "$MASTER_LOG" ]]; then
            url="$(grep -oE 'Bound MasterWebUI to.*http://[^ ]+' "$MASTER_LOG" 2>/dev/null | grep -oE 'http://[^ ]+' | head -1)"
            if [[ -n "$url" ]]; then
                echo "[fileglancer] Spark master web UI: $url"
                return 0
            fi
        fi
        sleep "$POLL_INTERVAL"
        waited=$(( waited + POLL_INTERVAL ))
    done
    echo "[fileglancer] Spark web UI URL not found within ${URL_TIMEOUT}s (master may still be queued)." 1>&2
}
watch_url &
WATCH_PID=$!

# block until the driver job (the actual paintera-convert run) reaches a terminal state
echo "[fileglancer] waiting for the conversion to finish (polling every ${POLL_INTERVAL}s) ..."
seen=0
unseen_polls=0
final=""
while true; do
    stat="$(bjobs -a -noheader -o stat -J "$DRIVER_JOB" 2>/dev/null | tail -1)"
    case "$stat" in
        DONE) final=DONE; break ;;
        EXIT) final=EXIT; break ;;
        "")
            if (( seen )); then
                final=GONE; break
            fi
            unseen_polls=$(( unseen_polls + 1 ))
            if (( unseen_polls >= MAX_UNSEEN_POLLS )); then
                echo "[fileglancer] driver job '$DRIVER_JOB' never became visible to bjobs; giving up on the wait." 1>&2
                final=UNSEEN; break
            fi
            ;;
        *) seen=1; unseen_polls=0 ;;  # PEND / RUN / PROV / WAIT / *SUSP
    esac
    sleep "$POLL_INTERVAL"
done

kill "$WATCH_PID" 2>/dev/null
wait "$WATCH_PID" 2>/dev/null
WATCH_PID=""

case "$final" in
    DONE)
        echo "[fileglancer] conversion finished successfully (driver job DONE)."
        exit 0 ;;
    EXIT)
        echo "[fileglancer] conversion FAILED (driver job EXIT). Tail of $DRIVER_LOG:" 1>&2
        tail -n 40 "$DRIVER_LOG" >&2 2>/dev/null
        exit 1 ;;
    *)
        echo "[fileglancer] driver job no longer tracked ($final); assuming finished. Check $DRIVER_LOG"
        exit 0 ;;
esac
