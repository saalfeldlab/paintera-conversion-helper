#!/bin/sh

PREFIX="${PREFIX:-$HOME/local}"
BIN_DIR="${PREFIX}/bin"
JRUN_PATH="${BIN_DIR}/jrun"
JRUN_URL="https://raw.githubusercontent.com/ctrueden/jrun/12b21d1427c661bd552163166e0d14e1a45e6658/jrun"

if [ ! -f "${JRUN_PATH}" ]; then
    curl "${JRUN_URL}" -o "${JRUN_PATH}"
fi

chmod u+x "${JRUN_PATH}"

cp paintera-conversion-helper "${BIN_DIR}"
