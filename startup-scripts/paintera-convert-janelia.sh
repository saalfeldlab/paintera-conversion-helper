#!/usr/bin/env bash

# Launch paintera-convert on the Janelia cluster via flintstone/spark-janelia.
# Builds the spark-provided jar (spark supplied by the cluster) and forwards all
# arguments to paintera-convert, e.g.:
#
#   ./paintera-convert-janelia.sh to-paintera --container=in.zarr -d labels/s0 \
#       --output-container=out.n5 --target-dataset=labels --type=label \
#       --scale 2,2,2 2,2,2 --block-size=32,32,32
#
# Every setting below honors a pre-existing env var, so callers can override
# without editing the script:
#   N_NODES=10 ./paintera-convert-janelia.sh to-paintera ...
#
# The jar is built once; delete target/dependency to force a rebuild.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export SPARK_HOME="${SPARK_HOME:-/misc/local/spark-versions/spark-3.4.1}"
export JAVA_HOME="${JAVA_HOME:-/misc/sc/jdks/zulu17.38.21-ca-jdk17.0.5-linux_x64}"

SPARK_JANELIA="${SPARK_JANELIA:-$SCRIPT_DIR/spark-janelia}"
CLASS="${CLASS:-org.janelia.saalfeldlab.conversion.PainteraConvert}"

# cluster sizing; consumed by flintstone.sh
export N_NODES="${N_NODES:-5}"
export N_CORES_DRIVER="${N_CORES_DRIVER:-4}"
export MIN_WORKERS="${MIN_WORKERS:-1}"

if [[ "$#" -lt 1 ]]; then
    echo "usage: $(basename "$0") <paintera-convert args...>" 1>&2
    exit 1
fi

# build the spark-provided jar unless target/dependency is already populated
LIB_DIR="${LIB_DIR:-$PROJECT_ROOT/target/dependency}"

find_main_jar() {
    ls "$LIB_DIR"/paintera-conversion-helper-*.jar 2>/dev/null \
        | grep -vE '\-(sources|javadoc)\.jar$' | head -1 || true
}

MAIN_JAR="${MAIN_JAR:-$(find_main_jar)}"

if [[ ! -f "$MAIN_JAR" ]]; then
    "$PROJECT_ROOT/mvnw" package -Pspark-provided ${MAVEN_ARGS:-}
    MAIN_JAR="$(find_main_jar)"
fi

if [[ ! -f "$MAIN_JAR" ]]; then
    echo "error: main jar not found in $LIB_DIR after build" 1>&2
    exit 1
fi

JVM_ARGS="${JVM_ARGS:---add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -XX:SoftRefLRUPolicyMSPerMB=1}"

# append our confs so a caller-provided SUBMIT_ARGS is preserved
export SUBMIT_ARGS="${SUBMIT_ARGS:-}\
 --conf spark.driver.extraClassPath='$LIB_DIR/*'\
 --conf spark.executor.extraClassPath='$LIB_DIR/*'\
 --conf spark.driver.extraJavaOptions='$JVM_ARGS'\
 --conf spark.executor.extraJavaOptions='$JVM_ARGS'"

exec "$SPARK_JANELIA/flintstone.sh" "$N_NODES" "$MAIN_JAR" "$CLASS" "$@"
