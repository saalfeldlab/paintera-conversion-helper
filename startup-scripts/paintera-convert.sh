#!/usr/bin/env bash

# paintera-convert wrapper script.
#
# This script builds the jar once, and explicitly excludes the default `spark-provided` profile (`-P=-spark-provided`).
# This ensure Spark is bundled onto the classpath instead of being supplied by a cluster SPARK_HOME.
# paintera-convert then runs on a local Spark master (local[*] by default).
#
# All arguments are passed directly to paintera-convert.
#
# usage: local-paintera-convert.sh <paintera-convert args>
#   e.g. local-paintera-convert.sh to-paintera --container=in.zarr -d labels/s0 \
#          --output-container=out.n5 --target-dataset=labels --type=label \
#          --scale 2,2,2 2,2,2 --block-size=32,32,32
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CLASS="${CLASS:-org.janelia.saalfeldlab.conversion.PainteraConvert}"
LIB_DIR="${LIB_DIR:-$PROJECT_ROOT/target/dependency}"

if [[ "$#" -eq 0 ]]; then
    echo "usage: $(basename "$0") <paintera-convert args>" 1>&2
    echo "   e.g. $(basename "$0") to-scalar -i in.n5 -I labels -o out.zarr --xyz-unit nm --scale 2,2,2" 1>&2
    exit 1
fi

find_main_jar() {
    local jar
    for jar in "$LIB_DIR"/paintera-conversion-helper-*.jar; do
        [[ -f "$jar" ]] || continue
        case "$jar" in
            *-sources.jar|*-javadoc.jar|*-tests.jar) continue ;;
        esac
        printf '%s\n' "$jar"
        return 0
    done
}

has_bundled_spark() {
    local jar
    for jar in "$LIB_DIR"/spark-core_2.12-*.jar; do
        [[ -f "$jar" ]] && return 0
    done
    return 1
}

MAIN_JAR="$(find_main_jar)"

# build once; also (re)build if a previous cluster build left the lib dir without a bundled Spark
if [[ ! -f "$MAIN_JAR" ]] || ! has_bundled_spark; then
    export MAVEN_OPTS="-XX:ActiveProcessorCount=4 -XX:MaxRAMPercentage=25 ${MAVEN_OPTS:-}"
    ( cd "$PROJECT_ROOT" && ./mvnw package -DskipTests -P=-spark-provided ${MAVEN_ARGS:-} )
    MAIN_JAR="$(find_main_jar)"
fi

if [[ ! -f "$MAIN_JAR" ]]; then
    echo "error: main jar not found in $LIB_DIR after build" 1>&2
    exit 1
fi

JAVACMD="java"
[[ -n "${JAVA_HOME:-}" && -x "$JAVA_HOME/bin/java" ]] && JAVACMD="$JAVA_HOME/bin/java"

JVM_ARGS="${JVM_ARGS:---add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -XX:SoftRefLRUPolicyMSPerMB=1}"

exec "$JAVACMD" $JVM_ARGS -cp "$LIB_DIR/*" "$CLASS" "$@"
