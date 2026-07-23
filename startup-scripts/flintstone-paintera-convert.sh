#!/usr/bin/env bash

# Launch paintera-convert on the Janelia cluster via spark-janelia/flintstone.sh.
#
# If the jar is not built, it will build the jar (once).
#
# Args before `--` go to flintstone.
# Args after `--` go to paintera-convert.
#
# e.g.:
# ./paintera-convert-janelia.sh <flintstone args> -- <paintera-convert args>
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export SPARK_HOME="${SPARK_HOME:-/misc/local/spark-versions/spark-3.4.1}"
export JAVA_HOME="${JAVA_HOME:-/misc/sc/jdks/zulu17.38.21-ca-jdk17.0.5-linux_x64}"

SPARK_JANELIA="${SPARK_JANELIA:-$SCRIPT_DIR/spark-janelia}"
CLASS="${CLASS:-org.janelia.saalfeldlab.conversion.PainteraConvert}"

usage() {
    cat <<EOF
usage: $(basename "$0") <flintstone args> -- <paintera-convert args>

Submits paintera-convert to the Janelia cluster via flintstone/spark-janelia.
Provides classpath and Main Class to flintstone, so they should NOT
be additionally specified.

Args before \`--' go to flintstone;
Args after \`--' go to paintera-convert.

Example:
  $(basename "$0") 5 -- to-paintera --container=in.zarr -d labels/s0 \\
    --output-container=out.n5 --target-dataset=labels --type=label

help:
  $(basename "$0") --help                  flintstone help
  $(basename "$0") -- --help               paintera-convert help
  $(basename "$0") -- to-paintera --help   paintera-convert to-paintera help
  $(basename "$0") -- to-scalar --help     paintera-convert to-scalar help
EOF
}

FLINTSTONE_ARGS=()
PAINTERA_ARGS=()
saw_separator=0
for arg in "$@"; do
    if [[ "$saw_separator" -eq 0 && "$arg" == "--" ]]; then
        saw_separator=1
        continue
    fi
    if [[ "$saw_separator" -eq 0 ]]; then
        FLINTSTONE_ARGS+=("$arg")
    else
        PAINTERA_ARGS+=("$arg")
    fi
done

# no `--`: all args go to flintstone
if [[ "$saw_separator" -eq 0 ]]; then
    if [[ "$#" -eq 0 ]]; then
        usage 1>&2
        exit 1
    fi
    exec "$SPARK_JANELIA/flintstone.sh" "$@"
fi

if [[ "${#PAINTERA_ARGS[@]}" -eq 0 ]]; then
    echo "error: no paintera-convert arguments after '--'" 1>&2
    exit 1
fi

# --help after `--` runs paintera-convert locally, not on the cluster
help_requested=0
for arg in "${PAINTERA_ARGS[@]}"; do
    [[ "$arg" == -h || "$arg" == --help ]] && help_requested=1
done

# an actual submission needs the flintstone worker node count
if [[ "$help_requested" -eq 0 && "${#FLINTSTONE_ARGS[@]}" -eq 0 ]]; then
    echo "error: missing flintstone args before '--' first args (node count) is required," \
         "e.g. $(basename "$0") 5 -- to-paintera ..." 1>&2
    exit 1
fi

# build the jar unless target/dependency is already populated
LIB_DIR="${LIB_DIR:-$PROJECT_ROOT/target/dependency}"

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

# a bundled Spark jar means a prior local build populated the dir with compile-scope Spark; prepend
# `clean` to the build so it starts fresh with -Pspark-provided, since a bundled Spark would conflict
# with the cluster's SPARK_HOME
has_bundled_spark() {
    local jar
    for jar in "$LIB_DIR"/spark-core_2.12-*.jar; do
        [[ -f "$jar" ]] && return 0
    done
    return 1
}
clean_first=0
if has_bundled_spark; then
    echo "cleaning a prior local (Spark-bundled) build before the cluster build" 1>&2
    clean_first=1
fi

MAIN_JAR="${MAIN_JAR:-$(find_main_jar)}"

if [[ "$clean_first" -eq 1 || ! -f "$MAIN_JAR" ]]; then
    export MAVEN_OPTS="-XX:ActiveProcessorCount=4 -XX:MaxRAMPercentage=25 ${MAVEN_OPTS:-}"
    goals=(package)
    [[ "$clean_first" -eq 1 ]] && goals=(clean package)
    "$PROJECT_ROOT/mvnw" "${goals[@]}" -DskipTests -Pspark-provided ${MAVEN_ARGS:-}
    MAIN_JAR="$(find_main_jar)"
fi

if [[ ! -f "$MAIN_JAR" ]]; then
    echo "error: main jar not found in $LIB_DIR after build" 1>&2
    exit 1
fi

JVM_ARGS="${JVM_ARGS:---add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -XX:SoftRefLRUPolicyMSPerMB=1}"

# help for paintera-convert runs locally
if [[ "$help_requested" -eq 1 ]]; then
    JAVACMD="java"
    [[ -x "$JAVA_HOME/bin/java" ]] && JAVACMD="$JAVA_HOME/bin/java"
    exec "$JAVACMD" $JVM_ARGS -cp "$LIB_DIR/*" "$CLASS" "${PAINTERA_ARGS[@]}"
fi

# append our confs so a caller-provided SUBMIT_ARGS is preserved
export SUBMIT_ARGS="${SUBMIT_ARGS:-}\
 --conf spark.driver.extraClassPath='$LIB_DIR/*'\
 --conf spark.executor.extraClassPath='$LIB_DIR/*'\
 --conf spark.driver.extraJavaOptions='$JVM_ARGS'\
 --conf spark.executor.extraJavaOptions='$JVM_ARGS'"

exec "$SPARK_JANELIA/flintstone.sh" \
  "${FLINTSTONE_ARGS[@]}" "$MAIN_JAR" "$CLASS" \
  "${PAINTERA_ARGS[@]}"
