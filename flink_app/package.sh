#!/usr/bin/env bash
set -euo pipefail

APP_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT="$APP_DIR/machinepulse-flink.zip"
DEPENDENCY_JAR="$APP_DIR/target/pyflink-dependencies.jar"

mvn -f "$APP_DIR/pom.xml" package

if ! jar tf "$DEPENDENCY_JAR" | grep -Fxq \
    'org/apache/hadoop/conf/Configuration.class'; then
    echo "Hadoop Configuration is missing from $DEPENDENCY_JAR" >&2
    exit 1
fi

mkdir -p "$APP_DIR/package/lib"
cp \
    "$APP_DIR/main.py" \
    "$APP_DIR/flink_processor.py" \
    "$APP_DIR/processing.py" \
    "$APP_DIR/package/"
cp "$DEPENDENCY_JAR" "$APP_DIR/package/lib/pyflink-dependencies.jar"

(
    cd "$APP_DIR/package"
    zip -q -r -FS "$OUTPUT" \
        main.py \
        flink_processor.py \
        processing.py \
        kinesis_connector.py \
        lib/pyflink-dependencies.jar
)

echo "Created $OUTPUT"
