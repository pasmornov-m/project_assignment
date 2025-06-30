#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_DIR="${SCRIPT_DIR}/spark_jars"

mkdir -p "$JAR_DIR"

curl -L -o "$JAR_DIR/postgresql-42.7.5.jar" \
  https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.5/postgresql-42.7.5.jar
