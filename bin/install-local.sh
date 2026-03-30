#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
DEFAULT_BUILD_DIR="$PROJECT_DIR/target"
BUILD_DIR=${PROJECT_BUILD_DIRECTORY:-"$DEFAULT_BUILD_DIR"}
INSTALL_DIR=${1:-"$HOME/.local/bin"}

mkdir -p "$INSTALL_DIR"
INSTALL_DIR=$(CDPATH= cd -- "$INSTALL_DIR" && pwd)
INSTALL_ROOT=$(CDPATH= cd -- "$INSTALL_DIR/.." && pwd)
INSTALL_LIB_DIR="$INSTALL_ROOT/lib/iceberg-inspect"
TARGET_COMMAND="$INSTALL_DIR/iceberg-inspect"

find_jar() {
  SEARCH_DIR=$1

  if [ ! -d "$SEARCH_DIR" ]; then
    return 1
  fi

  find "$SEARCH_DIR" -maxdepth 1 -type f -name 'iceberg-inspect-*.jar' \
    ! -name 'original-*' \
    | sort \
    | tail -n 1
}

JAR_PATH=$(find_jar "$BUILD_DIR" || true)

if [ -z "${JAR_PATH:-}" ] && [ "$BUILD_DIR" != "$DEFAULT_BUILD_DIR" ]; then
  JAR_PATH=$(find_jar "$DEFAULT_BUILD_DIR" || true)
fi

if [ -z "${JAR_PATH:-}" ] || [ ! -f "$JAR_PATH" ]; then
  echo "Missing packaged jar under $BUILD_DIR" >&2
  echo "Run: mvn -s .mvn/settings.xml package" >&2
  echo "If you packaged to a custom build directory, set PROJECT_BUILD_DIRECTORY first." >&2
  exit 1
fi

INSTALLED_JAR_NAME=$(basename "$JAR_PATH")
INSTALLED_JAR="$INSTALL_LIB_DIR/$INSTALLED_JAR_NAME"

mkdir -p "$INSTALL_LIB_DIR"
rm -f "$INSTALL_LIB_DIR"/iceberg-inspect-*.jar
cp "$JAR_PATH" "$INSTALLED_JAR"

cat > "$TARGET_COMMAND" <<EOF
#!/bin/sh

set -eu

SCRIPT_DIR=\$(CDPATH= cd -- "\$(dirname -- "\$0")" && pwd)
INSTALL_ROOT=\$(CDPATH= cd -- "\$SCRIPT_DIR/.." && pwd)
JAR_PATH="\$INSTALL_ROOT/lib/iceberg-inspect/$INSTALLED_JAR_NAME"

if [ ! -f "\$JAR_PATH" ]; then
  echo "Missing installed jar at \$JAR_PATH" >&2
  exit 1
fi

exec java \${JAVA_OPTS:-} -jar "\$JAR_PATH" "\$@"
EOF

chmod +x "$TARGET_COMMAND"

echo "Installed iceberg-inspect to $TARGET_COMMAND"
echo "Installed jar to $INSTALLED_JAR"
echo "If needed, add this to your shell profile:"
echo "export PATH=\"$INSTALL_DIR:\$PATH\""
