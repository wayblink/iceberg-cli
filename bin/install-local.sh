#!/bin/sh

set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
INSTALL_DIR=${1:-"$HOME/.local/bin"}
TARGET_COMMAND="$INSTALL_DIR/iceberg-inspect"

mkdir -p "$INSTALL_DIR"

cat > "$TARGET_COMMAND" <<EOF
#!/bin/sh
set -eu
PROJECT_DIR="$PROJECT_DIR"
exec "\$PROJECT_DIR/bin/iceberg-inspect" "\$@"
EOF

chmod +x "$TARGET_COMMAND"

echo "Installed iceberg-inspect to $TARGET_COMMAND"
echo "If needed, add this to your shell profile:"
echo "export PATH=\"$INSTALL_DIR:\$PATH\""
