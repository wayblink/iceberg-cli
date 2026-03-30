# Iceberg Inspect CLI

[中文文档](./README.zh-CN.md)

`iceberg-inspect` is a local Iceberg metadata inspection CLI. The current version focuses on local paths, detects V1/V2/V3 metadata automatically, supports a session-based `open` workflow, and provides structural inspection and basic statistics at the table, metadata-version, snapshot, and partition levels.

## Capabilities

- Open a table `metadata/` directory, a single `*.metadata.json` file, or a warehouse root
- Cache the current target in a local session so subsequent commands can omit `--path`
- Detect the table `format-version` automatically
- Inspect local copies of metadata originally referenced from S3 or HDFS, as long as the referenced metadata and manifest files are mirrored locally
- Show table structure, snapshots, metadata log, manifests, and partitions
- Compute table-level, metadata-version-level, snapshot-level, and partition-level statistics
- Scan and discover multiple tables under a warehouse
- Support both terminal output and JSON output; `--json` is a shortcut for `--format json`
- Support `--limit` and `--sort-by` for `show manifests` and `show partitions`

## JSON Schema

All `--json` and `--format json` outputs use the same envelope:

```json
{
  "resultType": "show-manifests",
  "target": {
    "path": "/abs/path/to/table",
    "formatVersion": 2
  },
  "request": {
    "scope": "current",
    "groupBy": "table",
    "mode": "auto"
  },
  "summary": {
    "rowCount": 1
  },
  "rows": []
}
```

Notes:

- `resultType`: result category such as `table`, `scan-warehouse`, or `show-snapshots`
- `target`: analysis target; always includes `path`, and includes `formatVersion` when it can be resolved
- `request`: normalized command parameters; optional unset fields are omitted instead of emitted as `null`
- `summary`: summary fields; always includes `rowCount`
- `rows`: detail records
- Local filesystem paths are always serialized as plain strings, not `file:///...` URIs

## Project Layout

```text
iceberg-cli/
├── .mvn/
├── bin/
├── pom.xml
├── src/
├── README.md
└── README.zh-CN.md
```

Run commands from the `iceberg-cli/` directory by default.

## Build And Test

Requirements:

- Java 11+
- Maven 3.9+

Run unit tests:

```bash
mvn -s .mvn/settings.xml test
```

Run full verification:

```bash
mvn -s .mvn/settings.xml verify
```

Build the executable fat jar:

```bash
mvn -s .mvn/settings.xml package
```

## Running

The preferred entrypoint is the bundled launcher script:

```bash
./bin/iceberg-inspect --help
```

You can also inspect help for any subcommand:

```bash
./bin/iceberg-inspect open --help
./bin/iceberg-inspect stat table --help
./bin/iceberg-inspect show manifests --help
```

The script automatically locates `target/iceberg-inspect-*.jar`. If the jar does not exist yet, it will tell you to run `mvn -s .mvn/settings.xml package` first.

To install it as a local command:

```bash
mvn -s .mvn/settings.xml package
./bin/install-local.sh
```

By default the installer writes the launcher to `~/.local/bin/iceberg-inspect` and copies the packaged jar to `~/.local/lib/iceberg-inspect/`. The installed command runs the copied jar, so it no longer depends on the source repository after installation. If `~/.local/bin` is not on your `PATH`, add this line to `~/.zshrc` or `~/.bashrc`:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

You can also install to a custom directory:

```bash
./bin/install-local.sh /usr/local/bin
```

That layout installs the launcher to `/usr/local/bin/iceberg-inspect` and the jar to `/usr/local/lib/iceberg-inspect/`.

If you built with a custom Maven output directory, point the installer at it:

```bash
PROJECT_BUILD_DIRECTORY=/tmp/iceberg-inspect-target ./bin/install-local.sh
```

After installation:

```bash
iceberg-inspect --help
```

You can still run the shaded jar directly:

```bash
cd iceberg-cli
java -jar target/iceberg-inspect-0.1.0-SNAPSHOT.jar --help
```

During development, Maven can launch the CLI directly:

```bash
mvn -s .mvn/settings.xml exec:java \
  -Dexec.mainClass=com.wayblink.iceberg.cli.IcebergInspectApplication \
  -Dexec.args="--help"
```

## Examples

Open a table and inspect the current session:

```bash
iceberg-inspect open /data/warehouse/db/table/metadata
iceberg-inspect current
```

Show structural metadata:

```bash
iceberg-inspect show table
iceberg-inspect show snapshots
iceberg-inspect show metadata-log
iceberg-inspect show manifests
iceberg-inspect show partitions
iceberg-inspect show manifests --json --limit 10 --sort-by deleted-files
iceberg-inspect show partitions --json --limit 20 --sort-by partition
```

Show statistics:

```bash
iceberg-inspect stat table
iceberg-inspect stat table --json
iceberg-inspect stat table --scope history --group-by snapshot --format json
iceberg-inspect stat table --scope history --group-by partition --format json
```

Scan a warehouse:

```bash
iceberg-inspect open /data/warehouse
iceberg-inspect scan warehouse --json
iceberg-inspect use table db/table_a
iceberg-inspect stat warehouse --group-by table
```

## Git

`iceberg-cli/` can be maintained as an independent git repository.

## Scope

The current version only supports the local filesystem. The next step is to extend the existing `FileIO` abstraction for HDFS and S3.
