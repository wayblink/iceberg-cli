# Iceberg Inspect CLI

[中文文档](./README.zh-CN.md)

`iceberg-inspect` is an Iceberg metadata inspection CLI for local paths, HDFS, and S3A. It detects V1/V2/V3 metadata automatically, supports a session-based `open` workflow, and provides structural inspection and basic statistics at the table, metadata-version, snapshot, and partition levels.

## Quick Start

Build the executable jar:

```bash
mvn -s .mvn/settings.xml package
```

Use the bundled launcher:

```bash
./bin/iceberg-inspect --help
```

Open a local table and reuse the current session:

```bash
iceberg-inspect open /data/warehouse/db/orders/metadata
iceberg-inspect current
iceberg-inspect show table
iceberg-inspect stat table --json
```

## Capabilities

- Open a table `metadata/` directory, a single `*.metadata.json` file, or a warehouse root
- Cache the current target in a local session so subsequent commands can omit `--path`
- Detect the table `format-version` automatically
- Read tables from the local filesystem, HDFS, and S3A through Hadoop `FileIO`
- Continue to inspect local copies of metadata originally referenced from S3 or HDFS, as long as the referenced metadata and manifest files are mirrored locally
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

`verify` now includes integration tests for:

- HDFS via `MiniDFSCluster`
- S3A via MinIO and Testcontainers

Notes:

- The S3A integration test is skipped automatically when Docker is unavailable.
- If your environment blocks Maven dependency downloads, run `verify` once in a network-enabled environment first.

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

## Storage Backends

`iceberg-inspect` now supports three storage backends:

- Local filesystem: explicit local paths or paths without a URI scheme
- HDFS: `hdfs://...`, optionally combined with `--fs hdfs --hadoop-conf-dir /etc/hadoop/conf`
- S3A: `s3a://...`, optionally combined with `--fs s3a --s3-endpoint ... --s3-region ... --s3-path-style`

Notes:

- `--fs` is optional when the path already has a supported URI scheme.
- `open` persists the current storage settings into the session, so follow-up `show`, `scan`, and `stat` commands can omit both `--path` and storage flags.
- `s3://` is intentionally rejected. Use `s3a://` so Hadoop and Iceberg resolve the correct filesystem implementation.

## HDFS Usage

Use HDFS when your Iceberg metadata and manifest files are stored behind Hadoop `FileSystem`.

Prerequisites:

- The target path should use the `hdfs://` scheme.
- `core-site.xml` and `hdfs-site.xml` should be available locally.
- Pass `--hadoop-conf-dir` when the required Hadoop client configuration is not already available from the environment.

Open a table from HDFS:

```bash
iceberg-inspect open hdfs://nameservice1/warehouse/db/table/metadata \
  --fs hdfs \
  --hadoop-conf-dir /etc/hadoop/conf
```

Inspect the current session and basic structure:

```bash
iceberg-inspect current
iceberg-inspect show table
iceberg-inspect show snapshots
iceberg-inspect show manifests --limit 10
```

Analyze statistics without repeating storage flags:

```bash
iceberg-inspect stat table
iceberg-inspect stat table --scope history --group-by snapshot --json
iceberg-inspect stat table --scope history --group-by partition --json
```

Open an HDFS warehouse and scan multiple tables:

```bash
iceberg-inspect open hdfs://nameservice1/warehouse \
  --fs hdfs \
  --hadoop-conf-dir /etc/hadoop/conf
iceberg-inspect scan warehouse --json
iceberg-inspect use table db/orders
iceberg-inspect stat table
```

Notes:

- After `open`, the HDFS backend and Hadoop config directory are stored in the local session.
- If the path already starts with `hdfs://`, `--fs hdfs` is optional; keeping it can make scripts more explicit.
- If name service resolution or HA settings fail, check that the selected `--hadoop-conf-dir` contains the correct cluster config.

## S3A Usage

Open a table from S3A or MinIO:

```bash
iceberg-inspect open s3a://warehouse-bucket/db/table/metadata \
  --fs s3a \
  --s3-endpoint http://minio:9000 \
  --s3-region us-east-1 \
  --s3-path-style
iceberg-inspect stat table --json
```

## Git

`iceberg-cli/` can be maintained as an independent git repository.
