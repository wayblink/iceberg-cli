# Iceberg Inspect CLI

[中文文档](./README.zh-CN.md)

`iceberg-inspect` is a session-oriented CLI for inspecting Iceberg metadata from local paths, HDFS, and S3A. It detects V1/V2/V3 table metadata automatically and exposes structure and statistics at the table, metadata-version, snapshot, and partition levels.

## Overview

Key capabilities:

- Open a table `metadata/` directory, a single `*.metadata.json` file, or a warehouse root
- Cache the current target in a local session so later commands can omit `--path`
- Inspect table structure, snapshots, metadata log, manifests, and partitions
- Compute statistics by table, metadata version, snapshot, and partition
- Scan warehouse roots and switch tables with `use table`
- Read from local paths, HDFS, and S3A through Hadoop `FileIO`
- Output either terminal-friendly text or structured JSON with `--json`

Supported storage backends:

- Local filesystem: plain paths or paths without a URI scheme
- HDFS: `hdfs://...`
- S3A: `s3a://...`

Notes:

- `s3://` is intentionally rejected. Use `s3a://`.
- If the path already has a supported URI scheme, `--fs` is optional.
- `open` persists the current storage settings into the session, so follow-up `show`, `scan`, and `stat` commands can omit both `--path` and storage flags.

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

## Common Workflows

### Local table

```bash
iceberg-inspect open /data/warehouse/db/table/metadata
iceberg-inspect show snapshots
iceberg-inspect show manifests --limit 10
iceberg-inspect stat table --scope history --group-by snapshot --json
```

### HDFS table

Prerequisites:

- The target path uses `hdfs://`
- The local machine can access the correct `core-site.xml` and `hdfs-site.xml`

Example:

```bash
iceberg-inspect open hdfs://nameservice1/warehouse/db/table/metadata \
  --fs hdfs \
  --hadoop-conf-dir /etc/hadoop/conf
iceberg-inspect current
iceberg-inspect show table
iceberg-inspect stat table --scope history --group-by partition --json
```

Tips:

- If the path already starts with `hdfs://`, `--fs hdfs` is optional.
- If nameservice or HA resolution fails, check that `--hadoop-conf-dir` points at the right cluster config.

### S3A or MinIO table

```bash
iceberg-inspect open s3a://warehouse-bucket/db/table/metadata \
  --fs s3a \
  --s3-endpoint http://minio:9000 \
  --s3-region us-east-1 \
  --s3-path-style
iceberg-inspect show manifests --json --limit 10 --sort-by deleted-files
iceberg-inspect stat table --json
```

### Warehouse scan

```bash
iceberg-inspect open /data/warehouse
iceberg-inspect scan warehouse --json
iceberg-inspect use table db/table_a
iceberg-inspect stat table
```

## JSON Output

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

Field meanings:

- `resultType`: result category such as `table`, `scan-warehouse`, or `show-snapshots`
- `target`: resolved analysis target; includes `path` and, when available, `formatVersion`
- `request`: normalized command parameters; unset optional fields are omitted
- `summary`: summary fields; always includes `rowCount`
- `rows`: detailed result rows

Local filesystem paths are serialized as plain strings, not `file:///...` URIs.

## Build, Test, and Install

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

`verify` includes integration tests for:

- HDFS via `MiniDFSCluster`
- S3A via MinIO and Testcontainers

Notes:

- The S3A integration test is skipped automatically when Docker is unavailable.
- If your environment blocks Maven dependency downloads, run `verify` once in a network-enabled environment first.

Install the command locally:

```bash
mvn -s .mvn/settings.xml package
./bin/install-local.sh
```

By default, the launcher is installed to `~/.local/bin/iceberg-inspect` and the packaged jar is copied to `~/.local/lib/iceberg-inspect/`.

If `~/.local/bin` is not on your `PATH`, add:

```bash
export PATH="$HOME/.local/bin:$PATH"
```

You can also install to a custom directory:

```bash
./bin/install-local.sh /usr/local/bin
```

For direct execution during development:

```bash
java -jar target/iceberg-inspect-0.1.0-SNAPSHOT.jar --help
```

Or via Maven:

```bash
mvn -s .mvn/settings.xml exec:java \
  -Dexec.mainClass=com.wayblink.iceberg.cli.IcebergInspectApplication \
  -Dexec.args="--help"
```

## Git

`iceberg-cli/` can be maintained as an independent git repository.
