# Iceberg Inspect CLI

[English README](./README.md)

`iceberg-inspect` 是一个面向 session 的 Iceberg 元信息分析命令行工具，支持本地路径、HDFS 和 S3A。它能够自动识别 V1/V2/V3 表元信息，并提供表、metadata-version、snapshot、partition 四个层级的结构查看与统计能力。

## 概览

核心能力：

- 打开单表 `metadata/` 目录、单个 `*.metadata.json` 文件或 warehouse 根目录
- 在本地 session 中缓存 current target，后续命令可省略 `--path`
- 查看表结构、snapshot、metadata log、manifest、partition
- 按表、metadata version、snapshot、partition 输出统计信息
- 扫描 warehouse 并通过 `use table` 切换当前表
- 通过 Hadoop `FileIO` 读取本地文件系统、HDFS 和 S3A
- 同时支持终端输出和 `--json` 结构化输出

支持的存储后端：

- 本地文件系统：普通路径或不带 URI scheme 的路径
- HDFS：`hdfs://...`
- S3A：`s3a://...`

说明：

- 工具会显式拒绝 `s3://`，请使用 `s3a://`。
- 如果路径本身已经带了受支持的 URI scheme，`--fs` 可以省略。
- `open` 会把当前存储配置一并写入 session，后续 `show`、`scan`、`stat` 可以同时省略 `--path` 和存储参数。

## 快速开始

先构建可执行 jar：

```bash
mvn -s .mvn/settings.xml package
```

优先使用仓库内自带启动脚本：

```bash
./bin/iceberg-inspect --help
```

打开一张本地表，并复用当前 session：

```bash
iceberg-inspect open /data/warehouse/db/orders/metadata
iceberg-inspect current
iceberg-inspect show table
iceberg-inspect stat table --json
```

## 常见工作流

### 本地表

```bash
iceberg-inspect open /data/warehouse/db/table/metadata
iceberg-inspect show snapshots
iceberg-inspect show manifests --limit 10
iceberg-inspect stat table --scope history --group-by snapshot --json
```

### HDFS 表

前置条件：

- 目标路径使用 `hdfs://`
- 本机可以访问正确的 `core-site.xml` 和 `hdfs-site.xml`

示例：

```bash
iceberg-inspect open hdfs://nameservice1/warehouse/db/table/metadata \
  --fs hdfs \
  --hadoop-conf-dir /etc/hadoop/conf
iceberg-inspect current
iceberg-inspect show table
iceberg-inspect stat table --scope history --group-by partition --json
```

提示：

- 如果路径已经以 `hdfs://` 开头，`--fs hdfs` 可以省略。
- 如果 nameservice 解析或 HA 配置失败，优先检查 `--hadoop-conf-dir` 是否指向了正确的集群配置目录。

### S3A 或 MinIO 表

```bash
iceberg-inspect open s3a://warehouse-bucket/db/table/metadata \
  --fs s3a \
  --s3-endpoint http://minio:9000 \
  --s3-region us-east-1 \
  --s3-path-style
iceberg-inspect show manifests --json --limit 10 --sort-by deleted-files
iceberg-inspect stat table --json
```

### Warehouse 扫描

```bash
iceberg-inspect open /data/warehouse
iceberg-inspect scan warehouse --json
iceberg-inspect use table db/table_a
iceberg-inspect stat table
```

## JSON 输出

所有 `--json` 和 `--format json` 输出统一使用同一套 envelope：

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

字段说明：

- `resultType`：结果类型，例如 `table`、`scan-warehouse`、`show-snapshots`
- `target`：解析后的目标对象，包含 `path`，在可识别时也包含 `formatVersion`
- `request`：归一化后的命令参数；未设置的可选字段会被省略
- `summary`：摘要信息，固定包含 `rowCount`
- `rows`：明细记录

本地文件系统路径会序列化成普通字符串，不会输出成 `file:///...` URI。

## 构建、测试与安装

要求：

- Java 11+
- Maven 3.9+

执行单元测试：

```bash
mvn -s .mvn/settings.xml test
```

执行完整验证：

```bash
mvn -s .mvn/settings.xml verify
```

`verify` 包含以下集成测试：

- 基于 `MiniDFSCluster` 的 HDFS 集成测试
- 基于 MinIO + Testcontainers 的 S3A 集成测试

说明：

- 如果当前机器没有 Docker，S3A 集成测试会自动跳过。
- 如果当前环境限制 Maven 下载依赖，建议先在可联网环境执行一次 `verify`。

安装为本地命令：

```bash
mvn -s .mvn/settings.xml package
./bin/install-local.sh
```

默认会把启动脚本安装到 `~/.local/bin/iceberg-inspect`，并把打包后的 jar 复制到 `~/.local/lib/iceberg-inspect/`。

如果 `~/.local/bin` 不在 `PATH` 中，加入：

```bash
export PATH="$HOME/.local/bin:$PATH"
```

也可以安装到自定义目录：

```bash
./bin/install-local.sh /usr/local/bin
```

开发期直接运行 jar：

```bash
java -jar target/iceberg-inspect-0.1.0-SNAPSHOT.jar --help
```

或者通过 Maven 启动：

```bash
mvn -s .mvn/settings.xml exec:java \
  -Dexec.mainClass=com.wayblink.iceberg.cli.IcebergInspectApplication \
  -Dexec.args="--help"
```

## Git

`iceberg-cli/` 可以作为独立 git 仓库持续演进。
