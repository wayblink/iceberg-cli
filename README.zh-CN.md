# Iceberg Inspect CLI

[English README](./README.md)

`iceberg-inspect` 是一个支持本地路径、HDFS 和 S3A 的 Iceberg 元信息分析命令行工具。它支持自动识别 V1/V2/V3 metadata，支持 session 化 `open` 工作流，并提供表、metadata-version、snapshot、partition 四个层级的结构查看与基础统计。

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

## 当前能力

- 打开单表 `metadata/` 目录、单个 `*.metadata.json` 文件或 warehouse 根目录
- 在本地 session 中缓存 current target，后续命令可省略 `--path`
- 自动识别表的 `format-version`
- 通过 Hadoop `FileIO` 直接读取本地文件系统、HDFS 和 S3A 上的表
- 继续支持解析从 S3 或 HDFS 拷贝到本地的 metadata 镜像目录，只要被引用的 metadata 和 manifest 文件也一并拷到本地
- 查看表结构、snapshot、metadata log、manifest、partition
- 统计表级、metadata-version 级、snapshot 级、partition 级指标
- 扫描 warehouse 下的多张表
- 同时支持终端输出和 JSON 输出，`--json` 可作为 `--format json` 的快捷方式
- `show manifests`、`show partitions` 支持 `--limit` 和 `--sort-by`

## JSON 输出结构

所有 `--json` / `--format json` 输出统一为同一套 envelope：

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

说明：

- `resultType`：结果类型，例如 `table`、`scan-warehouse`、`show-snapshots`
- `target`：分析目标，固定包含 `path`；`formatVersion` 在当前目标可识别时返回
- `request`：归一化后的命令请求参数；未显式指定的可选参数会被省略，不返回 `null`
- `summary`：摘要信息，固定包含 `rowCount`
- `rows`：明细记录数组
- 本地文件系统路径统一输出为普通字符串，不输出 `file:///...` URI

默认在 `iceberg-cli/` 目录下执行命令。

## 构建与测试

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

现在 `verify` 会包含两类集成测试：

- 基于 `MiniDFSCluster` 的 HDFS 集成测试
- 基于 MinIO + Testcontainers 的 S3A 集成测试

说明：

- 如果当前机器没有 Docker，S3A 集成测试会自动跳过。
- 如果你的环境限制 Maven 下载依赖，建议先在可联网环境执行一次 `verify`。

构建可执行 fat jar：

```bash
mvn -s .mvn/settings.xml package
```

## 运行方式

优先使用仓库内自带启动脚本：

```bash
./bin/iceberg-inspect --help
```

也可以查看任意子命令的独立帮助：

```bash
./bin/iceberg-inspect open --help
./bin/iceberg-inspect stat table --help
./bin/iceberg-inspect show manifests --help
```

脚本会自动定位 `target/iceberg-inspect-*.jar`。如果还没有构建产物，会提示你先执行 `mvn -s .mvn/settings.xml package`。

如果你希望把它安装成真正的本地命令：

```bash
mvn -s .mvn/settings.xml package
./bin/install-local.sh
```

安装脚本默认会把启动脚本写到 `~/.local/bin/iceberg-inspect`，并把打包好的 jar 复制到 `~/.local/lib/iceberg-inspect/`。安装后的命令直接运行这份已安装的 jar，不再依赖源码仓库路径。如果你的 `PATH` 里还没有 `~/.local/bin`，把下面这行加到 `~/.zshrc` 或 `~/.bashrc`：

```bash
export PATH="$HOME/.local/bin:$PATH"
```

也可以指定安装目录：

```bash
./bin/install-local.sh /usr/local/bin
```

这种布局会把启动脚本安装到 `/usr/local/bin/iceberg-inspect`，并把 jar 安装到 `/usr/local/lib/iceberg-inspect/`。

如果你构建时把 Maven 输出目录改到了别处，安装前先指定 `PROJECT_BUILD_DIRECTORY`：

```bash
PROJECT_BUILD_DIRECTORY=/tmp/iceberg-inspect-target ./bin/install-local.sh
```

安装完成后就可以直接运行：

```bash
iceberg-inspect --help
```

也可以直接运行 shaded jar：

```bash
cd iceberg-cli
java -jar target/iceberg-inspect-0.1.0-SNAPSHOT.jar --help
```

开发期也可以直接用 Maven 启动：

```bash
mvn -s .mvn/settings.xml exec:java \
  -Dexec.mainClass=com.wayblink.iceberg.cli.IcebergInspectApplication \
  -Dexec.args="--help"
```

## 使用示例

打开表并查看当前上下文：

```bash
iceberg-inspect open /data/warehouse/db/table/metadata
iceberg-inspect current
```

查看结构信息：

```bash
iceberg-inspect show table
iceberg-inspect show snapshots
iceberg-inspect show metadata-log
iceberg-inspect show manifests
iceberg-inspect show partitions
iceberg-inspect show manifests --json --limit 10 --sort-by deleted-files
iceberg-inspect show partitions --json --limit 20 --sort-by partition
```

输出统计：

```bash
iceberg-inspect stat table
iceberg-inspect stat table --json
iceberg-inspect stat table --scope history --group-by snapshot --format json
iceberg-inspect stat table --scope history --group-by partition --format json
```

扫描 warehouse：

```bash
iceberg-inspect open /data/warehouse
iceberg-inspect scan warehouse --json
iceberg-inspect use table db/table_a
iceberg-inspect stat warehouse --group-by table
```

## 存储后端

`iceberg-inspect` 当前支持三类存储后端：

- 本地文件系统：显式本地路径，或不带 URI scheme 的路径
- HDFS：`hdfs://...`，可选搭配 `--fs hdfs --hadoop-conf-dir /etc/hadoop/conf`
- S3A：`s3a://...`，可选搭配 `--fs s3a --s3-endpoint ... --s3-region ... --s3-path-style`

说明：

- 如果路径本身已经带了受支持的 URI scheme，`--fs` 可以省略。
- `open` 会把当前存储配置一起写进 session，后续 `show`、`scan`、`stat` 可以同时省略 `--path` 和存储参数。
- 工具会显式拒绝 `s3://`。在 Hadoop/Iceberg 语境里请使用 `s3a://`，这样才能命中正确的文件系统实现。

## HDFS 使用方法

当 Iceberg metadata 和 manifest 文件存放在 Hadoop `FileSystem` 上时，使用 HDFS 模式。

前置条件：

- 目标路径使用 `hdfs://` scheme。
- 本机可以拿到对应集群的 `core-site.xml` 和 `hdfs-site.xml`。
- 如果环境变量里没有现成的 Hadoop client 配置，显式传入 `--hadoop-conf-dir`。

打开 HDFS 上的一张表：

```bash
iceberg-inspect open hdfs://nameservice1/warehouse/db/table/metadata \
  --fs hdfs \
  --hadoop-conf-dir /etc/hadoop/conf
```

查看当前 session 和结构信息：

```bash
iceberg-inspect current
iceberg-inspect show table
iceberg-inspect show snapshots
iceberg-inspect show manifests --limit 10
```

在不重复传存储参数的情况下继续做统计：

```bash
iceberg-inspect stat table
iceberg-inspect stat table --scope history --group-by snapshot --json
iceberg-inspect stat table --scope history --group-by partition --json
```

打开 HDFS 上的 warehouse 并扫描多张表：

```bash
iceberg-inspect open hdfs://nameservice1/warehouse \
  --fs hdfs \
  --hadoop-conf-dir /etc/hadoop/conf
iceberg-inspect scan warehouse --json
iceberg-inspect use table db/orders
iceberg-inspect stat table
```

说明：

- 执行 `open` 后，HDFS 后端和 Hadoop 配置目录会一起写入本地 session。
- 如果路径本身已经以 `hdfs://` 开头，`--fs hdfs` 可以省略；保留它通常更利于脚本可读性。
- 如果 nameservice 解析或 HA 配置异常，优先检查 `--hadoop-conf-dir` 是否指向了正确的集群配置目录。

## S3A 使用方法

打开 S3A 或 MinIO 上的一张表：

```bash
iceberg-inspect open s3a://warehouse-bucket/db/table/metadata \
  --fs s3a \
  --s3-endpoint http://minio:9000 \
  --s3-region us-east-1 \
  --s3-path-style
iceberg-inspect stat table --json
```

## Git

`iceberg-cli/` 可以作为独立 git 仓库持续演进。
