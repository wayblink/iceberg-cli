# Iceberg Inspect CLI

[English README](./README.md)

`iceberg-inspect` 是一个本地 Iceberg 元信息分析命令行工具。当前版本聚焦本地路径，支持自动识别 V1/V2/V3 metadata，支持 session 化 `open` 工作流，并提供表、metadata-version、snapshot、partition 四个层级的结构查看与基础统计。

## 当前能力

- 打开单表 `metadata/` 目录、单个 `*.metadata.json` 文件或 warehouse 根目录
- 在本地 session 中缓存 current target，后续命令可省略 `--path`
- 自动识别表的 `format-version`
- 支持解析从 S3 或 HDFS 拷贝到本地的 metadata 镜像目录，只要被引用的 metadata 和 manifest 文件也一并拷到本地
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

## 项目目录

```text
iceberg-cli/
├── .mvn/
├── bin/
├── pom.xml
├── src/
├── README.md
└── README.zh-CN.md
```

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

## Git

`iceberg-cli/` 可以作为独立 git 仓库持续演进。

## 当前范围

当前版本只支持本地文件系统读取。后续可沿着现有 `FileIO` 抽象扩展 HDFS 和 S3。
