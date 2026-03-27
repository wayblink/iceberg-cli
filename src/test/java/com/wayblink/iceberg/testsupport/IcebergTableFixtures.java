package com.wayblink.iceberg.testsupport;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

public final class IcebergTableFixtures {

  private static final Schema PARTITIONED_SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "category", Types.StringType.get()),
      optional(3, "payload", Types.StringType.get()));

  private static final PartitionSpec PARTITIONED_SPEC =
      PartitionSpec.builderFor(PARTITIONED_SCHEMA)
          .identity("category")
          .build();

  private static final AtomicInteger TABLE_COUNTER = new AtomicInteger();

  private IcebergTableFixtures() {
  }

  public static Path createPartitionedTable(Path warehouseRoot) throws IOException {
    Path tablePath = warehouseRoot.resolve("table_" + TABLE_COUNTER.incrementAndGet());
    HadoopTables tables = new HadoopTables(new Configuration());
    Table table = tables.create(
        PARTITIONED_SCHEMA,
        PARTITIONED_SPEC,
        SortOrder.unsorted(),
        Map.of(TableProperties.FORMAT_VERSION, "2"),
        tablePath.toString());

    appendFile(table, tablePath, "category=books", "books-0001.parquet", 128L, 10L);
    appendFile(table, tablePath, "category=books", "books-0002.parquet", 256L, 12L);
    appendFile(table, tablePath, "category=games", "games-0001.parquet", 512L, 20L);
    return tablePath;
  }

  private static void appendFile(
      Table table,
      Path tablePath,
      String partitionPath,
      String fileName,
      long fileSizeInBytes,
      long recordCount)
      throws IOException {
    Path partitionDir = FixturePaths.dataDir(tablePath).resolve(partitionPath);
    Files.createDirectories(partitionDir);
    Path dataFilePath = partitionDir.resolve(fileName);
    if (Files.notExists(dataFilePath)) {
      Files.createFile(dataFilePath);
    }

    DataFile dataFile = DataFiles.builder(table.spec())
        .withPath(dataFilePath.toString())
        .withFormat(FileFormat.PARQUET)
        .withPartitionPath(partitionPath)
        .withFileSizeInBytes(fileSizeInBytes)
        .withRecordCount(recordCount)
        .build();
    table.newAppend().appendFile(dataFile).commit();
  }
}
