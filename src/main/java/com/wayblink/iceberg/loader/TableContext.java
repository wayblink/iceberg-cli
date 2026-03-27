package com.wayblink.iceberg.loader;

import java.nio.file.Path;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

public final class TableContext {

  private final Path tableRoot;
  private final Path metadataRoot;
  private final Path currentMetadataFile;
  private final FileIO fileIO;
  private final TableMetadata metadata;
  private final Table table;

  public TableContext(
      Path tableRoot,
      Path metadataRoot,
      Path currentMetadataFile,
      FileIO fileIO,
      TableMetadata metadata,
      Table table) {
    this.tableRoot = tableRoot;
    this.metadataRoot = metadataRoot;
    this.currentMetadataFile = currentMetadataFile;
    this.fileIO = fileIO;
    this.metadata = metadata;
    this.table = table;
  }

  public Path tableRoot() {
    return tableRoot;
  }

  public Path metadataRoot() {
    return metadataRoot;
  }

  public Path currentMetadataFile() {
    return currentMetadataFile;
  }

  public FileIO fileIO() {
    return fileIO;
  }

  public TableMetadata metadata() {
    return metadata;
  }

  public Table table() {
    return table;
  }
}
