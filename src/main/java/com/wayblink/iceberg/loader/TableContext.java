package com.wayblink.iceberg.loader;

import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.FileIO;

public final class TableContext {

  private final String tableRoot;
  private final String metadataRoot;
  private final String currentMetadataFile;
  private final FileIO fileIO;
  private final TableMetadata metadata;
  private final Table table;

  public TableContext(
      String tableRoot,
      String metadataRoot,
      String currentMetadataFile,
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

  public String tableRoot() {
    return tableRoot;
  }

  public String metadataRoot() {
    return metadataRoot;
  }

  public String currentMetadataFile() {
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
