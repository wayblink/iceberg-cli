package com.wayblink.iceberg.discovery;

import java.util.Objects;

public final class ResolvedTarget {

  private final String inputPath;
  private final ResolvedTargetType type;
  private final String tableRoot;
  private final String metadataRoot;
  private final String currentMetadataFile;

  public ResolvedTarget(
      String inputPath,
      ResolvedTargetType type,
      String tableRoot,
      String metadataRoot,
      String currentMetadataFile) {
    this.inputPath = Objects.requireNonNull(inputPath, "inputPath");
    this.type = Objects.requireNonNull(type, "type");
    this.tableRoot = tableRoot;
    this.metadataRoot = Objects.requireNonNull(metadataRoot, "metadataRoot");
    this.currentMetadataFile = currentMetadataFile;
  }

  public String inputPath() {
    return inputPath;
  }

  public ResolvedTargetType type() {
    return type;
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
}
