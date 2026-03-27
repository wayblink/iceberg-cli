package com.wayblink.iceberg.discovery;

import java.nio.file.Path;
import java.util.Objects;

public final class ResolvedTarget {

  private final Path inputPath;
  private final ResolvedTargetType type;
  private final Path tableRoot;
  private final Path metadataRoot;
  private final Path currentMetadataFile;

  public ResolvedTarget(
      Path inputPath,
      ResolvedTargetType type,
      Path tableRoot,
      Path metadataRoot,
      Path currentMetadataFile) {
    this.inputPath = Objects.requireNonNull(inputPath, "inputPath");
    this.type = Objects.requireNonNull(type, "type");
    this.tableRoot = tableRoot;
    this.metadataRoot = Objects.requireNonNull(metadataRoot, "metadataRoot");
    this.currentMetadataFile = currentMetadataFile;
  }

  public Path inputPath() {
    return inputPath;
  }

  public ResolvedTargetType type() {
    return type;
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
}
