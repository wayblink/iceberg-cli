package com.wayblink.iceberg.testsupport;

import java.nio.file.Path;

final class FixturePaths {

  private FixturePaths() {
  }

  static Path dataDir(Path tablePath) {
    return tablePath.resolve("data");
  }

  static Path metadataDir(Path tablePath) {
    return tablePath.resolve("metadata");
  }
}
