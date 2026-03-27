package com.wayblink.iceberg.testsupport;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IcebergTableFixturesTest {

  @TempDir
  Path tempDir;

  @Test
  void createsPartitionedTableWithSnapshotsAndMetadata() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir);

    assertTrue(Files.isDirectory(tablePath.resolve("metadata")));
    try (Stream<Path> files = Files.list(tablePath.resolve("metadata"))) {
      assertTrue(files.anyMatch(path -> path.getFileName().toString().endsWith(".metadata.json")));
    }
  }
}
