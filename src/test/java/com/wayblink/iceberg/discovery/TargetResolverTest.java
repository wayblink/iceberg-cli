package com.wayblink.iceberg.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wayblink.iceberg.loader.VersionDetector;
import com.wayblink.iceberg.testsupport.IcebergTableFixtures;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TargetResolverTest {

  @TempDir
  Path tempDir;

  @Test
  void resolvesMetadataDirectoryAndDetectsFormatVersion() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir);
    Path metadataDir = tablePath.resolve("metadata");

    TargetResolver resolver = new TargetResolver();
    VersionDetector versionDetector = new VersionDetector();

    ResolvedTarget target = resolver.resolve(metadataDir);

    assertEquals(ResolvedTargetType.TABLE_METADATA_DIR, target.type());
    assertEquals(tablePath, target.tableRoot());
    assertEquals(metadataDir, target.metadataRoot());
    assertNotNull(target.currentMetadataFile());
    assertEquals(2, versionDetector.detect(target.currentMetadataFile()));
  }

  @Test
  void resolvesExplicitMetadataFile() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir);
    Path metadataFile = latestMetadataFile(tablePath.resolve("metadata"));

    TargetResolver resolver = new TargetResolver();

    ResolvedTarget target = resolver.resolve(metadataFile);

    assertEquals(ResolvedTargetType.METADATA_FILE, target.type());
    assertEquals(tablePath, target.tableRoot());
    assertEquals(metadataFile.getParent(), target.metadataRoot());
    assertEquals(metadataFile, target.currentMetadataFile());
  }

  @Test
  void resolvesWarehouseRootAndScansTables() throws IOException {
    Path firstTable = IcebergTableFixtures.createPartitionedTable(tempDir);
    Path secondTable = IcebergTableFixtures.createPartitionedTable(tempDir);

    TargetResolver resolver = new TargetResolver();
    WarehouseScanner scanner = new WarehouseScanner(resolver);

    ResolvedTarget target = resolver.resolve(tempDir);
    List<ResolvedTarget> discoveredTables = scanner.scan(tempDir);

    assertEquals(ResolvedTargetType.WAREHOUSE, target.type());
    assertTrue(target.tableRoot() == null);
    assertEquals(tempDir, target.metadataRoot());
    assertEquals(2, discoveredTables.size());
    assertFalse(discoveredTables.stream().anyMatch(entry -> entry.currentMetadataFile() == null));
    List<Path> tableRoots = discoveredTables.stream()
        .map(ResolvedTarget::tableRoot)
        .sorted()
        .collect(Collectors.toList());
    assertEquals(List.of(firstTable, secondTable).stream().sorted().collect(Collectors.toList()), tableRoots);
  }

  @Test
  void resolvesMetadataDirectoryWithNumericMetadataFilesWithoutVersionHint() throws IOException {
    Path tablePath = tempDir.resolve("external_table");
    Path metadataDir = tablePath.resolve("metadata");
    Files.createDirectories(metadataDir);
    Path firstMetadata = metadataDir.resolve("00000-a.metadata.json");
    Path secondMetadata = metadataDir.resolve("00001-b.metadata.json");
    Files.writeString(firstMetadata, "{\"format-version\":2}", StandardCharsets.UTF_8);
    Files.writeString(secondMetadata, "{\"format-version\":2}", StandardCharsets.UTF_8);

    TargetResolver resolver = new TargetResolver();

    ResolvedTarget target = resolver.resolve(metadataDir);

    assertEquals(ResolvedTargetType.TABLE_METADATA_DIR, target.type());
    assertEquals(tablePath, target.tableRoot());
    assertEquals(metadataDir, target.metadataRoot());
    assertEquals(secondMetadata, target.currentMetadataFile());
  }

  private static Path latestMetadataFile(Path metadataDir) throws IOException {
    try (Stream<Path> stream = Files.list(metadataDir)) {
      return stream
          .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
          .max(Comparator.comparing(path -> path.getFileName().toString()))
          .orElseThrow(() -> new IllegalStateException("Expected metadata file under " + metadataDir));
    }
  }
}
