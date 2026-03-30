package com.wayblink.iceberg.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wayblink.iceberg.loader.VersionDetector;
import com.wayblink.iceberg.storage.StorageOptions;
import com.wayblink.iceberg.storage.StorageExplorer;
import com.wayblink.iceberg.testsupport.IcebergTableFixtures;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    assertEquals(tablePath.toString(), target.tableRoot());
    assertEquals(metadataDir.toString(), target.metadataRoot());
    assertNotNull(target.currentMetadataFile());
    assertEquals(2, versionDetector.detect(target.currentMetadataFile(), StorageOptions.defaults()));
  }

  @Test
  void resolvesExplicitMetadataFile() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir);
    Path metadataFile = latestMetadataFile(tablePath.resolve("metadata"));

    TargetResolver resolver = new TargetResolver();

    ResolvedTarget target = resolver.resolve(metadataFile);

    assertEquals(ResolvedTargetType.METADATA_FILE, target.type());
    assertEquals(tablePath.toString(), target.tableRoot());
    assertEquals(metadataFile.getParent().toString(), target.metadataRoot());
    assertEquals(metadataFile.toString(), target.currentMetadataFile());
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
    assertEquals(tempDir.toString(), target.metadataRoot());
    assertEquals(2, discoveredTables.size());
    assertFalse(discoveredTables.stream().anyMatch(entry -> entry.currentMetadataFile() == null));
    List<String> tableRoots = discoveredTables.stream()
        .map(ResolvedTarget::tableRoot)
        .sorted()
        .collect(Collectors.toList());
    assertEquals(
        List.of(firstTable.toString(), secondTable.toString()).stream().sorted().collect(Collectors.toList()),
        tableRoots);
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
    assertEquals(tablePath.toString(), target.tableRoot());
    assertEquals(metadataDir.toString(), target.metadataRoot());
    assertEquals(secondMetadata.toString(), target.currentMetadataFile());
  }

  @Test
  void resolvesS3aMetadataDirectoryUsingStorageExplorer() {
    FakeStorageExplorer explorer = new FakeStorageExplorer();
    explorer.addDirectory("s3a://bucket/warehouse/db/orders/metadata");
    explorer.addFile("s3a://bucket/warehouse/db/orders/metadata/00000-a.metadata.json");
    explorer.addFile("s3a://bucket/warehouse/db/orders/metadata/00001-b.metadata.json");

    TargetResolver resolver = new TargetResolver(explorer);

    ResolvedTarget target = resolver.resolve("s3a://bucket/warehouse/db/orders/metadata");

    assertEquals(ResolvedTargetType.TABLE_METADATA_DIR, target.type());
    assertEquals("s3a://bucket/warehouse/db/orders", target.tableRoot());
    assertEquals("s3a://bucket/warehouse/db/orders/metadata", target.metadataRoot());
    assertEquals(
        "s3a://bucket/warehouse/db/orders/metadata/00001-b.metadata.json",
        target.currentMetadataFile());
  }

  @Test
  void resolvesHdfsWarehouseRootUsingStorageExplorer() {
    FakeStorageExplorer explorer = new FakeStorageExplorer();
    explorer.addDirectory("hdfs://nn:8020/warehouse");
    explorer.addDirectory("hdfs://nn:8020/warehouse/db");
    explorer.addDirectory("hdfs://nn:8020/warehouse/db/orders");
    explorer.addDirectory("hdfs://nn:8020/warehouse/db/orders/metadata");
    explorer.addFile("hdfs://nn:8020/warehouse/db/orders/metadata/v1.metadata.json");

    TargetResolver resolver = new TargetResolver(explorer);

    ResolvedTarget target = resolver.resolve("hdfs://nn:8020/warehouse");

    assertEquals(ResolvedTargetType.WAREHOUSE, target.type());
    assertEquals("hdfs://nn:8020/warehouse", target.metadataRoot());
    assertEquals(null, target.currentMetadataFile());
  }

  private static Path latestMetadataFile(Path metadataDir) throws IOException {
    try (Stream<Path> stream = Files.list(metadataDir)) {
      return stream
          .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
          .max(Comparator.comparing(path -> path.getFileName().toString()))
          .orElseThrow(() -> new IllegalStateException("Expected metadata file under " + metadataDir));
    }
  }

  private static final class FakeStorageExplorer implements StorageExplorer {

    private final Map<String, Entry> entries = new HashMap<>();

    void addDirectory(String path) {
      entries.put(canonicalize(path), new Entry(true));
    }

    void addFile(String path) {
      String normalized = canonicalize(path);
      entries.put(normalized, new Entry(false));
      String parent = parent(normalized);
      if (parent != null) {
        addDirectory(parent);
      }
    }

    @Override
    public String normalize(String path) {
      return canonicalize(path);
    }

    @Override
    public boolean exists(String path) {
      return entries.containsKey(canonicalize(path));
    }

    @Override
    public boolean isFile(String path) {
      Entry entry = entries.get(canonicalize(path));
      return entry != null && !entry.directory;
    }

    @Override
    public boolean isDirectory(String path) {
      Entry entry = entries.get(canonicalize(path));
      return entry != null && entry.directory;
    }

    @Override
    public List<String> list(String directory) {
      String normalizedDirectory = canonicalize(directory);
      List<String> children = new ArrayList<>();
      for (String candidate : entries.keySet()) {
        if (normalizedDirectory.equals(parent(candidate))) {
          children.add(candidate);
        }
      }
      return children.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public List<String> walk(String root, int maxDepth) {
      String normalizedRoot = canonicalize(root);
      List<String> descendants = new ArrayList<>();
      for (String candidate : entries.keySet()) {
        if (candidate.equals(normalizedRoot)) {
          descendants.add(candidate);
          continue;
        }
        if (candidate.startsWith(normalizedRoot + "/") && depth(normalizedRoot, candidate) <= maxDepth) {
          descendants.add(candidate);
        }
      }
      return descendants.stream().sorted().collect(Collectors.toList());
    }

    @Override
    public String readText(String path) {
      throw new UnsupportedOperationException("readText not needed in fake explorer");
    }

    private static String canonicalize(String path) {
      if (path.endsWith("/") && path.length() > "s3a://bucket".length()) {
        return path.substring(0, path.length() - 1);
      }
      return path;
    }

    private static String parent(String path) {
      int slash = path.lastIndexOf('/');
      int schemeBoundary = path.indexOf("://");
      if (slash < 0) {
        return null;
      }
      if (schemeBoundary >= 0 && slash <= schemeBoundary + 2) {
        return null;
      }
      return path.substring(0, slash);
    }

    private static int depth(String root, String child) {
      String suffix = child.substring(root.length());
      if (suffix.startsWith("/")) {
        suffix = suffix.substring(1);
      }
      if (suffix.isEmpty()) {
        return 0;
      }
      return suffix.split("/").length;
    }

    private static final class Entry {

      private final boolean directory;

      private Entry(boolean directory) {
        this.directory = directory;
      }
    }
  }
}
