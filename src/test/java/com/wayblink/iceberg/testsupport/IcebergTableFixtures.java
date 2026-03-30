package com.wayblink.iceberg.testsupport;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StoragePaths;

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

  public static MirroredMetadataFixture createMirroredRemoteMetadataFixture(Path warehouseRoot) throws IOException {
    Path tablePath = createPartitionedTable(warehouseRoot);
    Path metadataDir = tablePath.resolve("metadata");
    Path currentMetadataFile = latestMetadataFile(metadataDir);
    String remoteTableLocation = "s3a://sample-lake/warehouse/" + tablePath.getFileName();

    HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
    TableMetadata metadata = TableMetadataParser.read(fileIO, currentMetadataFile.toString());
    Snapshot currentSnapshot = metadata.currentSnapshot();
    if (currentSnapshot == null) {
      throw new IllegalStateException("Expected fixture table to have a current snapshot");
    }

    List<ManifestFile> currentManifests = currentSnapshot.allManifests(fileIO);
    List<ManifestFile> mirroredManifests = new ArrayList<>(currentManifests.size());
    String firstMirroredManifestName = null;
    for (int index = 0; index < currentManifests.size(); index++) {
      ManifestFile manifest = currentManifests.get(index);
      String mirroredManifestName = "m" + (index + 1) + ".avro";
      if (firstMirroredManifestName == null) {
        firstMirroredManifestName = mirroredManifestName;
      }
      Files.copy(
          Path.of(manifest.path()),
          metadataDir.resolve(mirroredManifestName),
          StandardCopyOption.REPLACE_EXISTING);
      mirroredManifests.add(copyManifestFile(manifest, remoteTableLocation + "/metadata/" + mirroredManifestName));
    }

    rewriteManifestList(metadata.formatVersion(), currentSnapshot, fileIO, mirroredManifests);
    rewriteMetadataPaths(currentMetadataFile, tablePath, remoteTableLocation);

    return new MirroredMetadataFixture(
        metadataDir,
        currentSnapshot.snapshotId(),
        remoteTableLocation,
        firstMirroredManifestName == null ? "" : firstMirroredManifestName);
  }

  public static Path createPartitionedTable(Path warehouseRoot) throws IOException {
    String tableLocation = createPartitionedTable(new Configuration(), warehouseRoot.toString(), StorageBackend.LOCAL);
    return Paths.get(tableLocation);
  }

  public static String createPartitionedTable(
      Configuration configuration,
      String warehouseRoot,
      StorageBackend backend)
      throws IOException {
    String tableLocation = StoragePaths.resolve(
        warehouseRoot,
        "table_" + TABLE_COUNTER.incrementAndGet(),
        backend);
    HadoopTables tables = new HadoopTables(configuration);
    Table table = tables.create(
        PARTITIONED_SCHEMA,
        PARTITIONED_SPEC,
        SortOrder.unsorted(),
        Map.of(TableProperties.FORMAT_VERSION, "2"),
        tableLocation);

    appendFile(configuration, backend, table, tableLocation, "category=books", "books-0001.parquet", 128L, 10L);
    appendFile(configuration, backend, table, tableLocation, "category=books", "books-0002.parquet", 256L, 12L);
    appendFile(configuration, backend, table, tableLocation, "category=games", "games-0001.parquet", 512L, 20L);
    return tableLocation;
  }

  private static void appendFile(
      Configuration configuration,
      StorageBackend backend,
      Table table,
      String tableLocation,
      String partitionPath,
      String fileName,
      long fileSizeInBytes,
      long recordCount)
      throws IOException {
    String dataRoot = StoragePaths.resolve(tableLocation, "data", backend);
    String partitionDir = StoragePaths.resolve(dataRoot, partitionPath, backend);
    String dataFileLocation = StoragePaths.resolve(partitionDir, fileName, backend);
    createEmptyFile(configuration, dataFileLocation);

    DataFile dataFile = DataFiles.builder(table.spec())
        .withPath(dataFileLocation)
        .withFormat(FileFormat.PARQUET)
        .withPartitionPath(partitionPath)
        .withFileSizeInBytes(fileSizeInBytes)
        .withRecordCount(recordCount)
        .build();
    table.newAppend().appendFile(dataFile).commit();
  }

  private static void rewriteMetadataPaths(Path metadataFile, Path tablePath, String remoteTableLocation) throws IOException {
    String metadataJson = Files.readString(metadataFile);
    String localTableLocation = tablePath.toAbsolutePath().normalize().toString();
    Files.writeString(metadataFile, metadataJson.replace(localTableLocation, remoteTableLocation));
    deleteChecksumFile(metadataFile);
  }

  private static void deleteChecksumFile(Path file) throws IOException {
    Path checksumFile = file.resolveSibling("." + file.getFileName() + ".crc");
    Files.deleteIfExists(checksumFile);
  }

  private static void rewriteManifestList(
      int formatVersion,
      Snapshot snapshot,
      HadoopFileIO fileIO,
      List<ManifestFile> mirroredManifests) throws IOException {
    try {
      Class<?> manifestListsClass = Class.forName("org.apache.iceberg.ManifestLists");
      Method writeMethod = manifestListsClass.getDeclaredMethod(
          "write",
          int.class,
          org.apache.iceberg.io.OutputFile.class,
          long.class,
          Long.class,
          long.class,
          Long.class);
      writeMethod.setAccessible(true);
      Object writer = writeMethod.invoke(
          null,
          formatVersion,
          fileIO.newOutputFile(snapshot.manifestListLocation()),
          snapshot.snapshotId(),
          snapshot.parentId(),
          snapshot.sequenceNumber(),
          snapshot.firstRowId());

      Method addAllMethod = writer.getClass().getMethod("addAll", Iterable.class);
      addAllMethod.setAccessible(true);
      addAllMethod.invoke(writer, mirroredManifests);

      Method closeMethod = writer.getClass().getMethod("close");
      closeMethod.setAccessible(true);
      closeMethod.invoke(writer);
    } catch (ReflectiveOperationException exception) {
      throw new IOException("Failed to rewrite manifest list for mirrored metadata fixture", exception);
    }
  }

  private static ManifestFile copyManifestFile(ManifestFile manifest, String remappedPath) throws IOException {
    try {
      Constructor<?> constructor = Class.forName("org.apache.iceberg.GenericManifestFile").getDeclaredConstructor(
          String.class,
          long.class,
          int.class,
          ManifestContent.class,
          long.class,
          long.class,
          Long.class,
          List.class,
          java.nio.ByteBuffer.class,
          Integer.class,
          Long.class,
          Integer.class,
          Long.class,
          Integer.class,
          Long.class,
          Long.class);
      constructor.setAccessible(true);
      return (ManifestFile) constructor.newInstance(
          remappedPath,
          manifest.length(),
          manifest.partitionSpecId(),
          manifest.content(),
          manifest.sequenceNumber(),
          manifest.minSequenceNumber(),
          manifest.snapshotId(),
          manifest.partitions(),
          manifest.keyMetadata(),
          manifest.addedFilesCount(),
          manifest.addedRowsCount(),
          manifest.existingFilesCount(),
          manifest.existingRowsCount(),
          manifest.deletedFilesCount(),
          manifest.deletedRowsCount(),
          manifest.firstRowId());
    } catch (ReflectiveOperationException exception) {
      throw new IOException("Failed to remap manifest path for mirrored metadata fixture", exception);
    }
  }

  private static Path latestMetadataFile(Path metadataDir) throws IOException {
    try (java.util.stream.Stream<Path> stream = Files.list(metadataDir)) {
      return stream
          .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
          .max(Comparator.comparing(path -> path.getFileName().toString()))
          .orElseThrow(() -> new IllegalStateException("Expected metadata file under " + metadataDir));
    }
  }

  public static final class MirroredMetadataFixture {

    private final Path metadataDir;
    private final long snapshotId;
    private final String remoteTableLocation;
    private final String firstManifestName;

    private MirroredMetadataFixture(
        Path metadataDir, long snapshotId, String remoteTableLocation, String firstManifestName) {
      this.metadataDir = metadataDir;
      this.snapshotId = snapshotId;
      this.remoteTableLocation = remoteTableLocation;
      this.firstManifestName = firstManifestName;
    }

    public Path metadataDir() {
      return metadataDir;
    }

    public long snapshotId() {
      return snapshotId;
    }

    public String remoteTableLocation() {
      return remoteTableLocation;
    }

    public String firstManifestName() {
      return firstManifestName;
    }

  }

  private static void createEmptyFile(Configuration configuration, String location) throws IOException {
    org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(location);
    FileSystem fileSystem = filePath.getFileSystem(configuration);
    org.apache.hadoop.fs.Path parent = filePath.getParent();
    if (parent != null) {
      fileSystem.mkdirs(parent);
    }
    if (fileSystem.exists(filePath)) {
      return;
    }
    try (FSDataOutputStream output = fileSystem.create(filePath, false)) {
      output.write(new byte[0]);
    }
  }
}
