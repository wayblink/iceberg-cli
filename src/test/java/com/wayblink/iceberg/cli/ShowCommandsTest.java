package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wayblink.iceberg.testsupport.IcebergTableFixtures;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class ShowCommandsTest {

  @TempDir
  Path tempDir;

  @Test
  void showCommandsPrintStructureForOpenTable() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse"));
    Path sessionFile = tempDir.resolve("session.json");
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(sessionFile, stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    assertEquals(0, cli.execute("show", "table"));
    assertEquals(0, cli.execute("show", "snapshots"));
    assertEquals(0, cli.execute("show", "metadata-log"));
    assertEquals(0, cli.execute("show", "manifests"));
    assertEquals(0, cli.execute("show", "partitions"));

    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("Table Summary"));
    assertTrue(output.contains("Snapshot History"));
    assertTrue(output.contains("Metadata Log"));
    assertTrue(output.contains("Manifest List"));
    assertTrue(output.contains("Current Partitions"));
    assertTrue(output.contains("SNAPSHOT ID"));
    assertTrue(output.contains("MANIFEST COUNT"));
    assertTrue(output.contains("append"));
    assertTrue(output.contains(".metadata.json"));
    assertTrue(output.contains("DATA"));
    assertTrue(output.contains("category=books"));
    assertTrue(output.contains("category=games"));
  }

  @Test
  void showCommandsReadMirroredLocalMetadataArtifactsWhenOriginalPathsAreRemote() {
    Path sessionFile = tempDir.resolve("session.json");
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(sessionFile, stdout);
    Path sampleMetadataDir = Path.of(System.getProperty("user.dir")).resolve("metadata");

    assertEquals(0, cli.execute("open", sampleMetadataDir.toString()));
    assertEquals(0, cli.execute("show", "snapshots"));
    assertEquals(0, cli.execute("show", "manifests"));

    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("Snapshot History"));
    assertTrue(output.contains("Manifest List"));
    assertTrue(output.contains("103679476631229492"));
    assertTrue(output.contains("s3a://stepcrawl"));
    assertTrue(output.contains("m1.avro"));
  }

  @Test
  void showCommandsSupportJsonOutput() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse-json"));
    Path sessionFile = tempDir.resolve("session-json.json");
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(sessionFile, stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    stdout.reset();

    assertEquals(0, cli.execute("show", "snapshots", "--json"));

    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"resultType\":\"show-snapshots\""));
    assertTrue(output.contains("\"target\":{\"path\":"));
    assertTrue(output.contains("\"request\":{}"));
    assertTrue(output.contains("\"summary\":{\"rowCount\":"));
    assertTrue(output.contains("\"rows\":["));
    assertTrue(output.contains("\"snapshotId\""));
    assertTrue(output.contains("\"manifestCount\""));
  }

  @Test
  void showJsonOutputNormalizesPathsAndOmitsUnsetRequestFields() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse-json-normalized"));
    Path sessionFile = tempDir.resolve("session-json-normalized.json");
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(sessionFile, stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    stdout.reset();

    assertEquals(0, cli.execute("show", "table", "--json"));
    String tableJson = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(tableJson.contains("\"resultType\":\"show-table\""));
    assertTrue(tableJson.contains("\"tableRoot\":\""));
    assertTrue(tableJson.contains("\"metadataRoot\":\""));
    assertTrue(tableJson.contains("\"currentMetadataFile\":\""));
    assertTrue(!tableJson.contains("file:///"));

    stdout.reset();
    assertEquals(0, cli.execute("show", "manifests", "--json"));
    String manifestsJson = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(manifestsJson.contains("\"resultType\":\"show-manifests\""));
    assertTrue(manifestsJson.contains("\"request\":{\"snapshotId\":"));
    assertTrue(manifestsJson.contains("\"sortBy\":\"manifest-path\""));
    assertTrue(!manifestsJson.contains("\"limit\":null"));

    stdout.reset();
    assertEquals(0, cli.execute("show", "metadata-log", "--json"));
    String metadataLogJson = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(metadataLogJson.contains("\"resultType\":\"show-metadata-log\""));
    assertTrue(metadataLogJson.contains("\"currentMetadataFile\":\""));
    assertTrue(!metadataLogJson.contains("file:///"));
  }

  @Test
  void showManifestsSupportsJsonLimitAndSortOptions() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse-manifests"));
    Path sessionFile = tempDir.resolve("session-manifests.json");
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(sessionFile, stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    stdout.reset();

    assertEquals(0, cli.execute("show", "manifests", "--json", "--limit", "1", "--sort-by", "deleted-files"));

    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"resultType\":\"show-manifests\""));
    assertTrue(output.contains("\"request\":{\"snapshotId\":"));
    assertTrue(output.contains("\"sortBy\":\"deleted-files\""));
    assertTrue(output.contains("\"limit\":1"));
    assertTrue(output.contains("\"summary\":{\"rowCount\":1}"));
    assertTrue(output.contains("\"rows\":[{"));
  }

  @Test
  void showPartitionsSupportsJsonLimitAndSortOptions() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse-partitions"));
    Path sessionFile = tempDir.resolve("session-partitions.json");
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(sessionFile, stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    stdout.reset();

    assertEquals(0, cli.execute("show", "partitions", "--json", "--limit", "1", "--sort-by", "partition"));

    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"resultType\":\"show-partitions\""));
    assertTrue(output.contains("\"request\":{\"sortBy\":\"partition\",\"limit\":1}"));
    assertTrue(output.contains("\"summary\":{\"rowCount\":1}"));
    assertTrue(output.contains("\"partition\":\"category=books\""));
  }

  private static CommandLine cli(Path sessionFile, ByteArrayOutputStream stdout) {
    RootCommand rootCommand = RootCommand.forSessionFile(sessionFile);
    CommandLine commandLine = new CommandLine(rootCommand);
    commandLine.setOut(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    return commandLine;
  }
}
