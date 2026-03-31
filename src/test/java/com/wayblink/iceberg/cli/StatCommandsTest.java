package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wayblink.iceberg.testsupport.IcebergTableFixtures;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class StatCommandsTest {

  @TempDir
  Path tempDir;

  @Test
  void statSnapshotReturnsPartitionStatsForSpecificSnapshot() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse"));
    long snapshotId = currentSnapshotId(tablePath);
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("session.json"), stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));

    int exitCode =
        cli.execute(
            "stat",
            "snapshot",
            "--snapshot-id",
            Long.toString(snapshotId),
            "--group-by",
            "partition",
            "--format",
            "json");

    assertEquals(0, exitCode);
    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"resultType\":\"snapshot\""));
    assertTrue(output.contains("\"request\":{\"groupBy\":\"partition\",\"mode\":\"auto\",\"snapshotId\":" + snapshotId));
    assertTrue(output.contains("\"summary\":{\"rowCount\":"));
    assertTrue(output.contains("\"partition\":\"category=books\""));
    assertTrue(output.contains("\"snapshotId\""));
  }

  @Test
  void statTableUsesSummaryCardInTableFormat() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse-summary"));
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("summary-session.json"), stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));

    int exitCode = cli.execute("stat", "table");

    assertEquals(0, exitCode);
    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("Table Summary"));
    assertTrue(output.contains("Data Files"));
    assertTrue(output.contains("Data Size"));
    assertTrue(output.contains("Metadata Versions"));
    assertTrue(output.contains("Table Rows"));
    assertTrue(output.contains("Deleted Record Rows"));
    assertTrue(output.contains("Deletion Vector Count"));
  }

  @Test
  void statTableSupportsJsonAlias() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse-json-alias"));
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("json-alias-session.json"), stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    stdout.reset();

    int exitCode = cli.execute("stat", "table", "--json");

    assertEquals(0, exitCode);
    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"resultType\":\"table\""));
    assertTrue(output.contains("\"request\":{\"groupBy\":\"table\",\"mode\":\"auto\"}"));
  }

  @Test
  void statTableRejectsSnapshotGroupBy() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse-snapshot-group"));
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("snapshot-group-session.json"), stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    stdout.reset();

    int exitCode = cli.execute("stat", "table", "--group-by", "snapshot");

    assertEquals(2, exitCode);
    assertTrue(stdout.toString(StandardCharsets.UTF_8).contains("Use `stat snapshot --snapshot-id <id>`"));
  }

  private static CommandLine cli(Path sessionFile, ByteArrayOutputStream stdout) {
    RootCommand rootCommand = RootCommand.forSessionFile(sessionFile);
    CommandLine commandLine = new CommandLine(rootCommand);
    commandLine.setOut(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    return commandLine;
  }

  private static long currentSnapshotId(Path tablePath) throws IOException {
    Path metadataDir = tablePath.resolve("metadata");
    Path metadataFile;
    try (java.util.stream.Stream<Path> stream = Files.list(metadataDir)) {
      metadataFile =
          stream
              .filter(path -> path.getFileName().toString().endsWith(".metadata.json"))
              .max(Comparator.comparing(path -> path.getFileName().toString()))
              .orElseThrow(() -> new IllegalStateException("Expected metadata file under " + metadataDir));
    }
    TableMetadata metadata =
        TableMetadataParser.read(new HadoopFileIO(new Configuration()), metadataFile.toString());
    return metadata.currentSnapshot().snapshotId();
  }
}
