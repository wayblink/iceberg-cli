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

class StatCommandsTest {

  @TempDir
  Path tempDir;

  @Test
  void statTableReturnsPartitionStatsForHistoryScope() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse"));
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("session.json"), stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));

    int exitCode = cli.execute("stat", "table", "--scope", "history", "--group-by", "partition", "--format", "json");

    assertEquals(0, exitCode);
    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"request\":{\"scope\":\"history\",\"groupBy\":\"partition\",\"mode\":\"auto\"}"));
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
    assertTrue(output.contains("\"request\":{\"scope\":\"current\",\"groupBy\":\"table\",\"mode\":\"auto\"}"));
  }

  private static CommandLine cli(Path sessionFile, ByteArrayOutputStream stdout) {
    RootCommand rootCommand = RootCommand.forSessionFile(sessionFile);
    CommandLine commandLine = new CommandLine(rootCommand);
    commandLine.setOut(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    return commandLine;
  }
}
