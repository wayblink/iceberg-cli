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

class ListCommandsTest {

  @TempDir
  Path tempDir;

  @Test
  void listSnapshotsSupportsDirectPathAndJsonOutput() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse-list"));
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("list-session.json"), stdout);

    int exitCode = cli.execute("list", "snapshots", "--path", tablePath.resolve("metadata").toString(), "--json");

    assertEquals(0, exitCode);
    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"resultType\":\"list-snapshots\""));
    assertTrue(output.contains("\"summary\":{\"rowCount\":"));
    assertTrue(output.contains("\"snapshotId\""));
    assertTrue(output.contains("\"sequenceNumber\""));
    assertTrue(output.contains("\"manifestCount\""));
  }

  private static CommandLine cli(Path sessionFile, ByteArrayOutputStream stdout) {
    RootCommand rootCommand = RootCommand.forSessionFile(sessionFile);
    CommandLine commandLine = new CommandLine(rootCommand);
    commandLine.setOut(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    return commandLine;
  }
}