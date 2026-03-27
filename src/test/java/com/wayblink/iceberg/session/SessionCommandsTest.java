package com.wayblink.iceberg.session;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wayblink.iceberg.cli.RootCommand;
import com.wayblink.iceberg.testsupport.IcebergTableFixtures;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class SessionCommandsTest {

  @TempDir
  Path tempDir;

  @Test
  void openPersistsCurrentTargetAndCurrentShowsIt() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse"));
    Path sessionFile = tempDir.resolve("session.json");
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(sessionFile, stdout);

    int openExit = cli.execute("open", tablePath.resolve("metadata").toString());
    int currentExit = cli.execute("current");

    assertEquals(0, openExit);
    assertEquals(0, currentExit);
    assertTrue(Files.exists(sessionFile));

    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("format-version: 2"));
    assertTrue(output.contains(tablePath.resolve("metadata").toString()));
  }

  @Test
  void closeRemovesPersistedSession() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse"));
    Path sessionFile = tempDir.resolve("session.json");
    CommandLine cli = cli(sessionFile, new ByteArrayOutputStream());

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    assertTrue(Files.exists(sessionFile));

    assertEquals(0, cli.execute("close"));
    assertFalse(Files.exists(sessionFile));
  }

  @Test
  void useTableSelectsATableFromWarehouseSession() throws IOException {
    Path firstTable = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse"));
    Path secondTable = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse"));
    Path sessionFile = tempDir.resolve("session.json");
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(sessionFile, stdout);

    assertEquals(0, cli.execute("open", tempDir.resolve("warehouse").toString()));
    assertEquals(0, cli.execute("use", "table", secondTable.getFileName().toString()));
    assertEquals(0, cli.execute("current"));

    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("target-type: TABLE_METADATA_DIR"));
    assertTrue(output.contains(secondTable.toString()));
    assertFalse(output.contains(firstTable.resolve("metadata").toString()));
  }

  private static CommandLine cli(Path sessionFile, ByteArrayOutputStream stdout) {
    RootCommand rootCommand = RootCommand.forSessionFile(sessionFile);
    CommandLine commandLine = new CommandLine(rootCommand);
    commandLine.setOut(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    return commandLine;
  }
}
