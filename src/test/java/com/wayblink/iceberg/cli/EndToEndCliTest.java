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

class EndToEndCliTest {

  @TempDir
  Path tempDir;

  @Test
  void openShowAndStatWorkForCurrentTableSession() throws IOException {
    Path tablePath = IcebergTableFixtures.createPartitionedTable(tempDir.resolve("warehouse"));
    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("session.json"), stdout);

    assertEquals(0, cli.execute("open", tablePath.resolve("metadata").toString()));
    assertEquals(0, cli.execute("show", "table"));
    assertEquals(0, cli.execute("stat", "table", "--format", "json"));

    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("format-version: 2"));
    assertTrue(output.contains("\"currentDataFileCount\":3"));
  }

  private static CommandLine cli(Path sessionFile, ByteArrayOutputStream stdout) {
    RootCommand rootCommand = RootCommand.forSessionFile(sessionFile);
    CommandLine commandLine = new CommandLine(rootCommand);
    commandLine.setOut(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    return commandLine;
  }
}
