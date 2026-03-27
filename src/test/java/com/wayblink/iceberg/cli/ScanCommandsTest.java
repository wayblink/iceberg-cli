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

class ScanCommandsTest {

  @TempDir
  Path tempDir;

  @Test
  void scanWarehouseListsDiscoveredTables() throws IOException {
    Path warehouseRoot = tempDir.resolve("warehouse");
    IcebergTableFixtures.createPartitionedTable(warehouseRoot.resolve("db_a"));
    IcebergTableFixtures.createPartitionedTable(warehouseRoot.resolve("db_b"));

    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("session.json"), stdout);

    int exitCode = cli.execute("scan", "warehouse", "--path", warehouseRoot.toString(), "--format", "json");

    assertEquals(0, exitCode);
    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"target\":{\"path\":"));
    assertTrue(output.contains("\"request\":{\"scope\":\"current\",\"groupBy\":\"table\",\"mode\":\"summary\"}"));
    assertTrue(output.contains("\"targetType\":\"TABLE_METADATA_DIR\""));
    assertTrue(output.contains("db_a"));
    assertTrue(output.contains("db_b"));
  }

  @Test
  void scanWarehouseSupportsJsonAlias() throws IOException {
    Path warehouseRoot = tempDir.resolve("warehouse-json-alias");
    IcebergTableFixtures.createPartitionedTable(warehouseRoot.resolve("db_a"));

    ByteArrayOutputStream stdout = new ByteArrayOutputStream();
    CommandLine cli = cli(tempDir.resolve("json-session.json"), stdout);

    int exitCode = cli.execute("scan", "warehouse", "--path", warehouseRoot.toString(), "--json");

    assertEquals(0, exitCode);
    String output = stdout.toString(StandardCharsets.UTF_8);
    assertTrue(output.contains("\"resultType\":\"scan-warehouse\""));
    assertTrue(output.contains("\"summary\":{\"rowCount\":1}"));
    assertTrue(output.contains("\"rows\":["));
  }

  private static CommandLine cli(Path sessionFile, ByteArrayOutputStream stdout) {
    RootCommand rootCommand = RootCommand.forSessionFile(sessionFile);
    CommandLine commandLine = new CommandLine(rootCommand);
    commandLine.setOut(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    return commandLine;
  }
}
