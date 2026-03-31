package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

class HelpCommandTest {

  @Test
  void rootHelpShowsDescriptionAndExamples() {
    HelpResult result = executeHelp("--help");

    assertEquals(0, result.exitCode());
    assertTrue(result.output().contains("Description:"));
    assertTrue(result.output().contains("Use `open` once to establish a current session"));
    assertTrue(result.output().contains("Examples:"));
    assertTrue(result.output().contains("iceberg-inspect list snapshots"));
    assertTrue(result.output().contains("iceberg-inspect stat snapshot --snapshot-id 123456789"));
  }

  @Test
  void versionFlagPrintsPomVersion() {
    HelpResult result = executeHelp("-V");

    assertEquals(0, result.exitCode());
    assertTrue(result.output().contains("iceberg-inspect 0.1.0-SNAPSHOT"));
  }

  @Test
  void statTableHelpHasOwnHelpOptionAndExamples() {
    HelpResult result = executeHelp("stat", "table", "--help");

    assertEquals(0, result.exitCode());
    assertTrue(result.output().contains("Analyze the current table or a specific table path."));
    assertTrue(result.output().contains("--format"));
    assertTrue(result.output().contains("--json"));
    assertTrue(result.output().contains("iceberg-inspect stat table --group-by partition --format json"));
  }

  @Test
  void statSnapshotHelpHasSnapshotSelector() {
    HelpResult result = executeHelp("stat", "snapshot", "--help");

    assertEquals(0, result.exitCode());
    assertTrue(result.output().contains("Analyze a specific snapshot for the current table or a specific table path."));
    assertTrue(result.output().contains("--snapshot-id"));
    assertTrue(result.output().contains("--group-by"));
    assertTrue(result.output().contains("iceberg-inspect stat snapshot --snapshot-id 123456789"));
  }

  @Test
  void openHelpExplainsSupportedTargets() {
    HelpResult result = executeHelp("open", "--help");

    assertEquals(0, result.exitCode());
    assertTrue(result.output().contains("Open an Iceberg target and save it as the current session."));
    assertTrue(result.output().contains("metadata JSON file"));
    assertTrue(result.output().contains("Parameters:"));
    assertTrue(result.output().contains("iceberg-inspect open /warehouse"));
  }

  @Test
  void showManifestsHelpDescribesSnapshotOption() {
    HelpResult result = executeHelp("show", "manifests", "--help");

    assertEquals(0, result.exitCode());
    assertTrue(result.output().contains("Show manifests for the current snapshot or a specific snapshot."));
    assertTrue(result.output().contains("--snapshot-id"));
    assertTrue(result.output().contains("--json"));
    assertTrue(result.output().contains("--limit"));
    assertTrue(result.output().contains("--sort-by"));
    assertTrue(result.output().contains("iceberg-inspect show manifests --snapshot-id 123456789"));
  }

  private static HelpResult executeHelp(String... args) {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    CommandLine commandLine = new CommandLine(new RootCommand());
    commandLine.setOut(new PrintWriter(outputStream, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(outputStream, true, StandardCharsets.UTF_8));
    int exitCode = commandLine.execute(args);
    return new HelpResult(exitCode, outputStream.toString(StandardCharsets.UTF_8));
  }

  private static final class HelpResult {

    private final int exitCode;
    private final String output;

    private HelpResult(int exitCode, String output) {
      this.exitCode = exitCode;
      this.output = output;
    }

    int exitCode() {
      return exitCode;
    }

    String output() {
      return output;
    }
  }
}
