package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class LauncherScriptIT {

  @Test
  void launcherScriptShowsHelp() throws IOException, InterruptedException {
    Path projectDirectory = Path.of(System.getProperty("user.dir"));
    Path script = projectDirectory.resolve("bin/iceberg-inspect");

    Process process = new ProcessBuilder("sh", script.toString(), "--help")
        .directory(projectDirectory.toFile())
        .start();
    String output = readAll(process.getInputStream()) + readAll(process.getErrorStream());
    int exitCode = process.waitFor();

    assertEquals(0, exitCode);
    assertTrue(output.contains("Inspect local Iceberg metadata targets from a local metadata directory"));
    assertTrue(output.contains("Commands:"));
    assertTrue(output.contains("Examples:"));
  }

  private static String readAll(InputStream inputStream) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    inputStream.transferTo(outputStream);
    return outputStream.toString(StandardCharsets.UTF_8);
  }
}
