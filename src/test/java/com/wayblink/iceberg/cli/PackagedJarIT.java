package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class PackagedJarIT {

  @Test
  void shadedJarShowsHelp() throws IOException, InterruptedException {
    Path buildDirectory = Path.of(System.getProperty("projectBuildDirectory"));
    Path jar = buildDirectory.resolve("iceberg-inspect-0.1.0-SNAPSHOT.jar");

    Process process = new ProcessBuilder("java", "-jar", jar.toString(), "--help").start();
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
