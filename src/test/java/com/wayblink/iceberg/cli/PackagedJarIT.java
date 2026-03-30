package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.jar.JarFile;
import org.junit.jupiter.api.Test;

class PackagedJarIT {

  private static final String MAIN_CLASS_ENTRY = "com/wayblink/iceberg/cli/IcebergInspectApplication.class";

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

  @Test
  void shadedJarTargetsJava11Bytecode() throws IOException {
    Path buildDirectory = Path.of(System.getProperty("projectBuildDirectory"));
    Path jar = buildDirectory.resolve("iceberg-inspect-0.1.0-SNAPSHOT.jar");

    try (JarFile jarFile = new JarFile(jar.toFile());
        InputStream classStream = jarFile.getInputStream(jarFile.getJarEntry(MAIN_CLASS_ENTRY));
        DataInputStream dataInputStream = new DataInputStream(classStream)) {
      assertEquals(0xCAFEBABE, dataInputStream.readInt());
      dataInputStream.readUnsignedShort();
      int majorVersion = dataInputStream.readUnsignedShort();

      assertEquals(55, majorVersion, "Expected Java 11 bytecode, got major version " + majorVersion);
    }
  }

  private static String readAll(InputStream inputStream) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    inputStream.transferTo(outputStream);
    return outputStream.toString(StandardCharsets.UTF_8);
  }
}
