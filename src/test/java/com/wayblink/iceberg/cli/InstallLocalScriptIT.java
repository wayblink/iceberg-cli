package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class InstallLocalScriptIT {

  private static final String ROOT_HELP_DESCRIPTION =
      "Inspect Iceberg metadata targets from local paths, HDFS, or S3A.";

  @TempDir
  Path tempDir;

  @Test
  void installLocalCopiesJarIntoInstallLocation() throws IOException, InterruptedException {
    Path projectDirectory = Path.of(System.getProperty("user.dir"));
    Path buildDirectory = Path.of(System.getProperty("projectBuildDirectory"));
    Path installBinDirectory = tempDir.resolve("bin");
    Path installedCommand = installBinDirectory.resolve("iceberg-inspect");
    Path installedJar = tempDir.resolve("lib").resolve("iceberg-inspect").resolve("iceberg-inspect-0.1.0-SNAPSHOT.jar");

    ProcessBuilder installProcessBuilder = new ProcessBuilder("sh", "bin/install-local.sh", installBinDirectory.toString())
        .directory(projectDirectory.toFile());
    installProcessBuilder.environment().put("PROJECT_BUILD_DIRECTORY", buildDirectory.toString());
    Process installProcess = installProcessBuilder.start();
    String installOutput = readAll(installProcess.getInputStream()) + readAll(installProcess.getErrorStream());
    int installExitCode = installProcess.waitFor();

    assertEquals(0, installExitCode, installOutput);
    assertTrue(Files.isRegularFile(installedCommand));
    assertTrue(Files.isRegularFile(installedJar));

    String installedScript = Files.readString(installedCommand, StandardCharsets.UTF_8);
    assertFalse(installedScript.contains(projectDirectory.toString()));
    assertFalse(installedScript.contains(buildDirectory.toString()));

    Process helpProcess = new ProcessBuilder(installedCommand.toString(), "--help").start();
    String helpOutput = readAll(helpProcess.getInputStream()) + readAll(helpProcess.getErrorStream());
    int helpExitCode = helpProcess.waitFor();

    assertEquals(0, helpExitCode, helpOutput);
    assertTrue(helpOutput.contains(ROOT_HELP_DESCRIPTION));
  }

  private static String readAll(InputStream inputStream) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    inputStream.transferTo(outputStream);
    return outputStream.toString(StandardCharsets.UTF_8);
  }
}
