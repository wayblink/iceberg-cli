package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import picocli.CommandLine;

class ApplicationSmokeTest {

  @Test
  void rootCommandShowsUsage() {
    int exitCode = new CommandLine(new RootCommand()).execute("--help");
    assertEquals(0, exitCode);
  }
}
