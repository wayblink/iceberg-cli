package com.wayblink.iceberg.cli;

import picocli.CommandLine;

public final class IcebergInspectApplication {

  private IcebergInspectApplication() {
  }

  public static void main(String[] args) {
    int exitCode = new CommandLine(new RootCommand()).execute(args);
    System.exit(exitCode);
  }
}
