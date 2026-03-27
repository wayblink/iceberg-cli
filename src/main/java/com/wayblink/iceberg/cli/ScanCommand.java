package com.wayblink.iceberg.cli;

import picocli.CommandLine.Command;

@Command(
    name = "scan",
    mixinStandardHelpOptions = true,
    description = {
        "Discover Iceberg tables from a table path or warehouse root.",
        "Use `scan table` to inspect one resolved target, or `scan warehouse` to enumerate all tables under a warehouse."
    },
    optionListHeading = "%nOptions:%n",
    commandListHeading = "%nCommands:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect scan table --path /warehouse/db/orders/metadata",
        "  iceberg-inspect scan warehouse --path /warehouse --format json"
    },
    subcommands = {
        ScanTableCommand.class,
        ScanWarehouseCommand.class
    })
public final class ScanCommand implements Runnable {

  @picocli.CommandLine.ParentCommand
  private RootCommand rootCommand;

  RootCommand rootCommand() {
    return rootCommand;
  }

  @Override
  public void run() {
    // A nested subcommand is required.
  }
}
