package com.wayblink.iceberg.cli;

import picocli.CommandLine.Command;

@Command(
    name = "list",
    mixinStandardHelpOptions = true,
    description = {
        "List Iceberg metadata entities for the current table session.",
        "These commands provide concise inventories rather than deep inspection details."
    },
    optionListHeading = "%nOptions:%n",
    commandListHeading = "%nCommands:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect list snapshots",
        "  iceberg-inspect list snapshots --path /warehouse/db/orders/metadata --json"
    },
    subcommands = {
        ListSnapshotsCommand.class
    })
public final class ListCommand implements Runnable {

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
