package com.wayblink.iceberg.cli;

import picocli.CommandLine.Command;

@Command(
    name = "show",
    mixinStandardHelpOptions = true,
    description = {
        "Show structural Iceberg metadata information for the current table session.",
        "These commands are meant for inspection and debugging rather than aggregated statistics."
    },
    optionListHeading = "%nOptions:%n",
    commandListHeading = "%nCommands:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect show table",
        "  iceberg-inspect show snapshots",
        "  iceberg-inspect show manifests --snapshot-id 123456789"
    },
    subcommands = {
        ShowTableCommand.class,
        ShowSnapshotsCommand.class,
        ShowMetadataLogCommand.class,
        ShowManifestsCommand.class,
        ShowPartitionsCommand.class
    })
public final class ShowCommand implements Runnable {

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
