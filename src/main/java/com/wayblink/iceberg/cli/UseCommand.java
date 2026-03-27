package com.wayblink.iceberg.cli;

import picocli.CommandLine.Command;

@Command(
    name = "use",
    mixinStandardHelpOptions = true,
    description = {
        "Switch sub-context within the current session.",
        "This is mainly used after opening a warehouse, then selecting one table under that warehouse."
    },
    optionListHeading = "%nOptions:%n",
    commandListHeading = "%nCommands:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect open /warehouse",
        "  iceberg-inspect use table db/orders"
    },
    subcommands = {UseTableCommand.class})
public final class UseCommand implements Runnable {

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
