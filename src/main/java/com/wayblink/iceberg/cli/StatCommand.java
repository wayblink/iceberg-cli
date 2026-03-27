package com.wayblink.iceberg.cli;

import picocli.CommandLine.Command;

@Command(
    name = "stat",
    mixinStandardHelpOptions = true,
    description = {
        "Analyze Iceberg table or warehouse statistics.",
        "Use `stat table` for a single table view and `stat warehouse` to aggregate across all discovered tables."
    },
    optionListHeading = "%nOptions:%n",
    commandListHeading = "%nCommands:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect stat table --scope current",
        "  iceberg-inspect stat table --scope all --group-by snapshot --format json",
        "  iceberg-inspect stat warehouse --path /warehouse"
    },
    subcommands = {
        StatTableCommand.class,
        StatWarehouseCommand.class
    })
public final class StatCommand implements Runnable {

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
