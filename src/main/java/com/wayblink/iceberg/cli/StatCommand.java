package com.wayblink.iceberg.cli;

import picocli.CommandLine.Command;

@Command(
    name = "stat",
    mixinStandardHelpOptions = true,
    description = {
        "Analyze Iceberg table or warehouse statistics.",
      "Use `stat table` for the current table state, `stat snapshot` for a specific snapshot, and `stat warehouse` to aggregate across discovered tables."
    },
    optionListHeading = "%nOptions:%n",
    commandListHeading = "%nCommands:%n",
    footerHeading = "%nExamples:%n",
    footer = {
      "  iceberg-inspect stat table",
      "  iceberg-inspect stat snapshot --snapshot-id 123456789 --group-by partition --format json",
        "  iceberg-inspect stat warehouse --path /warehouse"
    },
    subcommands = {
        StatTableCommand.class,
      StatSnapshotCommand.class,
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
