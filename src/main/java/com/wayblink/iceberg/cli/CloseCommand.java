package com.wayblink.iceberg.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

@Command(
    name = "close",
    mixinStandardHelpOptions = true,
    description = "Clear the current session target.",
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect close"
    })
public final class CloseCommand implements Callable<Integer> {

  @ParentCommand
  private RootCommand rootCommand;

  @Override
  public Integer call() {
    rootCommand.sessionStore().clear();
    return 0;
  }
}
