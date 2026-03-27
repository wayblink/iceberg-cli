package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.session.SessionState;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "current",
    mixinStandardHelpOptions = true,
    description = "Show the current session target, including the resolved metadata file and detected format version.",
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect current"
    })
public final class CurrentCommand implements Callable<Integer> {

  @ParentCommand
  private RootCommand rootCommand;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    SessionState state = rootCommand.sessionStore().load()
        .orElseThrow(() -> new IllegalStateException("No current target. Run open <path> first."));
    rootCommand.printSessionState(state, spec.commandLine().getOut());
    return 0;
  }
}
