package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.discovery.ResolvedTargetType;
import com.wayblink.iceberg.session.SessionState;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(
    name = "open",
    mixinStandardHelpOptions = true,
    description = {
        "Open an Iceberg target and save it as the current session.",
        "The target may be a table metadata directory, a metadata JSON file, or a warehouse root."
    },
    parameterListHeading = "%nParameters:%n",
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect open metadata",
        "  iceberg-inspect open /warehouse/db/orders/metadata/v3.metadata.json",
        "  iceberg-inspect open /warehouse"
    })
public final class OpenCommand implements Callable<Integer> {

  @ParentCommand
  private RootCommand rootCommand;

  @Parameters(index = "0", description = "Path to a metadata directory, metadata file, or warehouse root.")
  private Path path;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    ResolvedTarget target = rootCommand.targetResolver().resolve(path);
    String warehouseRoot = target.type() == ResolvedTargetType.WAREHOUSE
        ? target.metadataRoot().toString()
        : null;
    SessionState state = rootCommand.newSessionState(target, warehouseRoot);
    rootCommand.sessionStore().save(state);
    rootCommand.printSessionState(state, spec.commandLine().getOut());
    return 0;
  }
}
