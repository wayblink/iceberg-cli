package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.session.SessionResolver;
import com.wayblink.iceberg.session.SessionState;
import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StoragePaths;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Command(
    name = "table",
    mixinStandardHelpOptions = true,
    description = {
        "Select a table from the current warehouse session.",
        "The table may be referenced by leaf name or by warehouse-relative path such as db/orders."
    },
    parameterListHeading = "%nParameters:%n",
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect use table orders",
        "  iceberg-inspect use table db/orders"
    })
public final class UseTableCommand implements Callable<Integer> {

  @ParentCommand
  private UseCommand useCommand;

  @Parameters(index = "0", description = "Table name or relative path under the current warehouse root.")
  private String tableName;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    RootCommand rootCommand = useCommand.rootCommand();
    SessionState currentState = new SessionResolver(rootCommand.sessionStore()).requireCurrent();
    String warehouseRoot = currentState.warehouseRoot();
    if (warehouseRoot == null) {
      throw new IllegalStateException("Current target is not a warehouse. Run open <warehouse-path> first.");
    }

    StorageBackend backend = currentState.storageOptions().resolveBackend(warehouseRoot);
    ResolvedTarget selectedTable =
        rootCommand.warehouseScanner().scan(warehouseRoot, currentState.storageOptions()).stream()
            .filter(target -> matchesTable(target, warehouseRoot, backend))
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("Table not found under warehouse: " + tableName));

    SessionState nextState = rootCommand.newSessionState(selectedTable, warehouseRoot, currentState.storageOptions());
    rootCommand.sessionStore().save(nextState);
    rootCommand.printSessionState(nextState, spec.commandLine().getOut());
    return 0;
  }

  private boolean matchesTable(ResolvedTarget target, String warehouseRoot, StorageBackend backend) {
    String tableRoot = target.tableRoot();
    if (tableRoot == null) {
      return false;
    }

    String relativePath = StoragePaths.relativize(warehouseRoot, tableRoot, backend);
    String leafName = StoragePaths.fileName(tableRoot, backend);
    return tableName.equals(relativePath) || tableName.equals(leafName);
  }
}
