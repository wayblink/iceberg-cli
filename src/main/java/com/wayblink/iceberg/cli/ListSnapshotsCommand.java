package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.render.JsonPayloadBuilder;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import com.wayblink.iceberg.storage.StorageOptions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.iceberg.Snapshot;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;

@Command(
    name = "snapshots",
    mixinStandardHelpOptions = true,
    description = {
        "List snapshots for the current table.",
        "If --path is omitted, the current table session is used.",
        "Rows are sorted by timestamp descending by default."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect list snapshots",
        "  iceberg-inspect list snapshots --path /warehouse/db/orders/metadata --json"
    })
public final class ListSnapshotsCommand implements Callable<Integer> {

  private final JsonPayloadBuilder payloadBuilder = new JsonPayloadBuilder();
  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private ListCommand listCommand;

  @Option(
      names = "--path",
      description = "Path to a table metadata directory, metadata file, or table root.")
  private String path;

  @Mixin
  private RenderOptions renderOptions;

  @Mixin
  private StorageOptionsMixin storageOptionsMixin;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    RootCommand rootCommand = listCommand.rootCommand();
    StorageOptions storageOptions =
        rootCommand.effectiveStorageOptions(path, storageOptionsMixin.toOptions(), storageOptionsMixin.hasOverrides());
    TableContext tableContext = path == null
        ? rootCommand.requireCurrentTable(storageOptionsMixin.toOptions(), storageOptionsMixin.hasOverrides())
        : rootCommand.loadTable(path, storageOptions);

    List<Snapshot> snapshots = new ArrayList<>();
    tableContext.table().snapshots().forEach(snapshots::add);
    snapshots.sort(Comparator.comparingLong(Snapshot::timestampMillis).reversed());

    List<Map<String, Object>> rows = new ArrayList<>(snapshots.size());
    for (Snapshot snapshot : snapshots) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("snapshotId", snapshot.snapshotId());
      row.put("sequenceNumber", snapshot.sequenceNumber());
      row.put("timestampMs", snapshot.timestampMillis());
      row.put("operation", snapshot.operation());
      row.put("manifestCount", snapshot.allManifests(tableContext.fileIO()).size());
      rows.add(row);
    }

    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(payloadBuilder.payload(
            "list-snapshots",
            tableContext.tableRoot(),
            tableContext.metadata().formatVersion(),
            Map.of(),
            payloadBuilder.summary(rows.size()),
            rows))
        : tableRenderer.renderGrid("Snapshots", rows);
    spec.commandLine().getOut().println(output);
    return 0;
  }
}
