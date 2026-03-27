package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.render.JsonPayloadBuilder;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.iceberg.Snapshot;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "snapshots",
    mixinStandardHelpOptions = true,
    description = "Show snapshot history for the current table.",
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect show snapshots"
    })
public final class ShowSnapshotsCommand implements Callable<Integer> {

  private final JsonPayloadBuilder payloadBuilder = new JsonPayloadBuilder();
  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private ShowCommand showCommand;

  @Mixin
  private RenderOptions renderOptions;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    RootCommand rootCommand = showCommand.rootCommand();
    TableContext tableContext = rootCommand.requireCurrentTable();
    List<Snapshot> snapshots = new ArrayList<>();
    tableContext.table().snapshots().forEach(snapshots::add);
    snapshots.sort(Comparator.comparingLong(Snapshot::timestampMillis));
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
        ? jsonRenderer.render(jsonPayload(tableContext, rows))
        : tableRenderer.renderGrid("Snapshot History", rows);
    spec.commandLine().getOut().println(output);
    return 0;
  }

  private Map<String, Object> jsonPayload(TableContext tableContext, List<Map<String, Object>> rows) {
    return payloadBuilder.payload(
        "show-snapshots",
        tableContext.tableRoot().toString(),
        tableContext.metadata().formatVersion(),
        Map.of(),
        payloadBuilder.summary(rows.size()),
        rows);
  }
}
