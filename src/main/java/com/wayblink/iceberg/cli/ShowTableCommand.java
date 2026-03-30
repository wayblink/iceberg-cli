package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.render.JsonPayloadBuilder;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "table",
    mixinStandardHelpOptions = true,
    description = "Show a summary of the current table, including metadata location, format version, and basic counts.",
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect show table"
    })
public final class ShowTableCommand implements Callable<Integer> {

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
    Map<String, Object> context = new LinkedHashMap<>();
    context.put("tableRoot", tableContext.tableRoot());
    context.put("metadataRoot", tableContext.metadataRoot());
    context.put("currentMetadataFile", tableContext.currentMetadataFile());
    context.put("formatVersion", tableContext.metadata().formatVersion());
    context.put("location", tableContext.table().location());

    Map<String, Object> metrics = new LinkedHashMap<>();
    metrics.put("currentSnapshotId", tableContext.table().currentSnapshot().snapshotId());
    metrics.put("snapshotCount", tableContext.metadata().snapshots().size());
    metrics.put("schemaCount", tableContext.metadata().schemas().size());
    metrics.put("partitionSpecCount", tableContext.metadata().specs().size());
    metrics.put("refsCount", tableContext.metadata().refs().size());

    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(jsonPayload(tableContext, context, metrics))
        : tableRenderer.renderSummary("Table Summary", context, metrics);
    spec.commandLine().getOut().println(output);
    return 0;
  }

  private Map<String, Object> jsonPayload(
      TableContext tableContext, Map<String, Object> context, Map<String, Object> metrics) {
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.putAll(context);
    summary.putAll(metrics);
    return payloadBuilder.payload(
        "show-table",
        tableContext.tableRoot(),
        tableContext.metadata().formatVersion(),
        Map.of(),
        summary,
        List.of());
  }
}
