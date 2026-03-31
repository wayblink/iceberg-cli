package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.render.JsonPayloadBuilder;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import com.wayblink.iceberg.storage.StorageOptions;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.iceberg.TableMetadata;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "metadata-log",
    mixinStandardHelpOptions = true,
    description = {
        "Show the current metadata file and previous metadata log entries for the current table.",
        "If --path is omitted, the current table session is used."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect show metadata-log",
        "  iceberg-inspect show metadata-log --path /warehouse/db/orders/metadata --json"
    })
public final class ShowMetadataLogCommand implements Callable<Integer> {

  private final JsonPayloadBuilder payloadBuilder = new JsonPayloadBuilder();
  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private ShowCommand showCommand;

  @picocli.CommandLine.Option(
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
    RootCommand rootCommand = showCommand.rootCommand();
    StorageOptions storageOptions =
        rootCommand.effectiveStorageOptions(path, storageOptionsMixin.toOptions(), storageOptionsMixin.hasOverrides());
    TableContext tableContext = path == null
        ? rootCommand.requireCurrentTable(storageOptionsMixin.toOptions(), storageOptionsMixin.hasOverrides())
        : rootCommand.loadTable(path, storageOptions);
    Map<String, Object> context = new LinkedHashMap<>();
    context.put("currentMetadataFile", tableContext.currentMetadataFile());
    context.put("lastUpdatedMs", tableContext.metadata().lastUpdatedMillis());

    List<Map<String, Object>> rows = new ArrayList<>();
    for (TableMetadata.MetadataLogEntry entry : tableContext.metadata().previousFiles()) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("metadataFile", entry.file());
      row.put("timestampMs", entry.timestampMillis());
      rows.add(row);
    }
    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(jsonPayload(tableContext, context, rows))
        : renderTableOutput(context, rows);
    spec.commandLine().getOut().println(output);
    return 0;
  }

  private String renderTableOutput(Map<String, Object> context, List<Map<String, Object>> rows) {
    StringBuilder output = new StringBuilder();
    output.append(tableRenderer.renderSummary("Metadata Log", context, Map.of()));
    if (!rows.isEmpty()) {
      output.append(System.lineSeparator()).append(System.lineSeparator());
      output.append(tableRenderer.renderGrid("Previous Metadata Files", rows));
    }
    return output.toString();
  }

  private Map<String, Object> jsonPayload(
      TableContext tableContext, Map<String, Object> context, List<Map<String, Object>> rows) {
    return payloadBuilder.payload(
        "show-metadata-log",
        tableContext.tableRoot(),
        tableContext.metadata().formatVersion(),
        Map.of(),
        context,
        rows);
  }
}
