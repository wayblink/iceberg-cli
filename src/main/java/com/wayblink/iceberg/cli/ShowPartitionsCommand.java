package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.render.JsonPayloadBuilder;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.io.CloseableIterable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "partitions",
    mixinStandardHelpOptions = true,
    description = {
        "Show current partition values for the table and the number of data files visible in each partition.",
        "Rows are sorted by data file count by default; use --sort-by and --limit to control the listing."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect show partitions",
        "  iceberg-inspect show partitions --json --limit 20 --sort-by partition"
    })
public final class ShowPartitionsCommand implements Callable<Integer> {

  private final JsonPayloadBuilder payloadBuilder = new JsonPayloadBuilder();
  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private ShowCommand showCommand;

  @Option(names = "--limit", description = "Maximum number of rows to show. Defaults to all rows.")
  private Integer limit;

  @Option(
      names = "--sort-by",
      defaultValue = "data-file-count",
      description = "Sort rows by: partition or data-file-count. Default: ${DEFAULT-VALUE}.")
  private String sortBy;

  @Mixin
  private RenderOptions renderOptions;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    RootCommand rootCommand = showCommand.rootCommand();
    TableContext tableContext = rootCommand.requireCurrentTable();
    Map<String, Integer> partitionCounts = new LinkedHashMap<>();

    try (CloseableIterable<FileScanTask> tasks = tableContext.table().newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        PartitionSpec partitionSpec = tableContext.table().specs().get(task.file().specId());
        String partitionPath = partitionSpec.partitionToPath(task.file().partition());
        partitionCounts.merge(partitionPath, 1, Integer::sum);
      }
    } catch (IOException exception) {
      throw new UncheckedIOException("Failed to read scan tasks", exception);
    }

    List<Map<String, Object>> rows = new ArrayList<>(partitionCounts.size());
    for (Map.Entry<String, Integer> entry : partitionCounts.entrySet()) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("partition", entry.getKey());
      row.put("dataFileCount", entry.getValue());
      rows.add(row);
    }
    rows.sort(partitionComparator(sortBy));
    if (limit != null && limit < rows.size()) {
      rows = new ArrayList<>(rows.subList(0, limit));
    }

    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(jsonPayload(tableContext, rows))
        : tableRenderer.renderGrid("Current Partitions", rows);
    spec.commandLine().getOut().println(output);
    return 0;
  }

  private Comparator<Map<String, Object>> partitionComparator(String rawSortBy) {
    String normalized = rawSortBy == null ? "data-file-count" : rawSortBy.toLowerCase();
    switch (normalized) {
      case "partition":
        return Comparator.comparing(row -> String.valueOf(row.get("partition")));
      case "data-file-count":
        return Comparator.<Map<String, Object>>comparingInt(row -> ((Number) row.get("dataFileCount")).intValue())
            .reversed()
            .thenComparing(row -> String.valueOf(row.get("partition")));
      default:
        throw new IllegalArgumentException("Unsupported sort key for partitions: " + rawSortBy);
    }
  }

  private Map<String, Object> jsonPayload(TableContext tableContext, List<Map<String, Object>> rows) {
    Map<String, Object> request = new LinkedHashMap<>();
    request.put("sortBy", sortBy);
    request.put("limit", limit);
    return payloadBuilder.payload(
        "show-partitions",
        tableContext.tableRoot(),
        tableContext.metadata().formatVersion(),
        request,
        payloadBuilder.summary(rows.size()),
        rows);
  }
}
