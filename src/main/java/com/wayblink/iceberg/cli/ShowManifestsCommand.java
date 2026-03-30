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
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "manifests",
    mixinStandardHelpOptions = true,
    description = {
        "Show manifests for the current snapshot or a specific snapshot.",
        "This is useful when you need to inspect manifest paths, content type, and added or deleted file counts.",
        "Rows are sorted by manifest path by default; use --sort-by and --limit to focus the output."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect show manifests",
        "  iceberg-inspect show manifests --snapshot-id 123456789",
        "  iceberg-inspect show manifests --json --limit 10 --sort-by deleted-files"
    })
public final class ShowManifestsCommand implements Callable<Integer> {

  private final JsonPayloadBuilder payloadBuilder = new JsonPayloadBuilder();
  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private ShowCommand showCommand;

  @Option(names = "--snapshot-id", description = "Specific snapshot ID to inspect.")
  private Long snapshotId;

  @Option(names = "--limit", description = "Maximum number of rows to show. Defaults to all rows.")
  private Integer limit;

  @Option(
      names = "--sort-by",
      defaultValue = "manifest-path",
      description = "Sort rows by: manifest-path, content, spec-id, added-files, existing-files, or deleted-files. Default: ${DEFAULT-VALUE}.")
  private String sortBy;

  @Mixin
  private RenderOptions renderOptions;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    RootCommand rootCommand = showCommand.rootCommand();
    TableContext tableContext = rootCommand.requireCurrentTable();
    Snapshot snapshot = snapshotId == null
        ? tableContext.table().currentSnapshot()
        : tableContext.table().snapshot(snapshotId);
    if (snapshot == null) {
      throw new IllegalStateException("Snapshot not found: " + snapshotId);
    }

    List<Map<String, Object>> rows = new ArrayList<>();
    for (ManifestFile manifest : snapshot.allManifests(tableContext.fileIO())) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("manifestPath", manifest.path());
      row.put("content", manifest.content());
      row.put("specId", manifest.partitionSpecId());
      row.put("addedFilesCount", manifest.addedFilesCount());
      row.put("existingFilesCount", manifest.existingFilesCount());
      row.put("deletedFilesCount", manifest.deletedFilesCount());
      rows.add(row);
    }
    rows.sort(manifestComparator(sortBy));
    if (limit != null && limit < rows.size()) {
      rows = new ArrayList<>(rows.subList(0, limit));
    }

    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(jsonPayload(tableContext, snapshot, rows))
        : tableRenderer.renderGrid("Manifest List", rows);
    spec.commandLine().getOut().println(output);
    return 0;
  }

  private Comparator<Map<String, Object>> manifestComparator(String rawSortBy) {
    String normalized = rawSortBy == null ? "manifest-path" : rawSortBy.toLowerCase();
    switch (normalized) {
      case "manifest-path":
        return Comparator.comparing(
            (Map<String, Object> row) -> String.valueOf(row.get("manifestPath")));
      case "content":
        return Comparator.comparing(
                (Map<String, Object> row) -> String.valueOf(row.get("content")))
            .thenComparing(row -> String.valueOf(row.get("manifestPath")));
      case "spec-id":
        return Comparator.comparingInt(
                (Map<String, Object> row) -> ((Number) row.get("specId")).intValue())
            .thenComparing(row -> String.valueOf(row.get("manifestPath")));
      case "added-files":
        return compareNumericDescending("addedFilesCount");
      case "existing-files":
        return compareNumericDescending("existingFilesCount");
      case "deleted-files":
        return compareNumericDescending("deletedFilesCount");
      default:
        throw new IllegalArgumentException("Unsupported sort key for manifests: " + rawSortBy);
    }
  }

  private Comparator<Map<String, Object>> compareNumericDescending(String key) {
    return Comparator.<Map<String, Object>>comparingLong(row -> ((Number) row.get(key)).longValue())
        .reversed()
        .thenComparing(row -> String.valueOf(row.get("manifestPath")));
  }

  private Map<String, Object> jsonPayload(
      TableContext tableContext, Snapshot snapshot, List<Map<String, Object>> rows) {
    Map<String, Object> request = new LinkedHashMap<>();
    request.put("snapshotId", snapshot.snapshotId());
    request.put("sortBy", sortBy);
    request.put("limit", limit);
    return payloadBuilder.payload(
        "show-manifests",
        tableContext.tableRoot(),
        tableContext.metadata().formatVersion(),
        request,
        payloadBuilder.summary(rows.size()),
        rows);
  }
}
