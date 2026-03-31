package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.analyzer.AnalysisGroupBy;
import com.wayblink.iceberg.analyzer.AnalysisPrecision;
import com.wayblink.iceberg.analyzer.AnalysisRequest;
import com.wayblink.iceberg.analyzer.AnalysisResult;
import com.wayblink.iceberg.analyzer.ManifestTraversalService;
import com.wayblink.iceberg.analyzer.PartitionAnalyzer;
import com.wayblink.iceberg.analyzer.SnapshotAnalyzer;
import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import com.wayblink.iceberg.storage.StorageOptions;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.iceberg.Snapshot;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "snapshot",
    mixinStandardHelpOptions = true,
    description = {
        "Analyze a specific snapshot for the current table or a specific table path.",
        "Choose either --snapshot-id or --latest.",
        "If --path is omitted, the current table session is used."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect stat snapshot --snapshot-id 123456789",
        "  iceberg-inspect stat snapshot --snapshot-id 123456789 --group-by partition --format json",
        "  iceberg-inspect stat snapshot --latest --group-by table"
    })
public final class StatSnapshotCommand implements Callable<Integer> {

  private final ManifestTraversalService traversalService = new ManifestTraversalService();
  private final SnapshotAnalyzer snapshotAnalyzer = new SnapshotAnalyzer(traversalService);
  private final PartitionAnalyzer partitionAnalyzer = new PartitionAnalyzer(traversalService);
  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private StatCommand statCommand;

  @Option(names = "--path", description = "Path to a table metadata directory or metadata file.")
  private String path;

  @Option(names = "--snapshot-id", description = "Snapshot ID to analyze.")
  private Long snapshotId;

  @Option(names = "--latest", description = "Analyze the current snapshot.")
  private boolean latest;

  @Option(
      names = "--group-by",
      defaultValue = "snapshot",
      description = "Grouping: snapshot, partition, or table.")
  private String groupBy;

  @Mixin
  private RenderOptions renderOptions;

  @Mixin
  private StorageOptionsMixin storageOptionsMixin;

  @Option(names = "--mode", defaultValue = "auto", description = "Traversal mode: auto, summary, or detail.")
  private String mode;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    validateSnapshotSelector();

    RootCommand rootCommand = statCommand.rootCommand();
    StorageOptions storageOptions =
        rootCommand.effectiveStorageOptions(path, storageOptionsMixin.toOptions(), storageOptionsMixin.hasOverrides());
    TableContext tableContext = path == null
        ? rootCommand.requireCurrentTable(storageOptionsMixin.toOptions(), storageOptionsMixin.hasOverrides())
        : rootCommand.loadTable(path, storageOptions);

    Snapshot snapshot = latest ? tableContext.table().currentSnapshot() : tableContext.table().snapshot(snapshotId);
    if (snapshot == null) {
      throw new picocli.CommandLine.ParameterException(
          spec.commandLine(),
          latest ? "The table does not have a current snapshot." : "Snapshot not found: " + snapshotId);
    }

    AnalysisGroupBy resolvedGroupBy = AnalysisGroupBy.parse(groupBy);
    if (resolvedGroupBy == AnalysisGroupBy.METADATA_VERSION) {
      throw new picocli.CommandLine.ParameterException(
          spec.commandLine(),
          "`stat snapshot` does not support --group-by metadata-version. Use snapshot, partition, or table.");
    }

    AnalysisRequest request = new AnalysisRequest(resolvedGroupBy, AnalysisPrecision.parse(mode), snapshot.snapshotId());
    List<Map<String, Object>> rows;
    switch (resolvedGroupBy) {
      case PARTITION:
        rows = partitionAnalyzer.analyze(tableContext, snapshot);
        break;
      case SNAPSHOT:
      case TABLE:
        rows = List.of(snapshotAnalyzer.analyzeSnapshot(tableContext, snapshot));
        break;
      default:
        throw new IllegalStateException("Unsupported group-by: " + resolvedGroupBy);
    }

    AnalysisResult result = new AnalysisResult(
        "snapshot",
        tableContext.tableRoot(),
        tableContext.metadata().formatVersion(),
        request,
        rows);
    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(result)
        : tableRenderer.render(result);
    spec.commandLine().getOut().println(output);
    return 0;
  }

  private void validateSnapshotSelector() {
    if (latest && snapshotId != null) {
      throw new picocli.CommandLine.ParameterException(
          spec.commandLine(),
          "Choose either --snapshot-id or --latest, not both.");
    }
    if (!latest && snapshotId == null) {
      throw new picocli.CommandLine.ParameterException(
          spec.commandLine(),
          "Missing snapshot selector. Pass --snapshot-id <id> or --latest.");
    }
  }
}