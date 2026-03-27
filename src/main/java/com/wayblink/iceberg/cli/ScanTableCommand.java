package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.analyzer.AnalysisGroupBy;
import com.wayblink.iceberg.analyzer.AnalysisPrecision;
import com.wayblink.iceberg.analyzer.AnalysisRequest;
import com.wayblink.iceberg.analyzer.AnalysisResult;
import com.wayblink.iceberg.analyzer.AnalysisScope;
import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "table",
    mixinStandardHelpOptions = true,
    description = {
        "Resolve and describe a single table target.",
        "If --path is omitted, the current session target is used."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect scan table",
        "  iceberg-inspect scan table --path /warehouse/db/orders/metadata --format json"
    })
public final class ScanTableCommand implements Callable<Integer> {

  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private ScanCommand scanCommand;

  @Option(names = "--path", description = "Path to a metadata directory, metadata file, or table root.")
  private Path path;

  @Mixin
  private RenderOptions renderOptions;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    RootCommand rootCommand = scanCommand.rootCommand();
    ResolvedTarget target = path == null
        ? rootCommand.requireCurrentResolvedTarget()
        : rootCommand.targetResolver().resolve(path);
    Integer formatVersion = target.currentMetadataFile() == null
        ? null
        : rootCommand.versionDetector().detect(target.currentMetadataFile());

    Map<String, Object> row = new LinkedHashMap<>();
    row.put("targetType", target.type().name());
    row.put("inputPath", target.inputPath().toString());
    row.put("tableRoot", target.tableRoot() == null ? null : target.tableRoot().toString());
    row.put("metadataRoot", target.metadataRoot().toString());
    row.put("currentMetadataFile", target.currentMetadataFile() == null ? null : target.currentMetadataFile().toString());

    AnalysisResult result = new AnalysisResult(
        "scan-table",
        target.inputPath().toString(),
        formatVersion,
        new AnalysisRequest(AnalysisScope.CURRENT, AnalysisGroupBy.TABLE, AnalysisPrecision.SUMMARY),
        Collections.singletonList(row));

    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(result)
        : tableRenderer.render(result);
    spec.commandLine().getOut().println(output);
    return 0;
  }
}
