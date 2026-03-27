package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.analyzer.AnalysisGroupBy;
import com.wayblink.iceberg.analyzer.AnalysisPrecision;
import com.wayblink.iceberg.analyzer.AnalysisRequest;
import com.wayblink.iceberg.analyzer.AnalysisResult;
import com.wayblink.iceberg.analyzer.AnalysisScope;
import com.wayblink.iceberg.analyzer.TableAnalyzer;
import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import java.nio.file.Path;
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
        "Analyze the current table or a specific table path.",
        "If --path is omitted, the current table session is used."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect stat table",
        "  iceberg-inspect stat table --scope all --group-by snapshot",
        "  iceberg-inspect stat table --group-by partition --format json"
    })
public final class StatTableCommand implements Callable<Integer> {

  private final TableAnalyzer tableAnalyzer = new TableAnalyzer();
  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private StatCommand statCommand;

  @Option(names = "--path", description = "Path to a table metadata directory or metadata file.")
  private Path path;

  @Option(
      names = "--scope",
      defaultValue = "current",
      description = "Analysis scope: current, history, or all.")
  private String scope;

  @Option(
      names = "--group-by",
      defaultValue = "table",
      description = "Grouping: table, metadata-version, snapshot, or partition.")
  private String groupBy;

  @Mixin
  private RenderOptions renderOptions;

  @Option(names = "--mode", defaultValue = "auto", description = "Traversal mode: auto, summary, or detail.")
  private String mode;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    RootCommand rootCommand = statCommand.rootCommand();
    TableContext tableContext = path == null ? rootCommand.requireCurrentTable() : rootCommand.loadTable(path);
    AnalysisRequest request = new AnalysisRequest(
        AnalysisScope.parse(scope),
        AnalysisGroupBy.parse(groupBy),
        AnalysisPrecision.parse(mode));
    AnalysisResult result = tableAnalyzer.analyzeTable(tableContext, request);
    render(result, renderOptions.resolve());
    return 0;
  }

  private void render(AnalysisResult result, RenderFormat renderFormat) {
    String output = renderFormat == RenderFormat.JSON
        ? jsonRenderer.render(result)
        : tableRenderer.render(result);
    spec.commandLine().getOut().println(output);
  }
}
