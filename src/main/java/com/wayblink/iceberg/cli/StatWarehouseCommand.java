package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.analyzer.AnalysisGroupBy;
import com.wayblink.iceberg.analyzer.AnalysisPrecision;
import com.wayblink.iceberg.analyzer.AnalysisRequest;
import com.wayblink.iceberg.analyzer.AnalysisResult;
import com.wayblink.iceberg.analyzer.AnalysisScope;
import com.wayblink.iceberg.analyzer.TableAnalyzer;
import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.render.JsonRenderer;
import com.wayblink.iceberg.render.RenderFormat;
import com.wayblink.iceberg.render.TableRenderer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParentCommand;
import picocli.CommandLine.Spec;

@Command(
    name = "warehouse",
    mixinStandardHelpOptions = true,
    description = {
        "Analyze all discovered tables under a warehouse root.",
        "If --path is omitted, the current warehouse session is used."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect stat warehouse --path /warehouse",
        "  iceberg-inspect stat warehouse --scope current --group-by table --format json"
    })
public final class StatWarehouseCommand implements Callable<Integer> {

  private final TableAnalyzer tableAnalyzer = new TableAnalyzer();
  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private StatCommand statCommand;

  @Option(names = "--path", description = "Warehouse root path. Defaults to the current warehouse session.")
  private Path path;

  @Option(names = "--scope", defaultValue = "current", description = "Analysis scope: current, history, or all.")
  private String scope;

  @Option(names = "--group-by", defaultValue = "table", description = "Grouping: table, metadata-version, snapshot, or partition.")
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
    Path warehousePath = rootCommand.resolveWarehousePath(path);
    AnalysisRequest request = new AnalysisRequest(
        AnalysisScope.parse(scope),
        AnalysisGroupBy.parse(groupBy),
        AnalysisPrecision.parse(mode));

    List<Map<String, Object>> rows = new ArrayList<>();
    for (ResolvedTarget target : rootCommand.warehouseScanner().scan(warehousePath)) {
      TableContext tableContext = rootCommand.loadResolvedTable(target);
      AnalysisResult tableResult = tableAnalyzer.analyzeTable(tableContext, request);
      for (Map<String, Object> row : tableResult.getRows()) {
        Map<String, Object> withTable = new LinkedHashMap<>();
        withTable.put("tableRoot", tableContext.tableRoot().toString());
        withTable.putAll(row);
        rows.add(withTable);
      }
    }

    AnalysisResult result = new AnalysisResult("warehouse", warehousePath.toString(), null, request, rows);
    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(result)
        : tableRenderer.render(result);
    spec.commandLine().getOut().println(output);
    return 0;
  }
}
