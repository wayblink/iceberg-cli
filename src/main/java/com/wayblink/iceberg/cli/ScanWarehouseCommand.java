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
import com.wayblink.iceberg.storage.StorageOptions;
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
        "Discover tables under a warehouse root.",
        "If --path is omitted, the current warehouse session is used."
    },
    optionListHeading = "%nOptions:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect scan warehouse --path /warehouse",
        "  iceberg-inspect scan warehouse --format json"
    })
public final class ScanWarehouseCommand implements Callable<Integer> {

  private final JsonRenderer jsonRenderer = new JsonRenderer();
  private final TableRenderer tableRenderer = new TableRenderer();

  @ParentCommand
  private ScanCommand scanCommand;

  @Option(names = "--path", description = "Warehouse root path. Defaults to the current warehouse session.")
  private String path;

  @Mixin
  private RenderOptions renderOptions;

  @Mixin
  private StorageOptionsMixin storageOptionsMixin;

  @Spec
  private CommandSpec spec;

  @Override
  public Integer call() {
    RootCommand rootCommand = scanCommand.rootCommand();
    StorageOptions storageOptions =
        rootCommand.effectiveStorageOptions(path, storageOptionsMixin.toOptions(), storageOptionsMixin.hasOverrides());
    String warehousePath = rootCommand.resolveWarehousePath(path, storageOptions);
    List<Map<String, Object>> rows = new ArrayList<>();
    for (ResolvedTarget target : rootCommand.warehouseScanner().scan(warehousePath, storageOptions)) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("targetType", target.type().name());
      row.put("inputPath", target.inputPath());
      row.put("tableRoot", target.tableRoot());
      row.put("metadataRoot", target.metadataRoot());
      row.put("currentMetadataFile", target.currentMetadataFile());
      row.put("formatVersion", rootCommand.versionDetector().detect(target.currentMetadataFile(), storageOptions));
      rows.add(row);
    }

    AnalysisResult result = new AnalysisResult(
        "scan-warehouse",
        warehousePath,
        null,
        new AnalysisRequest(AnalysisScope.CURRENT, AnalysisGroupBy.TABLE, AnalysisPrecision.SUMMARY),
        rows);

    String output = renderOptions.resolve() == RenderFormat.JSON
        ? jsonRenderer.render(result)
        : tableRenderer.render(result);
    spec.commandLine().getOut().println(output);
    return 0;
  }
}
