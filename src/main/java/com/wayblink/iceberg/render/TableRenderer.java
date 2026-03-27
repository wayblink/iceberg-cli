package com.wayblink.iceberg.render;

import com.wayblink.iceberg.analyzer.AnalysisResult;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class TableRenderer {

  private final SummaryRenderer summaryRenderer;
  private final GridTableRenderer gridTableRenderer;

  public TableRenderer() {
    this(new TerminalCapabilities());
  }

  TableRenderer(TerminalCapabilities capabilities) {
    TerminalStyle style = new TerminalStyle(capabilities);
    ValueFormatter formatter = new ValueFormatter();
    this.summaryRenderer = new SummaryRenderer(style, formatter);
    this.gridTableRenderer = new GridTableRenderer(style, formatter);
  }

  public String render(AnalysisResult result) {
    if (shouldRenderSummary(result)) {
      return summaryRenderer.renderAnalysisSummary(
          titleFor(result.getResultType(), true),
          metadataFor(result),
          result.getRows().isEmpty() ? Map.of() : result.getRows().get(0));
    }
    return gridTableRenderer.render(titleFor(result.getResultType(), false), result.getRows());
  }

  public String renderSummary(String title, Map<String, Object> context, Map<String, Object> metrics) {
    return summaryRenderer.render(title, context, metrics);
  }

  public String renderGrid(String title, List<Map<String, Object>> rows) {
    return gridTableRenderer.render(title, rows);
  }

  private boolean shouldRenderSummary(AnalysisResult result) {
    return result.getRows().size() <= 1 && "table".equals(result.getGroupBy());
  }

  private Map<String, Object> metadataFor(AnalysisResult result) {
    Map<String, Object> metadata = new LinkedHashMap<>();
    metadata.put("target", result.getTarget());
    if (result.getFormatVersion() != null) {
      metadata.put("formatVersion", result.getFormatVersion());
    }
    metadata.put("scope", result.getScope());
    metadata.put("groupBy", result.getGroupBy());
    if (!"auto".equals(result.getPrecision())) {
      metadata.put("mode", result.getPrecision());
    }
    return metadata;
  }

  private String titleFor(String resultType, boolean summary) {
    switch (resultType) {
      case "table":
        return "Table Summary";
      case "scan-table":
        return "Table Target";
      case "scan-warehouse":
        return "Warehouse Scan";
      case "warehouse":
        return summary ? "Warehouse Summary" : "Warehouse Statistics";
      case "snapshot":
        return summary ? "Snapshot Summary" : "Snapshot Statistics";
      case "metadata-version":
        return summary ? "Metadata Summary" : "Metadata Versions";
      case "partition":
        return summary ? "Partition Summary" : "Partition Statistics";
      default:
        return summary ? "Result Summary" : "Result";
    }
  }
}
