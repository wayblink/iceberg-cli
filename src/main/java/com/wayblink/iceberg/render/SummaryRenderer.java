package com.wayblink.iceberg.render;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

final class SummaryRenderer {

  private final TerminalStyle style;
  private final ValueFormatter formatter;

  SummaryRenderer(TerminalStyle style, ValueFormatter formatter) {
    this.style = style;
    this.formatter = formatter;
  }

  String render(String title, Map<String, Object> context, Map<String, Object> metrics) {
    StringJoiner joiner = new StringJoiner(System.lineSeparator());
    joiner.add(style.title(title));
    joiner.add(style.muted(repeat('-', title.length())));
    joiner.add("");

    List<Map.Entry<String, Object>> contextEntries = new ArrayList<>(context.entrySet());
    int labelWidth = contextEntries.stream()
        .map(Map.Entry::getKey)
        .map(formatter::label)
        .mapToInt(String::length)
        .max()
        .orElse(10);
    for (Map.Entry<String, Object> entry : contextEntries) {
      joiner.add(pad(formatter.label(entry.getKey()), labelWidth, false)
          + "  "
          + formatter.format(entry.getKey(), entry.getValue()));
    }

    if (!metrics.isEmpty()) {
      joiner.add("");
      joiner.add(style.header("Metrics"));
      List<Map.Entry<String, Object>> metricEntries = new ArrayList<>(metrics.entrySet());
      List<String> cells = new ArrayList<>(metricEntries.size());
      int cellWidth = 0;
      for (Map.Entry<String, Object> entry : metricEntries) {
        String cell = formatter.label(entry.getKey()) + "  " + formatter.format(entry.getKey(), entry.getValue());
        cells.add(cell);
        cellWidth = Math.max(cellWidth, cell.length());
      }
      cellWidth = Math.min(Math.max(cellWidth, 24), 42);
      for (int index = 0; index < cells.size(); index += 2) {
        String left = pad(cells.get(index), cellWidth, false);
        String right = index + 1 < cells.size() ? cells.get(index + 1) : "";
        joiner.add(left + (right.isEmpty() ? "" : "  " + right));
      }
    }
    return joiner.toString();
  }

  String renderAnalysisSummary(String title, Map<String, Object> metadata, Map<String, Object> row) {
    Map<String, Object> context = new LinkedHashMap<>(metadata);
    Map<String, Object> metrics = new LinkedHashMap<>();
    for (Map.Entry<String, Object> entry : row.entrySet()) {
      if (context.containsKey(entry.getKey())) {
        continue;
      }
      if (isContextField(entry.getKey())) {
        context.put(entry.getKey(), entry.getValue());
      } else {
        metrics.put(entry.getKey(), entry.getValue());
      }
    }
    return render(title, context, metrics);
  }

  private boolean isContextField(String key) {
    String lowerKey = key.toLowerCase();
    return lowerKey.contains("path")
        || lowerKey.contains("root")
        || lowerKey.contains("file")
        || lowerKey.equals("location")
        || lowerKey.equals("formatversion")
        || lowerKey.equals("targettype")
        || lowerKey.equals("inputpath");
  }

  private String pad(String value, int width, boolean rightAlign) {
    String pattern = rightAlign ? "%" + width + "s" : "%-" + width + "s";
    return String.format(pattern, value);
  }

  private String repeat(char ch, int count) {
    return String.valueOf(ch).repeat(Math.max(0, count));
  }
}
