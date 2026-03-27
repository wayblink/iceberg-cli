package com.wayblink.iceberg.render;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

final class GridTableRenderer {

  private final TerminalStyle style;
  private final ValueFormatter formatter;

  GridTableRenderer(TerminalStyle style, ValueFormatter formatter) {
    this.style = style;
    this.formatter = formatter;
  }

  String render(String title, List<Map<String, Object>> rows) {
    StringJoiner joiner = new StringJoiner(System.lineSeparator());
    joiner.add(style.title(title));
    joiner.add(style.muted(repeat('-', title.length())));
    if (rows.isEmpty()) {
      joiner.add("No rows.");
      return joiner.toString();
    }
    joiner.add("");

    List<String> columns = orderedColumns(rows);
    List<Integer> widths = columnWidths(columns, rows);
    joiner.add(renderHeader(columns, widths));
    joiner.add(style.muted(renderSeparator(widths)));
    for (Map<String, Object> row : rows) {
      joiner.add(renderRow(columns, widths, row));
    }
    return joiner.toString();
  }

  private List<String> orderedColumns(List<Map<String, Object>> rows) {
    LinkedHashSet<String> columns = new LinkedHashSet<>();
    for (Map<String, Object> row : rows) {
      columns.addAll(row.keySet());
    }
    return new ArrayList<>(columns);
  }

  private List<Integer> columnWidths(List<String> columns, List<Map<String, Object>> rows) {
    List<Integer> widths = new ArrayList<>(columns.size());
    for (String column : columns) {
      int width = formatter.headerLabel(column).length();
      for (Map<String, Object> row : rows) {
        String formatted = formatter.format(column, row.get(column));
        width = Math.max(width, Math.min(formatter.maxWidthFor(column), formatted.length()));
      }
      widths.add(Math.min(width, formatter.maxWidthFor(column)));
    }
    return widths;
  }

  private String renderHeader(List<String> columns, List<Integer> widths) {
    List<String> parts = new ArrayList<>(columns.size());
    for (int index = 0; index < columns.size(); index++) {
      parts.add(pad(formatter.headerLabel(columns.get(index)), widths.get(index), false));
    }
    return style.header(String.join("  ", parts));
  }

  private String renderSeparator(List<Integer> widths) {
    List<String> parts = new ArrayList<>(widths.size());
    for (Integer width : widths) {
      parts.add(repeat('-', width));
    }
    return String.join("  ", parts);
  }

  private String renderRow(List<String> columns, List<Integer> widths, Map<String, Object> row) {
    List<String> parts = new ArrayList<>(columns.size());
    for (int index = 0; index < columns.size(); index++) {
      String column = columns.get(index);
      Object value = row.get(column);
      String rendered = formatter.truncate(formatter.format(column, value), widths.get(index));
      parts.add(pad(rendered, widths.get(index), formatter.isRightAligned(column, value)));
    }
    return String.join("  ", parts);
  }

  private String pad(String value, int width, boolean rightAlign) {
    String pattern = rightAlign ? "%" + width + "s" : "%-" + width + "s";
    return String.format(pattern, value);
  }

  private String repeat(char ch, int count) {
    return String.valueOf(ch).repeat(Math.max(0, count));
  }
}
