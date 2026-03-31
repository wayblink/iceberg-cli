package com.wayblink.iceberg.render;

import com.wayblink.iceberg.analyzer.AnalysisResult;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class JsonPayloadBuilder {

  public Map<String, Object> analysis(AnalysisResult result) {
    Map<String, Object> request = new LinkedHashMap<>();
    request.put("groupBy", result.getGroupBy());
    request.put("mode", result.getPrecision());
    if (result.getSnapshotId() != null) {
      request.put("snapshotId", result.getSnapshotId());
    }
    return payload(
        result.getResultType(),
        result.getTarget(),
        result.getFormatVersion(),
        request,
        summary(result.getRows().size()),
        result.getRows());
  }

  public Map<String, Object> payload(
      String resultType,
      String targetPath,
      Integer formatVersion,
      Map<String, Object> request,
      Map<String, Object> summary,
      List<Map<String, Object>> rows) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("resultType", resultType);
    payload.put("target", target(targetPath, formatVersion));
    payload.put("request", normalizeMap(request));

    List<Map<String, Object>> finalRows = normalizeRows(rows);
    Map<String, Object> finalSummary = normalizeMap(summary);
    finalSummary.putIfAbsent("rowCount", finalRows.size());
    payload.put("summary", finalSummary);
    payload.put("rows", finalRows);
    return payload;
  }

  public Map<String, Object> summary(int rowCount) {
    Map<String, Object> summary = new LinkedHashMap<>();
    summary.put("rowCount", rowCount);
    return summary;
  }

  public Map<String, Object> target(String path, Integer formatVersion) {
    Map<String, Object> target = new LinkedHashMap<>();
    target.put("path", path);
    target.put("formatVersion", formatVersion);
    return normalizeMap(target);
  }

  private List<Map<String, Object>> normalizeRows(List<Map<String, Object>> rows) {
    if (rows == null || rows.isEmpty()) {
      return List.of();
    }
    List<Map<String, Object>> normalized = new ArrayList<>(rows.size());
    for (Map<String, Object> row : rows) {
      normalized.add(normalizeMap(row));
    }
    return normalized;
  }

  private Map<String, Object> normalizeMap(Map<String, Object> input) {
    Map<String, Object> normalized = new LinkedHashMap<>();
    if (input == null || input.isEmpty()) {
      return normalized;
    }
    for (Map.Entry<String, Object> entry : input.entrySet()) {
      Object value = normalizeValue(entry.getValue());
      if (value != null) {
        normalized.put(entry.getKey(), value);
      }
    }
    return normalized;
  }

  private Object normalizeValue(Object value) {
    if (value == null) {
      return null;
    }
    if (value instanceof Path) {
      return ((Path) value).toString();
    }
    if (value instanceof Map<?, ?>) {
      Map<?, ?> map = (Map<?, ?>) value;
      Map<String, Object> normalized = new LinkedHashMap<>();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object normalizedValue = normalizeValue(entry.getValue());
        if (normalizedValue != null && entry.getKey() != null) {
          normalized.put(String.valueOf(entry.getKey()), normalizedValue);
        }
      }
      return normalized;
    }
    if (value instanceof List<?>) {
      List<?> list = (List<?>) value;
      List<Object> normalized = new ArrayList<>(list.size());
      for (Object item : list) {
        Object normalizedItem = normalizeValue(item);
        if (normalizedItem != null) {
          normalized.add(normalizedItem);
        }
      }
      return normalized;
    }
    return value;
  }
}
