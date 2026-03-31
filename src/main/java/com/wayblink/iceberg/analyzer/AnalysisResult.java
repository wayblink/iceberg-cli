package com.wayblink.iceberg.analyzer;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class AnalysisResult {

  private final String resultType;
  private final String target;
  private final Integer formatVersion;
  private final AnalysisRequest request;
  private final List<Map<String, Object>> rows;

  public AnalysisResult(
      String resultType,
      String target,
      Integer formatVersion,
      AnalysisRequest request,
      List<Map<String, Object>> rows) {
    this.resultType = Objects.requireNonNull(resultType, "resultType");
    this.target = Objects.requireNonNull(target, "target");
    this.formatVersion = formatVersion;
    this.request = Objects.requireNonNull(request, "request");
    this.rows = Objects.requireNonNull(rows, "rows");
  }

  public String getResultType() {
    return resultType;
  }

  public String getTarget() {
    return target;
  }

  public Integer getFormatVersion() {
    return formatVersion;
  }

  public String getGroupBy() {
    return request.groupBy().cliName();
  }

  public String getPrecision() {
    return request.precision().cliName();
  }

  public Long getSnapshotId() {
    return request.snapshotId();
  }

  public List<Map<String, Object>> getRows() {
    return rows;
  }
}
