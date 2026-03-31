package com.wayblink.iceberg.analyzer;

import java.util.Objects;

public final class AnalysisRequest {

  private final AnalysisGroupBy groupBy;
  private final AnalysisPrecision precision;
  private final Long snapshotId;

  public AnalysisRequest(
      AnalysisGroupBy groupBy,
      AnalysisPrecision precision) {
    this(groupBy, precision, null);
  }

  public AnalysisRequest(
      AnalysisGroupBy groupBy,
      AnalysisPrecision precision,
      Long snapshotId) {
    this.groupBy = Objects.requireNonNull(groupBy, "groupBy");
    this.precision = Objects.requireNonNull(precision, "precision");
    this.snapshotId = snapshotId;
  }

  public AnalysisGroupBy groupBy() {
    return groupBy;
  }

  public AnalysisPrecision precision() {
    return precision;
  }

  public Long snapshotId() {
    return snapshotId;
  }
}
