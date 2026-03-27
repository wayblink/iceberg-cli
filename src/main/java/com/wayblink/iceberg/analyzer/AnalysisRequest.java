package com.wayblink.iceberg.analyzer;

import java.util.Objects;

public final class AnalysisRequest {

  private final AnalysisScope scope;
  private final AnalysisGroupBy groupBy;
  private final AnalysisPrecision precision;

  public AnalysisRequest(
      AnalysisScope scope,
      AnalysisGroupBy groupBy,
      AnalysisPrecision precision) {
    this.scope = Objects.requireNonNull(scope, "scope");
    this.groupBy = Objects.requireNonNull(groupBy, "groupBy");
    this.precision = Objects.requireNonNull(precision, "precision");
  }

  public AnalysisScope scope() {
    return scope;
  }

  public AnalysisGroupBy groupBy() {
    return groupBy;
  }

  public AnalysisPrecision precision() {
    return precision;
  }
}
