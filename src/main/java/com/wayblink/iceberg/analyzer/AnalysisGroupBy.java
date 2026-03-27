package com.wayblink.iceberg.analyzer;

public enum AnalysisGroupBy {
  TABLE("table"),
  METADATA_VERSION("metadata-version"),
  SNAPSHOT("snapshot"),
  PARTITION("partition");

  private final String cliName;

  AnalysisGroupBy(String cliName) {
    this.cliName = cliName;
  }

  public String cliName() {
    return cliName;
  }

  public static AnalysisGroupBy parse(String rawValue) {
    for (AnalysisGroupBy groupBy : values()) {
      if (groupBy.cliName.equalsIgnoreCase(rawValue)) {
        return groupBy;
      }
    }
    throw new IllegalArgumentException("Unsupported group-by: " + rawValue);
  }
}
