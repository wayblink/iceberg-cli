package com.wayblink.iceberg.analyzer;

public enum AnalysisPrecision {
  AUTO("auto"),
  SUMMARY("summary"),
  DETAIL("detail");

  private final String cliName;

  AnalysisPrecision(String cliName) {
    this.cliName = cliName;
  }

  public String cliName() {
    return cliName;
  }

  public static AnalysisPrecision parse(String rawValue) {
    for (AnalysisPrecision precision : values()) {
      if (precision.cliName.equalsIgnoreCase(rawValue)) {
        return precision;
      }
    }
    throw new IllegalArgumentException("Unsupported mode: " + rawValue);
  }
}
