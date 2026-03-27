package com.wayblink.iceberg.analyzer;

public enum AnalysisScope {
  CURRENT("current"),
  HISTORY("history"),
  ALL("all");

  private final String cliName;

  AnalysisScope(String cliName) {
    this.cliName = cliName;
  }

  public String cliName() {
    return cliName;
  }

  public static AnalysisScope parse(String rawValue) {
    for (AnalysisScope scope : values()) {
      if (scope.cliName.equalsIgnoreCase(rawValue)) {
        return scope;
      }
    }
    throw new IllegalArgumentException("Unsupported scope: " + rawValue);
  }
}
