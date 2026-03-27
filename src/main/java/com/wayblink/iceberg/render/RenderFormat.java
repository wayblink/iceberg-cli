package com.wayblink.iceberg.render;

public enum RenderFormat {
  TABLE("table"),
  JSON("json");

  private final String cliName;

  RenderFormat(String cliName) {
    this.cliName = cliName;
  }

  public static RenderFormat parse(String rawValue) {
    for (RenderFormat format : values()) {
      if (format.cliName.equalsIgnoreCase(rawValue)) {
        return format;
      }
    }
    throw new IllegalArgumentException("Unsupported format: " + rawValue);
  }
}
