package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.render.RenderFormat;
import picocli.CommandLine.Option;

final class RenderOptions {

  @Option(names = "--format", defaultValue = "table", description = "Output format: table or json.")
  private String format;

  @Option(names = "--json", description = "Shortcut for --format json.")
  private boolean json;

  RenderFormat resolve() {
    return json ? RenderFormat.JSON : RenderFormat.parse(format);
  }
}
