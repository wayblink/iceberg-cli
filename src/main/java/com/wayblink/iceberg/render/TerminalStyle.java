package com.wayblink.iceberg.render;

final class TerminalStyle {

  private static final String RESET = "\u001B[0m";
  private static final String BOLD_CYAN = "\u001B[1;36m";
  private static final String BOLD = "\u001B[1m";
  private static final String DIM = "\u001B[2m";

  private final boolean colorEnabled;

  TerminalStyle(TerminalCapabilities capabilities) {
    this.colorEnabled = capabilities.colorEnabled();
  }

  String title(String text) {
    return wrap(BOLD_CYAN, text);
  }

  String header(String text) {
    return wrap(BOLD, text);
  }

  String muted(String text) {
    return wrap(DIM, text);
  }

  private String wrap(String prefix, String text) {
    if (!colorEnabled) {
      return text;
    }
    return prefix + text + RESET;
  }
}
