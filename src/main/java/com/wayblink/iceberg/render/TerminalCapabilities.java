package com.wayblink.iceberg.render;

public final class TerminalCapabilities {

  private final boolean colorEnabled;

  public TerminalCapabilities() {
    this(System.console() != null);
  }

  TerminalCapabilities(boolean colorEnabled) {
    this.colorEnabled = colorEnabled;
  }

  public boolean colorEnabled() {
    return colorEnabled;
  }
}
