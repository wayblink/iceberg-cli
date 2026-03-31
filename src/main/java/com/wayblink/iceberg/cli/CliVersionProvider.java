package com.wayblink.iceberg.cli;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import picocli.CommandLine.IVersionProvider;

public final class CliVersionProvider implements IVersionProvider {

  @Override
  public String[] getVersion() {
    Properties application = loadProperties("/version.properties");
    Properties git = loadProperties("/git.properties");

    String name = valueOrDefault(application.getProperty("app.name"), "iceberg-inspect");
    String version = valueOrDefault(application.getProperty("app.version"), "unknown");
    String commitId = normalize(git.getProperty("git.commit.id.abbrev"));
    String dirty = normalize(git.getProperty("git.dirty"));

    StringBuilder line = new StringBuilder(name).append(' ').append(version);
    if (commitId != null) {
      line.append(" (commit ").append(commitId);
      if ("true".equalsIgnoreCase(dirty)) {
        line.append(", dirty");
      }
      line.append(')');
    }
    return new String[] {line.toString()};
  }

  private static Properties loadProperties(String resourcePath) {
    Properties properties = new Properties();
    try (InputStream stream = CliVersionProvider.class.getResourceAsStream(resourcePath)) {
      if (stream != null) {
        properties.load(stream);
      }
    } catch (IOException ignored) {
      // Fall back to defaults when version metadata is unavailable.
    }
    return properties;
  }

  private static String valueOrDefault(String value, String fallback) {
    String normalized = normalize(value);
    return normalized == null ? fallback : normalized;
  }

  private static String normalize(String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    if (trimmed.isEmpty() || trimmed.startsWith("${")) {
      return null;
    }
    return trimmed;
  }
}