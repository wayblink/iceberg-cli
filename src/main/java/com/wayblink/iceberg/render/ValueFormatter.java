package com.wayblink.iceberg.render;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

final class ValueFormatter {

  private static final long KIB = 1024L;
  private static final long MIB = KIB * 1024L;
  private static final long GIB = MIB * 1024L;
  private static final long TIB = GIB * 1024L;
  private static final DateTimeFormatter TIMESTAMP_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault());
  private static final Map<String, String> LABEL_OVERRIDES = labelOverrides();

  String format(String key, Object value) {
    if (value == null) {
      return "-";
    }
    if (value instanceof Number) {
      return formatNumber(key, (Number) value);
    }
    if (value instanceof Boolean) {
      return Boolean.TRUE.equals(value) ? "yes" : "no";
    }
    return String.valueOf(value);
  }

  boolean isRightAligned(String key, Object value) {
    if (!(value instanceof Number)) {
      return false;
    }
    return !isTimestampKey(key);
  }

  String label(String key) {
    String override = LABEL_OVERRIDES.get(key);
    if (override != null) {
      return override;
    }
    String normalized = key
        .replace('-', ' ')
        .replace('_', ' ')
        .replaceAll("([a-z0-9])([A-Z])", "$1 $2")
        .trim();
    String[] words = normalized.split("\\s+");
    StringBuilder builder = new StringBuilder();
    for (String word : words) {
      if (builder.length() > 0) {
        builder.append(' ');
      }
      if ("id".equalsIgnoreCase(word)) {
        builder.append("ID");
      } else if ("io".equalsIgnoreCase(word)) {
        builder.append("IO");
      } else {
        builder.append(Character.toUpperCase(word.charAt(0)));
        if (word.length() > 1) {
          builder.append(word.substring(1));
        }
      }
    }
    return builder.toString();
  }

  String headerLabel(String key) {
    return label(key).toUpperCase(Locale.ROOT);
  }

  String truncate(String text, int width) {
    if (text.length() <= width) {
      return text;
    }
    if (width <= 3) {
      return text.substring(0, width);
    }
    int prefix = Math.max(1, (width - 3) / 2);
    int suffix = Math.max(1, width - 3 - prefix);
    return text.substring(0, prefix) + "..." + text.substring(text.length() - suffix);
  }

  int maxWidthFor(String key) {
    String lowerKey = key.toLowerCase(Locale.ROOT);
    if (lowerKey.contains("path") || lowerKey.contains("root") || lowerKey.contains("file")) {
      return 64;
    }
    if (isTimestampKey(key)) {
      return 19;
    }
    if (lowerKey.contains("partition")) {
      return 32;
    }
    if (lowerKey.contains("operation") || lowerKey.contains("content")) {
      return 14;
    }
    return 18;
  }

  private String formatNumber(String key, Number number) {
    String lowerKey = key.toLowerCase(Locale.ROOT);
    if (lowerKey.equals("formatversion") || lowerKey.equals("format-version")) {
      return "V" + number.intValue();
    }
    if (isTimestampKey(key)) {
      return TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(number.longValue()));
    }
    if (lowerKey.endsWith("bytes") || lowerKey.contains("size")) {
      return humanBytes(number.longValue());
    }
    if (isCountKey(lowerKey)) {
      return NumberFormat.getIntegerInstance(Locale.US).format(number.longValue());
    }
    if (isIntegral(number)) {
      return Long.toString(number.longValue());
    }
    DecimalFormat decimalFormat = new DecimalFormat("#,##0.##");
    return decimalFormat.format(number.doubleValue());
  }

  private String humanBytes(long bytes) {
    if (bytes < KIB) {
      return bytes + " B";
    }
    if (bytes < MIB) {
      return formatUnit(bytes, KIB, "KiB");
    }
    if (bytes < GIB) {
      return formatUnit(bytes, MIB, "MiB");
    }
    if (bytes < TIB) {
      return formatUnit(bytes, GIB, "GiB");
    }
    return formatUnit(bytes, TIB, "TiB");
  }

  private String formatUnit(long value, long unit, String suffix) {
    DecimalFormat decimalFormat = new DecimalFormat("0.##");
    return decimalFormat.format((double) value / unit) + " " + suffix;
  }

  private boolean isCountKey(String lowerKey) {
    return lowerKey.endsWith("count")
        || lowerKey.endsWith("counts")
        || lowerKey.contains("versioncount")
        || lowerKey.contains("versions")
        || lowerKey.contains("files")
        || lowerKey.contains("rows");
  }

  private boolean isTimestampKey(String key) {
    String lowerKey = key.toLowerCase(Locale.ROOT);
    return lowerKey.endsWith("timestampms")
        || lowerKey.endsWith("timestamp")
        || lowerKey.endsWith("updatedms")
        || lowerKey.equals("lastupdatedms")
        || lowerKey.equals("timestamp-ms")
        || lowerKey.equals("last-updated-ms");
  }

  private boolean isIntegral(Number number) {
    return number instanceof Byte
        || number instanceof Short
        || number instanceof Integer
        || number instanceof Long;
  }

  private static Map<String, String> labelOverrides() {
    Map<String, String> labels = new LinkedHashMap<>();
    labels.put("currentDataFileCount", "Data Files");
    labels.put("currentDeleteFileCount", "Delete Files");
    labels.put("currentDataBytes", "Data Size");
    labels.put("currentDeleteBytes", "Delete Size");
    labels.put("tableRowCount", "Table Rows");
    labels.put("deletedRecordCount", "Deleted Record Rows");
    labels.put("deleteRecordCount", "Deleted Record Rows");
    labels.put("dataRecordCount", "Data Record Rows");
    labels.put("deletionVectorCount", "Deletion Vector Count");
    labels.put("metadataVersionCount", "Metadata Versions");
    labels.put("snapshotVersionCount", "Snapshot Versions");
    labels.put("partitionSpecCount", "Partition Specs");
    labels.put("partitionCount", "Partitions");
    labels.put("schemaCount", "Schemas");
    labels.put("refsCount", "Refs");
    labels.put("currentSnapshotId", "Current Snapshot ID");
    labels.put("formatVersion", "Format Version");
    labels.put("tableRoot", "Table Root");
    labels.put("metadataRoot", "Metadata Root");
    labels.put("currentMetadataFile", "Current Metadata File");
    labels.put("inputPath", "Input Path");
    labels.put("targetType", "Target Type");
    labels.put("manifestPath", "Manifest Path");
    labels.put("manifestCount", "Manifest Count");
    labels.put("dataFileCount", "Data Files");
    labels.put("lastUpdatedMs", "Last Updated");
    return labels;
  }
}
