package com.wayblink.iceberg.storage;

public enum StorageBackend {
  LOCAL,
  HDFS,
  S3A;

  public static StorageBackend parse(String raw) {
    if (raw == null || raw.isBlank()) {
      return null;
    }

    switch (raw.trim().toLowerCase()) {
      case "local":
      case "file":
        return LOCAL;
      case "hdfs":
        return HDFS;
      case "s3a":
        return S3A;
      case "s3":
        throw new IllegalArgumentException("Unsupported storage backend s3. Use s3a for Hadoop and Iceberg.");
      default:
        throw new IllegalArgumentException("Unsupported storage backend: " + raw);
    }
  }
}
