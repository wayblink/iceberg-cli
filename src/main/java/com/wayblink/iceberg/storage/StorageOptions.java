package com.wayblink.iceberg.storage;

public final class StorageOptions {

  private final StorageBackend backend;
  private final String hadoopConfDir;
  private final String s3Endpoint;
  private final String s3Region;
  private final boolean s3PathStyle;
  private final String s3CredentialsProvider;

  private StorageOptions(Builder builder) {
    this.backend = builder.backend;
    this.hadoopConfDir = builder.hadoopConfDir;
    this.s3Endpoint = builder.s3Endpoint;
    this.s3Region = builder.s3Region;
    this.s3PathStyle = builder.s3PathStyle;
    this.s3CredentialsProvider = builder.s3CredentialsProvider;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static StorageOptions defaults() {
    return builder().build();
  }

  public StorageBackend backend() {
    return backend;
  }

  public String hadoopConfDir() {
    return hadoopConfDir;
  }

  public String s3Endpoint() {
    return s3Endpoint;
  }

  public String s3Region() {
    return s3Region;
  }

  public boolean s3PathStyle() {
    return s3PathStyle;
  }

  public String s3CredentialsProvider() {
    return s3CredentialsProvider;
  }

  public boolean isEmpty() {
    return backend == null
        && hadoopConfDir == null
        && s3Endpoint == null
        && s3Region == null
        && !s3PathStyle
        && s3CredentialsProvider == null;
  }

  public StorageOptions overlayOn(StorageOptions base) {
    StorageOptions fallback = base == null ? defaults() : base;
    return builder()
        .backend(backend != null ? backend : fallback.backend())
        .hadoopConfDir(hadoopConfDir != null ? hadoopConfDir : fallback.hadoopConfDir())
        .s3Endpoint(s3Endpoint != null ? s3Endpoint : fallback.s3Endpoint())
        .s3Region(s3Region != null ? s3Region : fallback.s3Region())
        .s3PathStyle(s3PathStyle || fallback.s3PathStyle())
        .s3CredentialsProvider(
            s3CredentialsProvider != null ? s3CredentialsProvider : fallback.s3CredentialsProvider())
        .build();
  }

  public StorageBackend resolveBackend(String path) {
    if (backend != null) {
      return backend;
    }
    String scheme = StoragePaths.scheme(path);
    if (scheme == null || scheme.isBlank() || "file".equalsIgnoreCase(scheme)) {
      return StorageBackend.LOCAL;
    }
    if ("hdfs".equalsIgnoreCase(scheme)) {
      return StorageBackend.HDFS;
    }
    if ("s3a".equalsIgnoreCase(scheme)) {
      return StorageBackend.S3A;
    }
    if ("s3".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("Unsupported URI scheme s3://. Use s3a:// for Hadoop and Iceberg.");
    }
    throw new IllegalArgumentException("Unsupported URI scheme: " + scheme);
  }

  public static final class Builder {

    private StorageBackend backend;
    private String hadoopConfDir;
    private String s3Endpoint;
    private String s3Region;
    private boolean s3PathStyle;
    private String s3CredentialsProvider;

    private Builder() {
    }

    public Builder backend(StorageBackend backend) {
      this.backend = backend;
      return this;
    }

    public Builder hadoopConfDir(String hadoopConfDir) {
      this.hadoopConfDir = hadoopConfDir;
      return this;
    }

    public Builder s3Endpoint(String s3Endpoint) {
      this.s3Endpoint = s3Endpoint;
      return this;
    }

    public Builder s3Region(String s3Region) {
      this.s3Region = s3Region;
      return this;
    }

    public Builder s3PathStyle(boolean s3PathStyle) {
      this.s3PathStyle = s3PathStyle;
      return this;
    }

    public Builder s3CredentialsProvider(String s3CredentialsProvider) {
      this.s3CredentialsProvider = s3CredentialsProvider;
      return this;
    }

    public StorageOptions build() {
      return new StorageOptions(this);
    }
  }
}
