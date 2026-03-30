package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StorageOptions;
import picocli.CommandLine.Option;

public final class StorageOptionsMixin {

  @Option(names = "--fs", description = "Storage backend: local, hdfs, or s3a. Defaults to the path URI scheme.")
  private String backend;

  @Option(names = "--hadoop-conf-dir", description = "Directory containing core-site.xml and hdfs-site.xml.")
  private String hadoopConfDir;

  @Option(names = "--s3-endpoint", description = "S3A endpoint, for example http://minio:9000.")
  private String s3Endpoint;

  @Option(names = "--s3-region", description = "S3 region or endpoint region.")
  private String s3Region;

  @Option(names = "--s3-path-style", description = "Enable path-style S3A access, useful for MinIO and some gateways.")
  private boolean s3PathStyle;

  @Option(names = "--s3-credentials-provider", description = "Custom Hadoop S3A credentials provider class.")
  private String s3CredentialsProvider;

  public boolean hasOverrides() {
    return backend != null
        || hadoopConfDir != null
        || s3Endpoint != null
        || s3Region != null
        || s3PathStyle
        || s3CredentialsProvider != null;
  }

  public StorageOptions toOptions() {
    return StorageOptions.builder()
        .backend(StorageBackend.parse(backend))
        .hadoopConfDir(hadoopConfDir)
        .s3Endpoint(s3Endpoint)
        .s3Region(s3Region)
        .s3PathStyle(s3PathStyle)
        .s3CredentialsProvider(s3CredentialsProvider)
        .build();
  }
}
