package com.wayblink.iceberg.session;

import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.discovery.ResolvedTargetType;
import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StorageOptions;
import com.fasterxml.jackson.annotation.JsonAutoDetect;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY)
public final class SessionState {

  private String inputPath;
  private ResolvedTargetType targetType;
  private String tableRoot;
  private String metadataRoot;
  private String currentMetadataFile;
  private Integer formatVersion;
  private long openedAtMillis;
  private long updatedAtMillis;
  private String warehouseRoot;
  private StorageBackend storageBackend;
  private String hadoopConfDir;
  private String s3Endpoint;
  private String s3Region;
  private boolean s3PathStyle;
  private String s3CredentialsProvider;

  public SessionState() {
  }

  public SessionState(
      String inputPath,
      ResolvedTargetType targetType,
      String tableRoot,
      String metadataRoot,
      String currentMetadataFile,
      Integer formatVersion,
      long openedAtMillis,
      long updatedAtMillis,
      String warehouseRoot,
      StorageBackend storageBackend,
      String hadoopConfDir,
      String s3Endpoint,
      String s3Region,
      boolean s3PathStyle,
      String s3CredentialsProvider) {
    this.inputPath = inputPath;
    this.targetType = targetType;
    this.tableRoot = tableRoot;
    this.metadataRoot = metadataRoot;
    this.currentMetadataFile = currentMetadataFile;
    this.formatVersion = formatVersion;
    this.openedAtMillis = openedAtMillis;
    this.updatedAtMillis = updatedAtMillis;
    this.warehouseRoot = warehouseRoot;
    this.storageBackend = storageBackend;
    this.hadoopConfDir = hadoopConfDir;
    this.s3Endpoint = s3Endpoint;
    this.s3Region = s3Region;
    this.s3PathStyle = s3PathStyle;
    this.s3CredentialsProvider = s3CredentialsProvider;
  }

  public static SessionState fromResolvedTarget(
      ResolvedTarget target,
      Integer formatVersion,
      long nowMillis,
      String warehouseRoot,
      StorageOptions storageOptions) {
    return new SessionState(
        target.inputPath(),
        target.type(),
        target.tableRoot(),
        target.metadataRoot(),
        target.currentMetadataFile(),
        formatVersion,
        nowMillis,
        nowMillis,
        warehouseRoot,
        storageOptions.resolveBackend(target.inputPath()),
        storageOptions.hadoopConfDir(),
        storageOptions.s3Endpoint(),
        storageOptions.s3Region(),
        storageOptions.s3PathStyle(),
        storageOptions.s3CredentialsProvider());
  }

  public String inputPath() {
    return inputPath;
  }

  public void setInputPath(String inputPath) {
    this.inputPath = inputPath;
  }

  public ResolvedTargetType targetType() {
    return targetType;
  }

  public void setTargetType(ResolvedTargetType targetType) {
    this.targetType = targetType;
  }

  public String tableRoot() {
    return tableRoot;
  }

  public void setTableRoot(String tableRoot) {
    this.tableRoot = tableRoot;
  }

  public String metadataRoot() {
    return metadataRoot;
  }

  public void setMetadataRoot(String metadataRoot) {
    this.metadataRoot = metadataRoot;
  }

  public String currentMetadataFile() {
    return currentMetadataFile;
  }

  public void setCurrentMetadataFile(String currentMetadataFile) {
    this.currentMetadataFile = currentMetadataFile;
  }

  public Integer formatVersion() {
    return formatVersion;
  }

  public void setFormatVersion(Integer formatVersion) {
    this.formatVersion = formatVersion;
  }

  public long openedAtMillis() {
    return openedAtMillis;
  }

  public void setOpenedAtMillis(long openedAtMillis) {
    this.openedAtMillis = openedAtMillis;
  }

  public long updatedAtMillis() {
    return updatedAtMillis;
  }

  public void setUpdatedAtMillis(long updatedAtMillis) {
    this.updatedAtMillis = updatedAtMillis;
  }

  public String warehouseRoot() {
    return warehouseRoot;
  }

  public void setWarehouseRoot(String warehouseRoot) {
    this.warehouseRoot = warehouseRoot;
  }

  public StorageBackend storageBackend() {
    return storageBackend;
  }

  public void setStorageBackend(StorageBackend storageBackend) {
    this.storageBackend = storageBackend;
  }

  public String hadoopConfDir() {
    return hadoopConfDir;
  }

  public void setHadoopConfDir(String hadoopConfDir) {
    this.hadoopConfDir = hadoopConfDir;
  }

  public String s3Endpoint() {
    return s3Endpoint;
  }

  public void setS3Endpoint(String s3Endpoint) {
    this.s3Endpoint = s3Endpoint;
  }

  public String s3Region() {
    return s3Region;
  }

  public void setS3Region(String s3Region) {
    this.s3Region = s3Region;
  }

  public boolean s3PathStyle() {
    return s3PathStyle;
  }

  public void setS3PathStyle(boolean s3PathStyle) {
    this.s3PathStyle = s3PathStyle;
  }

  public String s3CredentialsProvider() {
    return s3CredentialsProvider;
  }

  public void setS3CredentialsProvider(String s3CredentialsProvider) {
    this.s3CredentialsProvider = s3CredentialsProvider;
  }

  public StorageOptions storageOptions() {
    return StorageOptions.builder()
        .backend(storageBackend)
        .hadoopConfDir(hadoopConfDir)
        .s3Endpoint(s3Endpoint)
        .s3Region(s3Region)
        .s3PathStyle(s3PathStyle)
        .s3CredentialsProvider(s3CredentialsProvider)
        .build();
  }
}
