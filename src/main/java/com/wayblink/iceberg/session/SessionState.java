package com.wayblink.iceberg.session;

import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.discovery.ResolvedTargetType;
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
      String warehouseRoot) {
    this.inputPath = inputPath;
    this.targetType = targetType;
    this.tableRoot = tableRoot;
    this.metadataRoot = metadataRoot;
    this.currentMetadataFile = currentMetadataFile;
    this.formatVersion = formatVersion;
    this.openedAtMillis = openedAtMillis;
    this.updatedAtMillis = updatedAtMillis;
    this.warehouseRoot = warehouseRoot;
  }

  public static SessionState fromResolvedTarget(
      ResolvedTarget target,
      Integer formatVersion,
      long nowMillis,
      String warehouseRoot) {
    return new SessionState(
        target.inputPath().toString(),
        target.type(),
        target.tableRoot() == null ? null : target.tableRoot().toString(),
        target.metadataRoot().toString(),
        target.currentMetadataFile() == null ? null : target.currentMetadataFile().toString(),
        formatVersion,
        nowMillis,
        nowMillis,
        warehouseRoot);
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
}
