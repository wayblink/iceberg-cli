package com.wayblink.iceberg.loader;

import com.wayblink.iceberg.io.DefaultFileIOProvider;
import com.wayblink.iceberg.io.FileIOProvider;
import com.wayblink.iceberg.storage.StorageOptions;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

public final class VersionDetector {

  private final FileIOProvider fileIOProvider;

  public VersionDetector() {
    this(new DefaultFileIOProvider());
  }

  public VersionDetector(FileIOProvider fileIOProvider) {
    this.fileIOProvider = fileIOProvider;
  }

  public int detect(String metadataFile) {
    return detect(metadataFile, StorageOptions.defaults());
  }

  public int detect(String metadataFile, StorageOptions options) {
    FileIO fileIO = fileIOProvider.createBootstrap(options, metadataFile);
    TableMetadata metadata = TableMetadataParser.read(fileIO, metadataFile);
    return metadata.formatVersion();
  }
}
