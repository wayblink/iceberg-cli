package com.wayblink.iceberg.io;

import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StorageOptions;

public final class DefaultFileIOProvider implements FileIOProvider {

  private final LocalFileIOProvider localFileIOProvider;
  private final HadoopFileIOProvider hadoopFileIOProvider;

  public DefaultFileIOProvider() {
    this.localFileIOProvider = new LocalFileIOProvider();
    this.hadoopFileIOProvider = new HadoopFileIOProvider();
  }

  @Override
  public org.apache.iceberg.io.FileIO createBootstrap(StorageOptions options, String metadataFileLocation) {
    return select(options, metadataFileLocation).createBootstrap(options, metadataFileLocation);
  }

  @Override
  public org.apache.iceberg.io.FileIO createForTable(StorageOptions options, String metadataRoot, String tableLocation) {
    return select(options, metadataRoot).createForTable(options, metadataRoot, tableLocation);
  }

  private FileIOProvider select(StorageOptions options, String location) {
    StorageBackend backend = options.resolveBackend(location);
    return backend == StorageBackend.LOCAL ? localFileIOProvider : hadoopFileIOProvider;
  }
}
