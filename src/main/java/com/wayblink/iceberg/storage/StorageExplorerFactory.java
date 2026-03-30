package com.wayblink.iceberg.storage;

import org.apache.hadoop.conf.Configuration;

public final class StorageExplorerFactory {

  private final HadoopConfigurationFactory configurationFactory;

  public StorageExplorerFactory() {
    this(new HadoopConfigurationFactory());
  }

  public StorageExplorerFactory(HadoopConfigurationFactory configurationFactory) {
    this.configurationFactory = configurationFactory;
  }

  public StorageExplorer create(String path, StorageOptions options) {
    StorageBackend backend = options.resolveBackend(path);
    if (backend == StorageBackend.LOCAL) {
      return new LocalStorageExplorer();
    }
    Configuration configuration = configurationFactory.create(options);
    return new HadoopStorageExplorer(configuration, backend);
  }
}
