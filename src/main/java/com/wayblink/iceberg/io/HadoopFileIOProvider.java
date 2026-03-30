package com.wayblink.iceberg.io;

import com.wayblink.iceberg.storage.HadoopConfigurationFactory;
import com.wayblink.iceberg.storage.StorageOptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

public final class HadoopFileIOProvider implements FileIOProvider {

  private final HadoopConfigurationFactory configurationFactory;

  public HadoopFileIOProvider() {
    this(new HadoopConfigurationFactory());
  }

  public HadoopFileIOProvider(HadoopConfigurationFactory configurationFactory) {
    this.configurationFactory = configurationFactory;
  }

  @Override
  public FileIO createBootstrap(StorageOptions options, String metadataFileLocation) {
    Configuration configuration = configurationFactory.create(options);
    return new HadoopFileIO(configuration);
  }

  @Override
  public FileIO createForTable(StorageOptions options, String metadataRoot, String tableLocation) {
    return createBootstrap(options, metadataRoot);
  }
}
