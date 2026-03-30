package com.wayblink.iceberg.io;

import com.wayblink.iceberg.storage.StorageOptions;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

public final class LocalFileIOProvider implements FileIOProvider {

  @Override
  public FileIO createBootstrap(StorageOptions options, String metadataFileLocation) {
    return new HadoopFileIO(new Configuration());
  }

  @Override
  public FileIO createForTable(StorageOptions options, String metadataRoot, String tableLocation) {
    Path metadataDirectory = Paths.get(metadataRoot).toAbsolutePath().normalize();
    return new LocalMetadataMirrorFileIO(createBootstrap(options, metadataRoot), metadataDirectory, tableLocation);
  }
}
