package com.wayblink.iceberg.io;

import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

public final class LocalFileIOProvider {

  public FileIO create() {
    return new HadoopFileIO(new Configuration());
  }

  public FileIO create(Path metadataRoot, String tableLocation) {
    return new LocalMetadataMirrorFileIO(create(), metadataRoot, tableLocation);
  }
}
