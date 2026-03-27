package com.wayblink.iceberg.loader;

import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.hadoop.HadoopFileIO;

public final class VersionDetector {

  public int detect(Path metadataFile) {
    HadoopFileIO fileIO = new HadoopFileIO(new Configuration());
    TableMetadata metadata = TableMetadataParser.read(fileIO, metadataFile.toAbsolutePath().toString());
    return metadata.formatVersion();
  }
}
