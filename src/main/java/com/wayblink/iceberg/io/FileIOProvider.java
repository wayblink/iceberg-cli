package com.wayblink.iceberg.io;

import com.wayblink.iceberg.storage.StorageOptions;
import org.apache.iceberg.io.FileIO;

public interface FileIOProvider {

  FileIO createBootstrap(StorageOptions options, String metadataFileLocation);

  FileIO createForTable(StorageOptions options, String metadataRoot, String tableLocation);
}
