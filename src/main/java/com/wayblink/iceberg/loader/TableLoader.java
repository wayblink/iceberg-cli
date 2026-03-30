package com.wayblink.iceberg.loader;

import com.wayblink.iceberg.io.FileIOProvider;
import com.wayblink.iceberg.session.SessionState;
import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StorageOptions;
import com.wayblink.iceberg.storage.StoragePaths;
import java.util.Objects;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

public final class TableLoader {

  private final FileIOProvider fileIOProvider;

  public TableLoader(FileIOProvider fileIOProvider) {
    this.fileIOProvider = Objects.requireNonNull(fileIOProvider, "fileIOProvider");
  }

  public TableContext load(SessionState sessionState) {
    StorageOptions options = sessionState.storageOptions();
    StorageBackend backend = options.resolveBackend(sessionState.currentMetadataFile());
    String metadataFile = StoragePaths.normalize(sessionState.currentMetadataFile(), backend);
    String metadataRoot = StoragePaths.normalize(sessionState.metadataRoot(), backend);
    String tableRoot = sessionState.tableRoot() == null
        ? StoragePaths.parent(metadataRoot, backend)
        : StoragePaths.normalize(sessionState.tableRoot(), backend);

    FileIO bootstrapFileIO = fileIOProvider.createBootstrap(options, metadataFile);
    TableMetadata metadata = TableMetadataParser.read(bootstrapFileIO, metadataFile);
    FileIO fileIO = fileIOProvider.createForTable(options, metadataRoot, metadata.location());
    Table table =
        new BaseTable(new StaticTableOperations(metadataFile, fileIO), StoragePaths.fileName(tableRoot, backend));
    return new TableContext(tableRoot, metadataRoot, metadataFile, fileIO, metadata, table);
  }
}
