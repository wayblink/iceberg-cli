package com.wayblink.iceberg.loader;

import com.wayblink.iceberg.io.LocalFileIOProvider;
import com.wayblink.iceberg.session.SessionState;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

public final class TableLoader {

  private final LocalFileIOProvider fileIOProvider;

  public TableLoader(LocalFileIOProvider fileIOProvider) {
    this.fileIOProvider = Objects.requireNonNull(fileIOProvider, "fileIOProvider");
  }

  public TableContext load(SessionState sessionState) {
    Path metadataFile = Paths.get(sessionState.currentMetadataFile()).toAbsolutePath().normalize();
    Path metadataRoot = Paths.get(sessionState.metadataRoot()).toAbsolutePath().normalize();
    Path tableRoot = sessionState.tableRoot() == null ? metadataRoot.getParent() : Paths.get(sessionState.tableRoot());

    FileIO bootstrapFileIO = fileIOProvider.create();
    TableMetadata metadata = TableMetadataParser.read(bootstrapFileIO, metadataFile.toString());
    FileIO fileIO = fileIOProvider.create(metadataRoot, metadata.location());
    Table table = new BaseTable(new StaticTableOperations(metadataFile.toString(), fileIO), tableRoot.getFileName().toString());
    return new TableContext(tableRoot, metadataRoot, metadataFile, fileIO, metadata, table);
  }
}
