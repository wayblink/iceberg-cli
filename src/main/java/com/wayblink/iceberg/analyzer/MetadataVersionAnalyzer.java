package com.wayblink.iceberg.analyzer;

import com.wayblink.iceberg.loader.TableContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;

public final class MetadataVersionAnalyzer {

  public List<Map<String, Object>> analyze(TableContext tableContext) {
    List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(rowForMetadata(
        tableContext.currentMetadataFile().toString(),
        tableContext.metadata().lastUpdatedMillis(),
        tableContext.metadata()));
    for (TableMetadata.MetadataLogEntry entry : tableContext.metadata().previousFiles()) {
      TableMetadata metadata = TableMetadataParser.read(tableContext.fileIO(), entry.file());
      rows.add(rowForMetadata(entry.file(), entry.timestampMillis(), metadata));
    }
    return rows;
  }

  private Map<String, Object> rowForMetadata(String metadataFile, long timestampMillis, TableMetadata metadata) {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("metadataFile", metadataFile);
    row.put("timestampMs", timestampMillis);
    row.put("formatVersion", metadata.formatVersion());
    row.put("currentSnapshotId", metadata.currentSnapshot() == null ? null : metadata.currentSnapshot().snapshotId());
    row.put("snapshotCount", metadata.snapshots().size());
    row.put("schemaCount", metadata.schemas().size());
    row.put("partitionSpecCount", metadata.specs().size());
    row.put("refsCount", metadata.refs().size());
    return row;
  }
}
