package com.wayblink.iceberg.analyzer;

import com.wayblink.iceberg.loader.TableContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;

public final class PartitionAnalyzer {

  private final ManifestTraversalService traversalService;

  public PartitionAnalyzer(ManifestTraversalService traversalService) {
    this.traversalService = traversalService;
  }

  public List<Map<String, Object>> analyze(TableContext tableContext, Snapshot snapshot) {
    if (snapshot == null) {
      return List.of();
    }
    return rowsForSnapshot(tableContext, snapshot);
  }

  private List<Map<String, Object>> rowsForSnapshot(TableContext tableContext, Snapshot snapshot) {
    Map<String, ManifestTraversalService.PartitionMetrics> aggregate = new LinkedHashMap<>();
    traversalService.summarizeDataPartitions(tableContext, snapshot)
        .forEach((partition, metrics) -> aggregate.put(partition, metrics));
    traversalService.summarizeDeletePartitions(tableContext, snapshot)
        .forEach((partition, metrics) -> aggregate.computeIfAbsent(partition, ignored -> new ManifestTraversalService.PartitionMetrics()).merge(metrics));

    List<Map<String, Object>> rows = new ArrayList<>();
    for (Map.Entry<String, ManifestTraversalService.PartitionMetrics> entry : aggregate.entrySet()) {
      ManifestTraversalService.PartitionMetrics metrics = entry.getValue();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("snapshotId", snapshot.snapshotId());
      row.put("timestampMs", snapshot.timestampMillis());
      row.put("partition", entry.getKey());
      row.put("dataFileCount", metrics.dataFileCount());
      row.put("dataRecordCount", metrics.dataRecordCount());
      row.put("dataBytes", metrics.dataBytes());
      row.put("positionDeleteFileCount", metrics.positionDeleteFileCount());
      row.put("equalityDeleteFileCount", metrics.equalityDeleteFileCount());
      row.put("deleteRecordCount", metrics.deleteRecordCount());
      row.put("deleteBytes", metrics.deleteBytes());
      row.put("lastUpdatedSnapshotId", snapshot.snapshotId());
      row.put("deletionVectorCount", metrics.deletionVectorCount());
      rows.add(row);
    }
    return rows;
  }
}
