package com.wayblink.iceberg.analyzer;

import com.wayblink.iceberg.loader.TableContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;

public final class PartitionAnalyzer {

  private final ManifestTraversalService traversalService;

  public PartitionAnalyzer(ManifestTraversalService traversalService) {
    this.traversalService = traversalService;
  }

  public List<Map<String, Object>> analyze(TableContext tableContext, AnalysisScope scope) {
    List<Map<String, Object>> rows = new ArrayList<>();
    if (scope == AnalysisScope.CURRENT) {
      Snapshot currentSnapshot = tableContext.table().currentSnapshot();
      if (currentSnapshot != null) {
        rows.addAll(rowsForSnapshot(tableContext, currentSnapshot, "current"));
      }
      return rows;
    }

    List<Snapshot> snapshots = new ArrayList<>();
    tableContext.table().snapshots().forEach(snapshots::add);
    snapshots.sort(Comparator.comparingLong(Snapshot::timestampMillis));
    for (Snapshot snapshot : snapshots) {
      rows.addAll(rowsForSnapshot(tableContext, snapshot, "history"));
    }

    if (scope == AnalysisScope.ALL) {
      Snapshot currentSnapshot = tableContext.table().currentSnapshot();
      if (currentSnapshot != null) {
        rows.addAll(0, rowsForSnapshot(tableContext, currentSnapshot, "current"));
      }
    }
    return rows;
  }

  private List<Map<String, Object>> rowsForSnapshot(TableContext tableContext, Snapshot snapshot, String rowScope) {
    Map<String, ManifestTraversalService.PartitionMetrics> aggregate = new LinkedHashMap<>();
    traversalService.summarizeDataPartitions(tableContext, snapshot)
        .forEach((partition, metrics) -> aggregate.put(partition, metrics));
    traversalService.summarizeDeletePartitions(tableContext, snapshot)
        .forEach((partition, metrics) -> aggregate.computeIfAbsent(partition, ignored -> new ManifestTraversalService.PartitionMetrics()).merge(metrics));

    List<Map<String, Object>> rows = new ArrayList<>();
    for (Map.Entry<String, ManifestTraversalService.PartitionMetrics> entry : aggregate.entrySet()) {
      ManifestTraversalService.PartitionMetrics metrics = entry.getValue();
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("rowScope", rowScope);
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
      row.put("deletionVectorCount", null);
      rows.add(row);
    }
    return rows;
  }
}
