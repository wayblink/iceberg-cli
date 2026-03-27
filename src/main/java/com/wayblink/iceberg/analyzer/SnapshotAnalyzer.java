package com.wayblink.iceberg.analyzer;

import com.wayblink.iceberg.loader.TableContext;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Snapshot;

public final class SnapshotAnalyzer {

  private final ManifestTraversalService traversalService;

  public SnapshotAnalyzer(ManifestTraversalService traversalService) {
    this.traversalService = traversalService;
  }

  public List<Map<String, Object>> analyze(TableContext tableContext, AnalysisScope scope) {
    List<Map<String, Object>> rows = new ArrayList<>();
    if (scope == AnalysisScope.CURRENT) {
      Snapshot currentSnapshot = tableContext.table().currentSnapshot();
      if (currentSnapshot != null) {
        rows.add(rowForSnapshot(tableContext, currentSnapshot, "current"));
      }
      return rows;
    }

    List<Snapshot> snapshots = new ArrayList<>();
    tableContext.table().snapshots().forEach(snapshots::add);
    snapshots.sort(Comparator.comparingLong(Snapshot::timestampMillis));
    for (Snapshot snapshot : snapshots) {
      rows.add(rowForSnapshot(tableContext, snapshot, "history"));
    }

    if (scope == AnalysisScope.ALL) {
      Snapshot currentSnapshot = tableContext.table().currentSnapshot();
      if (currentSnapshot != null) {
        rows.add(0, rowForSnapshot(tableContext, currentSnapshot, "current"));
      }
    }
    return rows;
  }

  private Map<String, Object> rowForSnapshot(TableContext tableContext, Snapshot snapshot, String rowScope) {
    ManifestTraversalService.FileMetrics dataMetrics = traversalService.summarizeDataFiles(tableContext, snapshot);
    ManifestTraversalService.FileMetrics deleteMetrics = traversalService.summarizeDeleteFiles(tableContext, snapshot);
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("rowScope", rowScope);
    row.put("snapshotId", snapshot.snapshotId());
    row.put("sequenceNumber", snapshot.sequenceNumber());
    row.put("timestampMs", snapshot.timestampMillis());
    row.put("operation", snapshot.operation());
    row.put("dataManifestCount", snapshot.dataManifests(tableContext.fileIO()).size());
    row.put("deleteManifestCount", snapshot.deleteManifests(tableContext.fileIO()).size());
    row.put("dataFileCount", dataMetrics.dataFileCount());
    row.put("deleteFileCount", deleteMetrics.deleteFileCount());
    row.put("positionDeleteFileCount", deleteMetrics.positionDeleteFileCount());
    row.put("equalityDeleteFileCount", deleteMetrics.equalityDeleteFileCount());
    row.put("dataBytes", dataMetrics.dataBytes());
    row.put("deleteBytes", deleteMetrics.deleteBytes());
    row.put("deletionVectorCount", null);
    return row;
  }
}
