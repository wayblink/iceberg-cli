package com.wayblink.iceberg.analyzer;

import com.wayblink.iceberg.loader.TableContext;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.Snapshot;

public final class TableAnalyzer {

  private final ManifestTraversalService traversalService;
  private final MetadataVersionAnalyzer metadataVersionAnalyzer;
  private final SnapshotAnalyzer snapshotAnalyzer;
  private final PartitionAnalyzer partitionAnalyzer;

  public TableAnalyzer() {
    this(new ManifestTraversalService());
  }

  public TableAnalyzer(ManifestTraversalService traversalService) {
    this.traversalService = traversalService;
    this.metadataVersionAnalyzer = new MetadataVersionAnalyzer();
    this.snapshotAnalyzer = new SnapshotAnalyzer(traversalService);
    this.partitionAnalyzer = new PartitionAnalyzer(traversalService);
  }

  public AnalysisResult analyzeTable(TableContext tableContext, AnalysisRequest request) {
    List<Map<String, Object>> rows;
    switch (request.groupBy()) {
      case TABLE:
        rows = analyzeTableSummary(tableContext);
        break;
      case METADATA_VERSION:
        rows = metadataVersionAnalyzer.analyze(tableContext);
        break;
      case SNAPSHOT:
        rows = snapshotAnalyzer.analyze(tableContext, request.scope());
        break;
      case PARTITION:
        rows = partitionAnalyzer.analyze(tableContext, request.scope());
        break;
      default:
        throw new IllegalStateException("Unsupported group-by: " + request.groupBy());
    }
    return new AnalysisResult(
        "table",
        tableContext.tableRoot().toString(),
        tableContext.metadata().formatVersion(),
        request,
        rows);
  }

  private List<Map<String, Object>> analyzeTableSummary(TableContext tableContext) {
    List<Map<String, Object>> rows = new ArrayList<>();
    Snapshot currentSnapshot = tableContext.table().currentSnapshot();
    ManifestTraversalService.FileMetrics currentDataMetrics = currentSnapshot == null
        ? new ManifestTraversalService.FileMetrics()
        : traversalService.summarizeDataFiles(tableContext, currentSnapshot);
    ManifestTraversalService.FileMetrics currentDeleteMetrics = currentSnapshot == null
        ? new ManifestTraversalService.FileMetrics()
        : traversalService.summarizeDeleteFiles(tableContext, currentSnapshot);

    Map<String, ContentFile<?>> uniqueDataFiles = new LinkedHashMap<>();
    Map<String, ContentFile<?>> uniqueDeleteFiles = new LinkedHashMap<>();
    tableContext.table().snapshots().forEach(snapshot -> {
      uniqueDataFiles.putAll(traversalService.uniqueDataFiles(tableContext, snapshot));
      uniqueDeleteFiles.putAll(traversalService.uniqueDeleteFiles(tableContext, snapshot));
    });

    long partitionCount = currentSnapshot == null
        ? 0
        : traversalService.summarizeDataPartitions(tableContext, currentSnapshot).size();

    Map<String, Object> row = new LinkedHashMap<>();
    row.put("tableRoot", tableContext.tableRoot().toString());
    row.put("formatVersion", tableContext.metadata().formatVersion());
    row.put("metadataVersionCount", tableContext.metadata().previousFiles().size() + 1L);
    row.put("snapshotVersionCount", tableContext.metadata().snapshots().size());
    row.put("partitionSpecCount", tableContext.metadata().specs().size());
    row.put("partitionCount", partitionCount);
    row.put("currentDataFileCount", currentDataMetrics.dataFileCount());
    row.put("currentDeleteFileCount", currentDeleteMetrics.deleteFileCount());
    row.put("currentDataBytes", currentDataMetrics.dataBytes());
    row.put("currentDeleteBytes", currentDeleteMetrics.deleteBytes());
    row.put("uniqueHistoricalDataFileCount", uniqueDataFiles.size());
    row.put("uniqueHistoricalDeleteFileCount", uniqueDeleteFiles.size());
    row.put("deletionVectorCount", null);
    rows.add(row);
    return rows;
  }
}
