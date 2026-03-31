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
  private final PartitionAnalyzer partitionAnalyzer;

  public TableAnalyzer() {
    this(new ManifestTraversalService());
  }

  public TableAnalyzer(ManifestTraversalService traversalService) {
    this.traversalService = traversalService;
    this.metadataVersionAnalyzer = new MetadataVersionAnalyzer();
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
      case PARTITION:
        rows = partitionAnalyzer.analyze(tableContext, tableContext.table().currentSnapshot());
        break;
      case SNAPSHOT:
      default:
        throw new IllegalStateException("Unsupported group-by: " + request.groupBy());
    }
    return new AnalysisResult(
        "table",
        tableContext.tableRoot(),
        tableContext.metadata().formatVersion(),
        request,
        rows);
  }

  private List<Map<String, Object>> analyzeTableSummary(TableContext tableContext) {
    List<Map<String, Object>> rows = new ArrayList<>();
    Snapshot currentSnapshot = tableContext.table().currentSnapshot();
    Map<String, ContentFile<?>> currentUniqueDataFiles = currentSnapshot == null
        ? new LinkedHashMap<>()
        : traversalService.uniqueDataFiles(tableContext, currentSnapshot);
    Map<String, ContentFile<?>> currentUniqueDeleteFiles = currentSnapshot == null
        ? new LinkedHashMap<>()
        : traversalService.uniqueDeleteFiles(tableContext, currentSnapshot);
    ManifestTraversalService.FileMetrics currentDataMetrics =
        traversalService.summarizeFiles(currentUniqueDataFiles);
    ManifestTraversalService.FileMetrics currentDeleteMetrics =
        traversalService.summarizeFiles(currentUniqueDeleteFiles);

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
    row.put("tableRoot", tableContext.tableRoot());
    row.put("formatVersion", tableContext.metadata().formatVersion());
    row.put("metadataVersionCount", tableContext.metadata().previousFiles().size() + 1L);
    row.put("snapshotVersionCount", tableContext.metadata().snapshots().size());
    row.put("partitionSpecCount", tableContext.metadata().specs().size());
    row.put("partitionCount", partitionCount);
    row.put("currentDataFileCount", currentDataMetrics.dataFileCount());
    row.put("currentDeleteFileCount", currentDeleteMetrics.deleteFileCount());
    row.put("currentDataBytes", currentDataMetrics.dataBytes());
    row.put("currentDeleteBytes", currentDeleteMetrics.deleteBytes());
    row.put("tableRowCount", currentDataMetrics.dataRecordCount());
    row.put("deletedRecordCount", currentDeleteMetrics.deleteRecordCount());
    row.put("uniqueHistoricalDataFileCount", uniqueDataFiles.size());
    row.put("uniqueHistoricalDeleteFileCount", uniqueDeleteFiles.size());
    row.put("deletionVectorCount", currentDeleteMetrics.deletionVectorCount());
    rows.add(row);
    return rows;
  }
}
