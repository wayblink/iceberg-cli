package com.wayblink.iceberg.render;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wayblink.iceberg.analyzer.AnalysisGroupBy;
import com.wayblink.iceberg.analyzer.AnalysisPrecision;
import com.wayblink.iceberg.analyzer.AnalysisRequest;
import com.wayblink.iceberg.analyzer.AnalysisResult;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RendererTest {

  @Test
  void jsonRendererOmitsNullFieldsAndNormalizesPathValues() {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("table", "sample");
    row.put("deletionVectorCount", null);
    row.put("metadataRoot", java.nio.file.Paths.get("/tmp/warehouse/table/metadata"));

    AnalysisResult result = new AnalysisResult(
        "table",
        "sample",
        2,
        new AnalysisRequest(AnalysisGroupBy.TABLE, AnalysisPrecision.AUTO),
        Collections.singletonList(row));

    String json = new JsonRenderer().render(result);

    assertTrue(!json.contains("\"deletionVectorCount\":null"));
    assertTrue(json.contains("\"metadataRoot\":\"/tmp/warehouse/table/metadata\""));
  }

  @Test
  void tableRendererUsesSummaryCardForSingleTableResults() {
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("tableRoot", "/warehouse/db/table");
    row.put("formatVersion", 2);
    row.put("metadataVersionCount", 12);
    row.put("snapshotVersionCount", 8);
    row.put("partitionCount", 16);
    row.put("currentDataFileCount", 26656);
    row.put("currentDeleteFileCount", 0);
    row.put("currentDataBytes", 7328863547909L);
    row.put("currentDeleteBytes", 0L);
    row.put("tableRowCount", 1048576L);
    row.put("deletedRecordCount", 4096L);
    row.put("deletionVectorCount", 12L);

    AnalysisResult result = new AnalysisResult(
        "table",
        "/warehouse/db/table",
        2,
        new AnalysisRequest(AnalysisGroupBy.TABLE, AnalysisPrecision.AUTO),
        Collections.singletonList(row));

    String rendered = new TableRenderer().render(result);

    assertTrue(rendered.contains("Table Summary"));
    assertTrue(rendered.contains("Data Files"));
    assertTrue(rendered.contains("26,656"));
    assertTrue(rendered.contains("TiB"));
    assertTrue(rendered.contains("Table Rows"));
    assertTrue(rendered.contains("1,048,576"));
    assertTrue(rendered.contains("Deleted Record Rows"));
    assertTrue(rendered.contains("4,096"));
    assertTrue(rendered.contains("Deletion Vector Count"));
  }

  @Test
  void tableRendererUsesGridForMultiRowResults() {
    Map<String, Object> first = new LinkedHashMap<>();
    first.put("snapshotId", 101L);
    first.put("operation", "append");
    first.put("manifestCount", 2);

    Map<String, Object> second = new LinkedHashMap<>();
    second.put("snapshotId", 102L);
    second.put("operation", "overwrite");
    second.put("manifestCount", 3);

    AnalysisResult result = new AnalysisResult(
        "snapshot",
        "/warehouse/db/table",
        2,
        new AnalysisRequest(AnalysisGroupBy.SNAPSHOT, AnalysisPrecision.SUMMARY, 102L),
        java.util.Arrays.asList(first, second));

    String rendered = new TableRenderer().render(result);

    assertTrue(rendered.contains("Snapshot"));
    assertTrue(rendered.contains("SNAPSHOT ID"));
    assertTrue(rendered.contains("OPERATION"));
    assertTrue(rendered.contains("MANIFEST COUNT"));
    assertTrue(rendered.contains("overwrite"));
  }
}
