package com.wayblink.iceberg.analyzer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class ManifestTraversalServiceTest {

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.optional(2, "category", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).identity("category").build();

  @Test
  void fileMetricsCountsDeletionVectorsSeparately() {
    ManifestTraversalService.FileMetrics metrics = new ManifestTraversalService.FileMetrics();

    DeleteFile deletionVector = FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath("/warehouse/table/metadata/delete-dv-0001.puffin")
        .withFormat(FileFormat.PUFFIN)
        .withPartitionPath("category=books")
        .withFileSizeInBytes(2048)
        .withRecordCount(64)
        .withReferencedDataFile("/warehouse/table/data/data-0001.parquet")
        .withContentOffset(128L)
        .withContentSizeInBytes(512L)
        .build();
    DeleteFile positionDelete = FileMetadata.deleteFileBuilder(SPEC)
        .ofPositionDeletes()
        .withPath("/warehouse/table/metadata/delete-pos-0001.parquet")
        .withFormat(FileFormat.PARQUET)
        .withPartitionPath("category=books")
        .withFileSizeInBytes(1024)
        .withRecordCount(16)
        .build();

    metrics.add(deletionVector);
    metrics.add(positionDelete);

    assertEquals(2L, metrics.deleteFileCount());
    assertEquals(2L, metrics.positionDeleteFileCount());
    assertEquals(1L, metrics.deletionVectorCount());
    assertEquals(80L, metrics.deleteRecordCount());
    assertEquals(3072L, metrics.deleteBytes());
  }
}
