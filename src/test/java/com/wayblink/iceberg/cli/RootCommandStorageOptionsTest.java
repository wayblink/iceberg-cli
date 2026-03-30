package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.discovery.ResolvedTargetType;
import com.wayblink.iceberg.session.SessionStore;
import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StorageOptions;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RootCommandStorageOptionsTest {

  @TempDir
  Path tempDir;

  @Test
  void effectiveStorageOptionsFallsBackToCurrentSessionWhenPathIsOmitted() {
    RootCommand rootCommand = RootCommand.forSessionFile(tempDir.resolve("session.json"));
    StorageOptions sessionOptions = StorageOptions.builder()
        .backend(StorageBackend.HDFS)
        .hadoopConfDir("/etc/hadoop/conf")
        .build();
    saveWarehouseSession(rootCommand, "/warehouse", sessionOptions);

    StorageOptions effective = rootCommand.effectiveStorageOptions(null, StorageOptions.defaults(), false);

    assertEquals(StorageBackend.HDFS, effective.backend());
    assertEquals("/etc/hadoop/conf", effective.hadoopConfDir());
  }

  @Test
  void effectiveStorageOptionsOverlaysExplicitFlagsOnTopOfCurrentSession() {
    RootCommand rootCommand = RootCommand.forSessionFile(tempDir.resolve("overlay-session.json"));
    StorageOptions sessionOptions = StorageOptions.builder()
        .backend(StorageBackend.S3A)
        .s3Endpoint("http://minio:9000")
        .build();
    saveWarehouseSession(rootCommand, "s3a://warehouse", sessionOptions);

    StorageOptions explicitOptions = StorageOptions.builder()
        .s3Region("us-east-1")
        .s3PathStyle(true)
        .build();

    StorageOptions effective = rootCommand.effectiveStorageOptions(null, explicitOptions, true);

    assertEquals(StorageBackend.S3A, effective.backend());
    assertEquals("http://minio:9000", effective.s3Endpoint());
    assertEquals("us-east-1", effective.s3Region());
    assertEquals(true, effective.s3PathStyle());
  }

  @Test
  void effectiveStorageOptionsUsesExplicitPathResolutionWithoutSessionFallback() {
    RootCommand rootCommand = RootCommand.forSessionFile(tempDir.resolve("override-session.json"));
    StorageOptions sessionOptions = StorageOptions.builder()
        .backend(StorageBackend.HDFS)
        .hadoopConfDir("/etc/hadoop/conf")
        .build();
    saveWarehouseSession(rootCommand, "/warehouse", sessionOptions);

    StorageOptions effective = rootCommand.effectiveStorageOptions(
        "s3a://bucket/table/metadata",
        StorageOptions.builder().backend(StorageBackend.S3A).s3Region("us-west-2").build(),
        true);

    assertEquals(StorageBackend.S3A, effective.backend());
    assertEquals("us-west-2", effective.s3Region());
    assertEquals(null, effective.hadoopConfDir());
  }

  private static void saveWarehouseSession(
      RootCommand rootCommand, String inputPath, StorageOptions storageOptions) {
    ResolvedTarget target = new ResolvedTarget(
        inputPath,
        ResolvedTargetType.WAREHOUSE,
        null,
        inputPath,
        null);
    SessionStore sessionStore = rootCommand.sessionStore();
    sessionStore.save(rootCommand.newSessionState(target, inputPath, storageOptions));
  }
}
