package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StoragePaths;
import com.wayblink.iceberg.testsupport.IcebergTableFixtures;
import com.wayblink.iceberg.testsupport.MinioTestSupport;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.DockerClientFactory;
import picocli.CommandLine;

@Testcontainers(disabledWithoutDocker = true)
class S3aCliIT {

  @TempDir
  Path tempDir;

  @Test
  void openShowAndStatWorkForS3aMetadataDirectory() throws Exception {
    Assumptions.assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required for S3A integration tests");
    try (MinioTestSupport minio = MinioTestSupport.start("iceberg-inspect-it")) {
      Path hadoopConfDir = minio.writeConfigurationDirectory(tempDir.resolve("hadoop-conf"));
      Configuration configuration = new Configuration(false);
      configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      configuration.set("fs.s3a.endpoint", minio.endpoint());
      configuration.set("fs.s3a.endpoint.region", minio.region());
      configuration.set("fs.s3a.aws.region", minio.region());
      configuration.setBoolean("fs.s3a.path.style.access", true);
      configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
      configuration.set("fs.s3a.access.key", minio.accessKey());
      configuration.set("fs.s3a.secret.key", minio.secretKey());
      configuration.set(
          "fs.s3a.aws.credentials.provider",
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

      String tableLocation = IcebergTableFixtures.createPartitionedTable(
          configuration,
          minio.warehouseRoot(),
          StorageBackend.S3A);
      String metadataDirectory = StoragePaths.resolve(tableLocation, "metadata", StorageBackend.S3A);

      ByteArrayOutputStream stdout = new ByteArrayOutputStream();
      CommandLine cli = cli(tempDir.resolve("session.json"), stdout);

      String openOutput = execute(cli, stdout,
          "open",
          metadataDirectory,
          "--fs",
          "s3a",
          "--hadoop-conf-dir",
          hadoopConfDir.toString(),
          "--s3-endpoint",
          minio.endpoint(),
          "--s3-region",
          minio.region(),
          "--s3-path-style",
          "--s3-credentials-provider",
          "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
      String showOutput = execute(cli, stdout, "show", "table", "--json");
      String statOutput = execute(cli, stdout, "stat", "table", "--json");

      assertTrue(openOutput.contains("storage-backend: S3A"));
      assertTrue(showOutput.contains("\"formatVersion\":2"));
      assertTrue(showOutput.contains("\"location\":\"s3a://iceberg-inspect-it/warehouse"));
      assertTrue(statOutput.contains("\"currentDataFileCount\":3"));
    }
  }

  private static CommandLine cli(Path sessionFile, ByteArrayOutputStream stdout) throws IOException {
    RootCommand rootCommand = RootCommand.forSessionFile(sessionFile);
    CommandLine commandLine = new CommandLine(rootCommand);
    commandLine.setOut(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    commandLine.setErr(new PrintWriter(stdout, true, StandardCharsets.UTF_8));
    return commandLine;
  }

  private static String execute(CommandLine cli, ByteArrayOutputStream stdout, String... args) {
    stdout.reset();
    assertEquals(0, cli.execute(args));
    return stdout.toString(StandardCharsets.UTF_8);
  }
}
