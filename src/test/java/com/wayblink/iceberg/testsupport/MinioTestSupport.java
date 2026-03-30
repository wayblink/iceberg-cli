package com.wayblink.iceberg.testsupport;

import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StoragePaths;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.S3Configuration;

public final class MinioTestSupport implements AutoCloseable {

  private static final DockerImageName MINIO_IMAGE =
      DockerImageName.parse("minio/minio:RELEASE.2024-01-16T16-07-38Z");
  private static final String ACCESS_KEY = "minioadmin";
  private static final String SECRET_KEY = "minioadmin";
  private static final String REGION = "us-east-1";

  private final GenericContainer<?> container;
  private final String bucketName;

  private MinioTestSupport(GenericContainer<?> container, String bucketName) {
    this.container = container;
    this.bucketName = bucketName;
  }

  public static MinioTestSupport start(String bucketName) {
    GenericContainer<?> container = new GenericContainer<>(MINIO_IMAGE)
        .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
        .withEnv("MINIO_ROOT_PASSWORD", SECRET_KEY)
        .withCommand("server", "/data", "--console-address", ":9001")
        .withExposedPorts(9000, 9001)
        .waitingFor(Wait.forHttp("/minio/health/ready").forPort(9000).forStatusCode(200));
    container.start();
    MinioTestSupport support = new MinioTestSupport(container, bucketName);
    support.createBucket();
    return support;
  }

  public String warehouseRoot() {
    return StoragePaths.resolve("s3a://" + bucketName, "warehouse", StorageBackend.S3A);
  }

  public String endpoint() {
    return "http://" + container.getHost() + ":" + container.getMappedPort(9000);
  }

  public String region() {
    return REGION;
  }

  public String accessKey() {
    return ACCESS_KEY;
  }

  public String secretKey() {
    return SECRET_KEY;
  }

  public Path writeConfigurationDirectory(Path directory) throws IOException {
    Files.createDirectories(directory);
    Configuration configuration = new Configuration(false);
    configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    configuration.set("fs.s3a.access.key", ACCESS_KEY);
    configuration.set("fs.s3a.secret.key", SECRET_KEY);
    configuration.setBoolean("fs.s3a.connection.ssl.enabled", false);
    try (OutputStream output = Files.newOutputStream(directory.resolve("core-site.xml"))) {
      configuration.writeXml(output);
    }
    return directory;
  }

  @Override
  public void close() {
    container.close();
  }

  private void createBucket() {
    AwsBasicCredentials credentials = AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY);
    try (S3Client client = S3Client.builder()
        .endpointOverride(URI.create(endpoint()))
        .credentialsProvider(StaticCredentialsProvider.create(credentials))
        .region(Region.of(REGION))
        .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
        .build()) {
      client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());
    }
  }
}
