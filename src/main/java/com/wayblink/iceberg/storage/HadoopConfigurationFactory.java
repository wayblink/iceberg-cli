package com.wayblink.iceberg.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;

public final class HadoopConfigurationFactory {

  public Configuration create(StorageOptions options) {
    Configuration configuration = new Configuration();
    if (options == null) {
      return configuration;
    }

    if (options.hadoopConfDir() != null && !options.hadoopConfDir().isBlank()) {
      addResourceIfExists(configuration, options.hadoopConfDir(), "core-site.xml");
      addResourceIfExists(configuration, options.hadoopConfDir(), "hdfs-site.xml");
    }

    if (options.s3Endpoint() != null && !options.s3Endpoint().isBlank()) {
      configuration.set("fs.s3a.endpoint", options.s3Endpoint());
    }
    if (options.s3Region() != null && !options.s3Region().isBlank()) {
      configuration.set("fs.s3a.endpoint.region", options.s3Region());
      configuration.set("fs.s3a.aws.region", options.s3Region());
    }
    if (options.s3PathStyle()) {
      configuration.setBoolean("fs.s3a.path.style.access", true);
    }
    if (options.s3CredentialsProvider() != null && !options.s3CredentialsProvider().isBlank()) {
      configuration.set("fs.s3a.aws.credentials.provider", options.s3CredentialsProvider());
    }

    return configuration;
  }

  private static void addResourceIfExists(Configuration configuration, String confDir, String fileName) {
    Path resource = Paths.get(confDir, fileName);
    if (Files.isRegularFile(resource)) {
      configuration.addResource(new org.apache.hadoop.fs.Path(resource.toUri()));
    }
  }
}
