package com.wayblink.iceberg.testsupport;

import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StoragePaths;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public final class HdfsTestCluster implements AutoCloseable {

  private final MiniDFSCluster cluster;
  private final Configuration configuration;
  private final String previousBaseDir;
  private final String warehouseRoot;

  private HdfsTestCluster(
      MiniDFSCluster cluster,
      Configuration configuration,
      String previousBaseDir,
      String warehouseRoot) {
    this.cluster = cluster;
    this.configuration = configuration;
    this.previousBaseDir = previousBaseDir;
    this.warehouseRoot = warehouseRoot;
  }

  public static HdfsTestCluster start(Path baseDir) throws IOException {
    Files.createDirectories(baseDir);
    String previousBaseDir = System.getProperty(MiniDFSCluster.HDFS_MINIDFS_BASEDIR);
    System.setProperty(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toAbsolutePath().toString());

    Configuration configuration = new HdfsConfiguration();
    configuration.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.toAbsolutePath().toString());
    MiniDFSCluster cluster = new MiniDFSCluster.Builder(configuration)
        .numDataNodes(1)
        .format(true)
        .build();
    cluster.waitClusterUp();
    String warehouseRoot = StoragePaths.resolve(
        cluster.getFileSystem().getUri().toString(),
        "warehouse",
        StorageBackend.HDFS);
    return new HdfsTestCluster(cluster, cluster.getConfiguration(0), previousBaseDir, warehouseRoot);
  }

  public Configuration configuration() {
    return configuration;
  }

  public String warehouseRoot() {
    return warehouseRoot;
  }

  public Path writeConfigurationDirectory(Path directory) throws IOException {
    Files.createDirectories(directory);
    writeXml(directory.resolve("core-site.xml"));
    writeXml(directory.resolve("hdfs-site.xml"));
    return directory;
  }

  @Override
  public void close() throws IOException {
    try {
      cluster.shutdown();
    } finally {
      if (previousBaseDir == null) {
        System.clearProperty(MiniDFSCluster.HDFS_MINIDFS_BASEDIR);
      } else {
        System.setProperty(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, previousBaseDir);
      }
    }
  }

  private void writeXml(Path file) throws IOException {
    try (OutputStream output = Files.newOutputStream(file)) {
      configuration.writeXml(output);
    }
  }
}
