package com.wayblink.iceberg.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StoragePaths;
import com.wayblink.iceberg.testsupport.HdfsTestCluster;
import com.wayblink.iceberg.testsupport.IcebergTableFixtures;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;

class HdfsCliIT {

  @TempDir
  Path tempDir;

  @Test
  void openShowAndStatWorkForHdfsMetadataDirectory() throws Exception {
    Assumptions.assumeTrue(
        isClassPresent("org.apache.hadoop.shaded.org.eclipse.jetty.server.ConnectionFactory"),
        "MiniDFSCluster requires Hadoop shaded Jetty test classes on the test classpath");
    try (HdfsTestCluster cluster = HdfsTestCluster.start(tempDir.resolve("mini-dfs"))) {
      Path hadoopConfDir = cluster.writeConfigurationDirectory(tempDir.resolve("hadoop-conf"));
      String tableLocation = IcebergTableFixtures.createPartitionedTable(
          cluster.configuration(),
          cluster.warehouseRoot(),
          StorageBackend.HDFS);
      String metadataDirectory = StoragePaths.resolve(tableLocation, "metadata", StorageBackend.HDFS);

      ByteArrayOutputStream stdout = new ByteArrayOutputStream();
      CommandLine cli = cli(tempDir.resolve("session.json"), stdout);

      String openOutput = execute(cli, stdout,
          "open",
          metadataDirectory,
          "--fs",
          "hdfs",
          "--hadoop-conf-dir",
          hadoopConfDir.toString());
      String showOutput = execute(cli, stdout, "show", "table", "--json");
      String statOutput = execute(cli, stdout, "stat", "table", "--json");

      assertTrue(openOutput.contains("storage-backend: HDFS"));
      assertTrue(showOutput.contains("\"formatVersion\":2"));
      assertTrue(showOutput.contains("\"location\":\"" + cluster.warehouseRoot()));
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

  private static boolean isClassPresent(String className) {
    try {
      Class.forName(className);
      return true;
    } catch (ClassNotFoundException error) {
      return false;
    }
  }
}
