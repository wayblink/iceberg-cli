package com.wayblink.iceberg.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public final class HadoopStorageExplorer implements StorageExplorer {

  private final Configuration configuration;
  private final StorageBackend backend;

  public HadoopStorageExplorer(Configuration configuration, StorageBackend backend) {
    this.configuration = configuration;
    this.backend = backend;
  }

  @Override
  public String normalize(String path) {
    return StoragePaths.normalize(path, backend);
  }

  @Override
  public boolean exists(String path) {
    try {
      return fileSystem(path).exists(new Path(normalize(path)));
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to access path " + path, exception);
    }
  }

  @Override
  public boolean isFile(String path) {
    try {
      return fileSystem(path).isFile(new Path(normalize(path)));
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to inspect file " + path, exception);
    }
  }

  @Override
  public boolean isDirectory(String path) {
    try {
      return fileSystem(path).getFileStatus(new Path(normalize(path))).isDirectory();
    } catch (IOException exception) {
      return false;
    }
  }

  @Override
  public List<String> list(String directory) {
    try {
      FileStatus[] statuses = fileSystem(directory).listStatus(new Path(normalize(directory)));
      List<String> children = new ArrayList<>(statuses.length);
      for (FileStatus status : statuses) {
        children.add(status.getPath().toString());
      }
      children.sort(Comparator.naturalOrder());
      return children;
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to list directory " + directory, exception);
    }
  }

  @Override
  public List<String> walk(String root, int maxDepth) {
    List<String> results = new ArrayList<>();
    walk(root, 0, maxDepth, results);
    results.sort(Comparator.naturalOrder());
    return results;
  }

  @Override
  public String readText(String path) {
    Path target = new Path(normalize(path));
    try (org.apache.hadoop.fs.FSDataInputStream input = fileSystem(path).open(target)) {
      byte[] bytes = input.readAllBytes();
      return new String(bytes, StandardCharsets.UTF_8);
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to read file " + path, exception);
    }
  }

  private void walk(String root, int depth, int maxDepth, List<String> results) {
    String normalizedRoot = normalize(root);
    results.add(normalizedRoot);
    if (depth >= maxDepth || !isDirectory(normalizedRoot)) {
      return;
    }
    for (String child : list(normalizedRoot)) {
      walk(child, depth + 1, maxDepth, results);
    }
  }

  private FileSystem fileSystem(String path) throws IOException {
    return new Path(normalize(path)).getFileSystem(configuration);
  }
}
