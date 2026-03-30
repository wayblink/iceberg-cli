package com.wayblink.iceberg.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class LocalStorageExplorer implements StorageExplorer {

  @Override
  public String normalize(String path) {
    return StoragePaths.normalize(path, StorageBackend.LOCAL);
  }

  @Override
  public boolean exists(String path) {
    return Files.exists(Paths.get(normalize(path)));
  }

  @Override
  public boolean isFile(String path) {
    return Files.isRegularFile(Paths.get(normalize(path)));
  }

  @Override
  public boolean isDirectory(String path) {
    return Files.isDirectory(Paths.get(normalize(path)));
  }

  @Override
  public List<String> list(String directory) {
    Path normalizedDirectory = Paths.get(normalize(directory));
    try (Stream<Path> stream = Files.list(normalizedDirectory)) {
      return stream
          .map(path -> path.toAbsolutePath().normalize().toString())
          .sorted()
          .collect(Collectors.toList());
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to list directory " + normalizedDirectory, exception);
    }
  }

  @Override
  public List<String> walk(String root, int maxDepth) {
    Path normalizedRoot = Paths.get(normalize(root));
    try (Stream<Path> stream = Files.walk(normalizedRoot, maxDepth)) {
      return stream
          .map(path -> path.toAbsolutePath().normalize().toString())
          .sorted()
          .collect(Collectors.toList());
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to walk path " + normalizedRoot, exception);
    }
  }

  @Override
  public String readText(String path) {
    try {
      return Files.readString(Paths.get(normalize(path)), StandardCharsets.UTF_8);
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to read file " + path, exception);
    }
  }
}
