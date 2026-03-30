package com.wayblink.iceberg.storage;

import java.util.List;

public interface StorageExplorer {

  String normalize(String path);

  boolean exists(String path);

  boolean isFile(String path);

  boolean isDirectory(String path);

  List<String> list(String directory);

  List<String> walk(String root, int maxDepth);

  String readText(String path);
}
