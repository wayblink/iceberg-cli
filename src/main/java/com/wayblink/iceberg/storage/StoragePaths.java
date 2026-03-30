package com.wayblink.iceberg.storage;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;

public final class StoragePaths {

  private StoragePaths() {
  }

  public static String scheme(String path) {
    if (path == null) {
      return null;
    }
    int index = path.indexOf("://");
    if (index <= 0) {
      return null;
    }
    return path.substring(0, index);
  }

  public static boolean isLocal(String path, StorageBackend backend) {
    return backend == StorageBackend.LOCAL || scheme(path) == null || "file".equalsIgnoreCase(scheme(path));
  }

  public static String normalize(String path, StorageBackend backend) {
    if (isLocal(path, backend)) {
      return Paths.get(path).toAbsolutePath().normalize().toString();
    }
    URI uri = parseUri(path);
    String normalizedPath = normalizeRemotePath(uri.getPath());
    return buildUri(uri.getScheme(), uri.getAuthority(), normalizedPath);
  }

  public static String resolve(String base, String child, StorageBackend backend) {
    if (scheme(child) != null) {
      return normalize(child, backend);
    }
    if (isLocal(base, backend)) {
      return Paths.get(base).resolve(child).normalize().toString();
    }
    URI baseUri = parseUri(base);
    String basePath = normalizeRemotePath(baseUri.getPath());
    if (!basePath.endsWith("/")) {
      basePath = basePath + "/";
    }
    String childPath = child.startsWith("/") ? child.substring(1) : child;
    return buildUri(baseUri.getScheme(), baseUri.getAuthority(), normalizeRemotePath(basePath + childPath));
  }

  public static String parent(String path, StorageBackend backend) {
    if (isLocal(path, backend)) {
      Path parent = Paths.get(path).toAbsolutePath().normalize().getParent();
      return parent == null ? null : parent.toString();
    }
    URI uri = parseUri(path);
    String currentPath = normalizeRemotePath(uri.getPath());
    if (currentPath == null || currentPath.isEmpty() || "/".equals(currentPath)) {
      return null;
    }
    int slash = currentPath.lastIndexOf('/');
    if (slash <= 0) {
      return buildUri(uri.getScheme(), uri.getAuthority(), null);
    }
    return buildUri(uri.getScheme(), uri.getAuthority(), currentPath.substring(0, slash));
  }

  public static String fileName(String path, StorageBackend backend) {
    if (isLocal(path, backend)) {
      Path fileName = Paths.get(path).getFileName();
      return fileName == null ? "" : fileName.toString();
    }
    URI uri = parseUri(path);
    String currentPath = normalizeRemotePath(uri.getPath());
    if (currentPath == null || currentPath.isEmpty() || "/".equals(currentPath)) {
      return "";
    }
    int slash = currentPath.lastIndexOf('/');
    return slash < 0 ? currentPath : currentPath.substring(slash + 1);
  }

  public static String relativize(String root, String child, StorageBackend backend) {
    if (isLocal(root, backend)) {
      return Paths.get(root).toAbsolutePath().normalize().relativize(Paths.get(child).toAbsolutePath().normalize()).toString();
    }
    URI rootUri = parseUri(root);
    URI childUri = parseUri(child);
    if (!equalsNullable(rootUri.getScheme(), childUri.getScheme())
        || !equalsNullable(rootUri.getAuthority(), childUri.getAuthority())) {
      return child;
    }
    String rootPath = normalizeRemotePath(rootUri.getPath());
    String childPath = normalizeRemotePath(childUri.getPath());
    if (rootPath == null || rootPath.isEmpty() || "/".equals(rootPath)) {
      return stripLeadingSlash(childPath);
    }
    String prefix = rootPath.endsWith("/") ? rootPath : rootPath + "/";
    if (!childPath.startsWith(prefix)) {
      return child;
    }
    return childPath.substring(prefix.length());
  }

  private static boolean equalsNullable(String left, String right) {
    return left == null ? right == null : left.equals(right);
  }

  private static String stripLeadingSlash(String value) {
    if (value == null) {
      return null;
    }
    return value.startsWith("/") ? value.substring(1) : value;
  }

  private static URI parseUri(String path) {
    try {
      return new URI(path);
    } catch (URISyntaxException exception) {
      throw new IllegalArgumentException("Invalid storage URI: " + path, exception);
    }
  }

  private static String buildUri(String scheme, String authority, String path) {
    StringBuilder builder = new StringBuilder();
    builder.append(scheme).append("://").append(authority);
    if (path != null && !path.isEmpty() && !"/".equals(path)) {
      if (!path.startsWith("/")) {
        builder.append('/');
      }
      builder.append(path);
    }
    return builder.toString();
  }

  private static String normalizeRemotePath(String rawPath) {
    if (rawPath == null || rawPath.isBlank()) {
      return null;
    }
    String normalized = Paths.get(rawPath).normalize().toString().replace('\\', '/');
    if (normalized.isEmpty() || ".".equals(normalized)) {
      return null;
    }
    if (!normalized.startsWith("/")) {
      normalized = "/" + normalized;
    }
    if (normalized.length() > 1 && normalized.endsWith("/")) {
      normalized = normalized.substring(0, normalized.length() - 1);
    }
    return normalized;
  }
}
