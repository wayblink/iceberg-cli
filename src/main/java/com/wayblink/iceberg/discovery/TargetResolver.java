package com.wayblink.iceberg.discovery;

import com.wayblink.iceberg.storage.StorageBackend;
import com.wayblink.iceberg.storage.StorageExplorer;
import com.wayblink.iceberg.storage.StorageExplorerFactory;
import com.wayblink.iceberg.storage.StorageOptions;
import com.wayblink.iceberg.storage.StoragePaths;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TargetResolver {

  private static final Pattern METADATA_FILE_PATTERN =
      Pattern.compile("^(?:v)?(\\d+)(?:-[^.]+)?\\.metadata\\.json$");
  private static final String METADATA_DIRECTORY_NAME = "metadata";
  private static final String VERSION_HINT_FILE_NAME = "version-hint.text";

  private final StorageExplorerFactory explorerFactory;
  private final StorageExplorer fixedExplorer;

  public TargetResolver() {
    this(new StorageExplorerFactory(), null);
  }

  public TargetResolver(StorageExplorer explorer) {
    this(null, explorer);
  }

  public TargetResolver(StorageExplorerFactory explorerFactory) {
    this(explorerFactory, null);
  }

  private TargetResolver(StorageExplorerFactory explorerFactory, StorageExplorer fixedExplorer) {
    this.explorerFactory = explorerFactory;
    this.fixedExplorer = fixedExplorer;
  }

  public ResolvedTarget resolve(Path input) {
    return resolve(input.toString(), StorageOptions.defaults());
  }

  public ResolvedTarget resolve(String input) {
    return resolve(input, StorageOptions.defaults());
  }

  public ResolvedTarget resolve(String input, StorageOptions options) {
    StorageOptions resolvedOptions = options == null ? StorageOptions.defaults() : options;
    StorageExplorer explorer = explorerFor(input, resolvedOptions);
    StorageBackend backend = resolvedOptions.resolveBackend(input);
    String normalizedInput = normalizeExisting(input, backend, explorer);
    if (explorer.isFile(normalizedInput)) {
      return resolveMetadataFile(normalizedInput, backend, explorer);
    }

    if (isMetadataDirectory(normalizedInput, explorer)) {
      return resolveMetadataDirectory(normalizedInput, normalizedInput, backend, explorer);
    }

    String nestedMetadataDirectory = StoragePaths.resolve(normalizedInput, METADATA_DIRECTORY_NAME, backend);
    if (isMetadataDirectory(nestedMetadataDirectory, explorer)) {
      return resolveMetadataDirectory(normalizedInput, nestedMetadataDirectory, backend, explorer);
    }

    if (looksLikeWarehouseRoot(normalizedInput, explorer)) {
      return new ResolvedTarget(normalizedInput, ResolvedTargetType.WAREHOUSE, null, normalizedInput, null);
    }

    throw new IllegalArgumentException("Unsupported Iceberg target: " + normalizedInput);
  }

  String resolveCurrentMetadataFile(String metadataDirectory, StorageOptions options) {
    StorageOptions resolvedOptions = options == null ? StorageOptions.defaults() : options;
    StorageExplorer explorer = explorerFor(metadataDirectory, resolvedOptions);
    StorageBackend backend = resolvedOptions.resolveBackend(metadataDirectory);
    return resolveCurrentMetadataFile(metadataDirectory, explorer, backend);
  }

  private String resolveCurrentMetadataFile(
      String metadataDirectory, StorageExplorer explorer, StorageBackend backend) {
    OptionalInt hintedVersion = readVersionHint(metadataDirectory, backend, explorer);
    if (hintedVersion.isPresent()) {
      String hintedFile =
          StoragePaths.resolve(metadataDirectory, "v" + hintedVersion.getAsInt() + ".metadata.json", backend);
      if (explorer.isFile(hintedFile)) {
        return explorer.normalize(hintedFile);
      }
    }

    return explorer.list(metadataDirectory).stream()
        .filter(explorer::isFile)
        .filter(this::isMetadataFile)
        .max(Comparator.comparingInt(this::metadataVersion))
        .map(explorer::normalize)
        .orElseThrow(() -> new IllegalArgumentException("No metadata file found under " + metadataDirectory));
  }

  public StorageExplorer explorerFor(String path, StorageOptions options) {
    if (fixedExplorer != null) {
      return fixedExplorer;
    }
    return explorerFactory.create(path, options);
  }

  private ResolvedTarget resolveMetadataFile(String metadataFile, StorageBackend backend, StorageExplorer explorer) {
    if (!isMetadataFile(metadataFile)) {
      throw new IllegalArgumentException("Unsupported metadata file: " + metadataFile);
    }

    String metadataRoot = StoragePaths.parent(metadataFile, backend);
    String tableRoot = metadataRoot == null ? null : StoragePaths.parent(metadataRoot, backend);
    return new ResolvedTarget(metadataFile, ResolvedTargetType.METADATA_FILE, tableRoot, metadataRoot, metadataFile);
  }

  private ResolvedTarget resolveMetadataDirectory(
      String inputPath, String metadataDirectory, StorageBackend backend, StorageExplorer explorer) {
    String metadataRoot = explorer.normalize(metadataDirectory);
    String tableRoot = StoragePaths.parent(metadataRoot, backend);
    return new ResolvedTarget(
        inputPath,
        ResolvedTargetType.TABLE_METADATA_DIR,
        tableRoot,
        metadataRoot,
        resolveCurrentMetadataFile(metadataRoot, explorer, backend));
  }

  private String normalizeExisting(String input, StorageBackend backend, StorageExplorer explorer) {
    String normalized = explorer.normalize(input);
    if (!explorer.exists(normalized)) {
      throw new IllegalArgumentException("Path does not exist: " + normalized);
    }
    return normalized;
  }

  private boolean isMetadataDirectory(String directory, StorageExplorer explorer) {
    return explorer.isDirectory(directory) && hasMetadataArtifacts(directory, explorer);
  }

  private boolean hasMetadataArtifacts(String directory, StorageExplorer explorer) {
    String versionHint = StoragePaths.resolve(directory, VERSION_HINT_FILE_NAME, backendFor(directory));
    if (explorer.isFile(versionHint)) {
      return true;
    }
    return explorer.list(directory).stream().anyMatch(this::isMetadataFile);
  }

  private boolean looksLikeWarehouseRoot(String directory, StorageExplorer explorer) {
    if (!explorer.isDirectory(directory)) {
      return false;
    }
    return explorer.walk(directory, 3).stream()
        .filter(path -> !directory.equals(path))
        .filter(explorer::isDirectory)
        .anyMatch(path -> isMetadataDirectory(path, explorer));
  }

  private OptionalInt readVersionHint(String metadataDirectory, StorageBackend backend, StorageExplorer explorer) {
    String versionHint = StoragePaths.resolve(metadataDirectory, VERSION_HINT_FILE_NAME, backend);
    if (!explorer.exists(versionHint)) {
      return OptionalInt.empty();
    }

    try {
      String raw = explorer.readText(versionHint).trim();
      return OptionalInt.of(Integer.parseInt(raw));
    } catch (RuntimeException exception) {
      return OptionalInt.empty();
    }
  }

  private boolean isMetadataFile(String candidate) {
    return METADATA_FILE_PATTERN.matcher(StoragePaths.fileName(candidate, backendFor(candidate))).matches();
  }

  private int metadataVersion(String metadataFile) {
    Matcher matcher = METADATA_FILE_PATTERN.matcher(StoragePaths.fileName(metadataFile, backendFor(metadataFile)));
    if (!matcher.matches()) {
      return Integer.MIN_VALUE;
    }
    return Integer.parseInt(matcher.group(1));
  }

  private StorageBackend backendFor(String path) {
    String scheme = StoragePaths.scheme(path);
    if (scheme == null || "file".equalsIgnoreCase(scheme)) {
      return StorageBackend.LOCAL;
    }
    if ("hdfs".equalsIgnoreCase(scheme)) {
      return StorageBackend.HDFS;
    }
    return StorageBackend.S3A;
  }
}
