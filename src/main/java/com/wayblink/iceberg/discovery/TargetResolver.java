package com.wayblink.iceberg.discovery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.OptionalInt;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class TargetResolver {

  private static final Pattern METADATA_FILE_PATTERN =
      Pattern.compile("^(?:v)?(\\d+)(?:-[^.]+)?\\.metadata\\.json$");
  private static final String METADATA_DIRECTORY_NAME = "metadata";
  private static final String VERSION_HINT_FILE_NAME = "version-hint.text";

  public ResolvedTarget resolve(Path input) {
    Path normalizedInput = normalizeExisting(input);
    if (Files.isRegularFile(normalizedInput)) {
      return resolveMetadataFile(normalizedInput);
    }

    if (isMetadataDirectory(normalizedInput)) {
      return resolveMetadataDirectory(normalizedInput, normalizedInput);
    }

    Path nestedMetadataDirectory = normalizedInput.resolve(METADATA_DIRECTORY_NAME);
    if (isMetadataDirectory(nestedMetadataDirectory)) {
      return resolveMetadataDirectory(normalizedInput, nestedMetadataDirectory);
    }

    if (looksLikeWarehouseRoot(normalizedInput)) {
      return new ResolvedTarget(normalizedInput, ResolvedTargetType.WAREHOUSE, null, normalizedInput, null);
    }

    throw new IllegalArgumentException("Unsupported Iceberg target: " + normalizedInput);
  }

  Path resolveCurrentMetadataFile(Path metadataDirectory) {
    OptionalInt hintedVersion = readVersionHint(metadataDirectory);
    if (hintedVersion.isPresent()) {
      Path hintedFile = metadataDirectory.resolve("v" + hintedVersion.getAsInt() + ".metadata.json");
      if (Files.isRegularFile(hintedFile)) {
        return hintedFile.toAbsolutePath().normalize();
      }
    }

    try (Stream<Path> stream = Files.list(metadataDirectory)) {
      return stream
          .filter(Files::isRegularFile)
          .filter(this::isMetadataFile)
          .max(Comparator.comparingInt(this::metadataVersion))
          .map(path -> path.toAbsolutePath().normalize())
          .orElseThrow(() -> new IllegalArgumentException("No metadata file found under " + metadataDirectory));
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to list metadata directory " + metadataDirectory, exception);
    }
  }

  private ResolvedTarget resolveMetadataFile(Path metadataFile) {
    if (!isMetadataFile(metadataFile)) {
      throw new IllegalArgumentException("Unsupported metadata file: " + metadataFile);
    }

    Path metadataRoot = metadataFile.getParent();
    Path tableRoot = metadataRoot == null ? null : metadataRoot.getParent();
    return new ResolvedTarget(
        metadataFile,
        ResolvedTargetType.METADATA_FILE,
        tableRoot,
        metadataRoot,
        metadataFile);
  }

  private ResolvedTarget resolveMetadataDirectory(Path inputPath, Path metadataDirectory) {
    Path metadataRoot = metadataDirectory.toAbsolutePath().normalize();
    Path tableRoot = metadataRoot.getParent();
    return new ResolvedTarget(
        inputPath,
        ResolvedTargetType.TABLE_METADATA_DIR,
        tableRoot,
        metadataRoot,
        resolveCurrentMetadataFile(metadataRoot));
  }

  private Path normalizeExisting(Path input) {
    Path normalized = input.toAbsolutePath().normalize();
    if (Files.notExists(normalized)) {
      throw new IllegalArgumentException("Path does not exist: " + normalized);
    }
    return normalized;
  }

  private boolean isMetadataDirectory(Path directory) {
    return Files.isDirectory(directory) && hasMetadataArtifacts(directory);
  }

  private boolean hasMetadataArtifacts(Path directory) {
    if (Files.isRegularFile(directory.resolve(VERSION_HINT_FILE_NAME))) {
      return true;
    }

    try (Stream<Path> stream = Files.list(directory)) {
      return stream.anyMatch(this::isMetadataFile);
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to inspect directory " + directory, exception);
    }
  }

  private boolean looksLikeWarehouseRoot(Path directory) {
    if (!Files.isDirectory(directory)) {
      return false;
    }

    try (Stream<Path> stream = Files.walk(directory, 3)) {
      return stream
          .filter(Files::isDirectory)
          .filter(path -> !path.equals(directory))
          .anyMatch(this::isMetadataDirectory);
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to scan warehouse root " + directory, exception);
    }
  }

  private OptionalInt readVersionHint(Path metadataDirectory) {
    Path versionHint = metadataDirectory.resolve(VERSION_HINT_FILE_NAME);
    if (Files.notExists(versionHint)) {
      return OptionalInt.empty();
    }

    try {
      String raw = Files.readString(versionHint, StandardCharsets.UTF_8).trim();
      return OptionalInt.of(Integer.parseInt(raw));
    } catch (IOException | NumberFormatException exception) {
      return OptionalInt.empty();
    }
  }

  private boolean isMetadataFile(Path candidate) {
    return Files.isRegularFile(candidate)
        && METADATA_FILE_PATTERN.matcher(candidate.getFileName().toString()).matches();
  }

  private int metadataVersion(Path metadataFile) {
    Matcher matcher = METADATA_FILE_PATTERN.matcher(metadataFile.getFileName().toString());
    if (!matcher.matches()) {
      return Integer.MIN_VALUE;
    }
    return Integer.parseInt(matcher.group(1));
  }
}
