package com.wayblink.iceberg.io;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.SeekableInputStream;

public final class LocalMetadataMirrorFileIO implements FileIO {

  private final FileIO delegate;
  private final Path metadataRoot;
  private final String remoteMetadataPrefix;

  public LocalMetadataMirrorFileIO(FileIO delegate, Path metadataRoot, String tableLocation) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
    this.metadataRoot = Objects.requireNonNull(metadataRoot, "metadataRoot").toAbsolutePath().normalize();
    this.remoteMetadataPrefix = normalizeRemoteMetadataPrefix(tableLocation);
  }

  @Override
  public InputFile newInputFile(String location) {
    String resolvedLocation = mirrorMetadataLocation(location);
    InputFile delegateInput = delegate.newInputFile(resolvedLocation);
    if (resolvedLocation.equals(location)) {
      return delegateInput;
    }
    return new RemappedInputFile(location, delegateInput);
  }

  @Override
  public OutputFile newOutputFile(String location) {
    return delegate.newOutputFile(mirrorMetadataLocation(location));
  }

  @Override
  public void deleteFile(String location) {
    delegate.deleteFile(mirrorMetadataLocation(location));
  }

  @Override
  public Map<String, String> properties() {
    return delegate.properties();
  }

  @Override
  public void initialize(Map<String, String> properties) {
    delegate.initialize(properties);
  }

  @Override
  public void close() {
    delegate.close();
  }

  private String mirrorMetadataLocation(String location) {
    if (location == null || location.isBlank()) {
      return location;
    }

    if (!shouldMirror(location)) {
      return location;
    }

    String fileName = fileNameOf(location);
    if (fileName.isEmpty()) {
      return location;
    }

    Path mirroredPath = metadataRoot.resolve(fileName).normalize();
    if (Files.isRegularFile(mirroredPath)) {
      return mirroredPath.toString();
    }

    return location;
  }

  private boolean shouldMirror(String location) {
    if (remoteMetadataPrefix != null && location.startsWith(remoteMetadataPrefix)) {
      return true;
    }

    return location.contains("/metadata/");
  }

  private static String normalizeRemoteMetadataPrefix(String tableLocation) {
    if (tableLocation == null || tableLocation.isBlank()) {
      return null;
    }

    String normalized = tableLocation.endsWith("/") ? tableLocation.substring(0, tableLocation.length() - 1) : tableLocation;
    return normalized + "/metadata/";
  }

  private static String fileNameOf(String location) {
    int slashIndex = location.lastIndexOf('/');
    if (slashIndex < 0 || slashIndex == location.length() - 1) {
      return slashIndex < 0 ? location : "";
    }
    return location.substring(slashIndex + 1);
  }

  private static final class RemappedInputFile implements InputFile {

    private final String originalLocation;
    private final InputFile delegate;

    private RemappedInputFile(String originalLocation, InputFile delegate) {
      this.originalLocation = originalLocation;
      this.delegate = delegate;
    }

    @Override
    public long getLength() {
      return delegate.getLength();
    }

    @Override
    public SeekableInputStream newStream() {
      return delegate.newStream();
    }

    @Override
    public String location() {
      return originalLocation;
    }

    @Override
    public boolean exists() {
      return delegate.exists();
    }
  }
}
