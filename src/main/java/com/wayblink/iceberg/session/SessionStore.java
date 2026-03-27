package com.wayblink.iceberg.session;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Optional;

public final class SessionStore {

  private final Path sessionFile;
  private final ObjectMapper objectMapper;

  public SessionStore(Path sessionFile) {
    this.sessionFile = sessionFile.toAbsolutePath().normalize();
    this.objectMapper = new ObjectMapper();
  }

  public Optional<SessionState> load() {
    if (Files.notExists(sessionFile)) {
      return Optional.empty();
    }

    try {
      return Optional.of(objectMapper.readValue(sessionFile.toFile(), SessionState.class));
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to read session file " + sessionFile, exception);
    }
  }

  public void save(SessionState state) {
    try {
      Files.createDirectories(sessionFile.getParent());
      Path tempFile = Files.createTempFile(sessionFile.getParent(), "session", ".json");
      objectMapper.writerWithDefaultPrettyPrinter().writeValue(tempFile.toFile(), state);
      Files.move(
          tempFile,
          sessionFile,
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.ATOMIC_MOVE);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to write session file " + sessionFile, exception);
    }
  }

  public void clear() {
    try {
      Files.deleteIfExists(sessionFile);
    } catch (IOException exception) {
      throw new IllegalStateException("Failed to clear session file " + sessionFile, exception);
    }
  }

  public Path sessionFile() {
    return sessionFile;
  }
}
