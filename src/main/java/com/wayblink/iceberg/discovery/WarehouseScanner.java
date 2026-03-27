package com.wayblink.iceberg.discovery;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

public final class WarehouseScanner {

  private final TargetResolver targetResolver;

  public WarehouseScanner(TargetResolver targetResolver) {
    this.targetResolver = Objects.requireNonNull(targetResolver, "targetResolver");
  }

  public List<ResolvedTarget> scan(Path warehouseRoot) {
    Path normalizedRoot = warehouseRoot.toAbsolutePath().normalize();
    Map<Path, ResolvedTarget> tablesByRoot = new LinkedHashMap<>();

    try (Stream<Path> stream = Files.walk(normalizedRoot, 3)) {
      stream
          .filter(Files::isDirectory)
          .sorted()
          .forEach(path -> tryResolveTable(path, tablesByRoot));
    } catch (IOException exception) {
      throw new IllegalArgumentException("Failed to scan warehouse root " + normalizedRoot, exception);
    }

    List<ResolvedTarget> targets = new ArrayList<>(tablesByRoot.values());
    targets.sort(Comparator.comparing(entry -> entry.tableRoot().toString()));
    return targets;
  }

  private void tryResolveTable(Path candidate, Map<Path, ResolvedTarget> tablesByRoot) {
    try {
      ResolvedTarget resolvedTarget = targetResolver.resolve(candidate);
      if (resolvedTarget.type() == ResolvedTargetType.TABLE_METADATA_DIR && resolvedTarget.tableRoot() != null) {
        tablesByRoot.putIfAbsent(resolvedTarget.tableRoot(), resolvedTarget);
      }
    } catch (IllegalArgumentException ignored) {
      // Ignore directories that are not valid Iceberg table targets while scanning a warehouse.
    }
  }
}
