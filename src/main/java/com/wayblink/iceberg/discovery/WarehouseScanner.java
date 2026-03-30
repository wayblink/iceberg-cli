package com.wayblink.iceberg.discovery;

import com.wayblink.iceberg.storage.StorageExplorer;
import com.wayblink.iceberg.storage.StorageOptions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class WarehouseScanner {

  private final TargetResolver targetResolver;

  public WarehouseScanner(TargetResolver targetResolver) {
    this.targetResolver = Objects.requireNonNull(targetResolver, "targetResolver");
  }

  public List<ResolvedTarget> scan(String warehouseRoot, StorageOptions options) {
    StorageOptions resolvedOptions = options == null ? StorageOptions.defaults() : options;
    StorageExplorer explorer = targetResolver.explorerFor(warehouseRoot, resolvedOptions);
    String normalizedRoot = explorer.normalize(warehouseRoot);
    Map<String, ResolvedTarget> tablesByRoot = new LinkedHashMap<>();
    for (String candidate : explorer.walk(normalizedRoot, 3)) {
      if (explorer.isDirectory(candidate)) {
        tryResolveTable(candidate, resolvedOptions, tablesByRoot);
      }
    }

    List<ResolvedTarget> targets = new ArrayList<>(tablesByRoot.values());
    targets.sort(Comparator.comparing(ResolvedTarget::tableRoot));
    return targets;
  }

  public List<ResolvedTarget> scan(java.nio.file.Path warehouseRoot) {
    return scan(warehouseRoot.toString(), StorageOptions.defaults());
  }

  private void tryResolveTable(String candidate, StorageOptions options, Map<String, ResolvedTarget> tablesByRoot) {
    try {
      ResolvedTarget resolvedTarget = targetResolver.resolve(candidate, options);
      if (resolvedTarget.type() == ResolvedTargetType.TABLE_METADATA_DIR && resolvedTarget.tableRoot() != null) {
        tablesByRoot.putIfAbsent(resolvedTarget.tableRoot(), resolvedTarget);
      }
    } catch (IllegalArgumentException ignored) {
      // Ignore directories that are not valid Iceberg table targets while scanning a warehouse.
    }
  }
}
