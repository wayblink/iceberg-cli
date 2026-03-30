package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.discovery.TargetResolver;
import com.wayblink.iceberg.discovery.WarehouseScanner;
import com.wayblink.iceberg.io.DefaultFileIOProvider;
import com.wayblink.iceberg.io.FileIOProvider;
import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.loader.TableLoader;
import com.wayblink.iceberg.loader.VersionDetector;
import com.wayblink.iceberg.session.SessionResolver;
import com.wayblink.iceberg.session.SessionState;
import com.wayblink.iceberg.session.SessionStore;
import com.wayblink.iceberg.storage.StorageOptions;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import picocli.CommandLine.Command;

@Command(
    name = "iceberg-inspect",
    mixinStandardHelpOptions = true,
    description = {
        "Inspect Iceberg metadata targets from local paths, HDFS, or S3A.",
        "Use `open` once to establish a current session, then run `show`, `stat`, or `scan` commands without repeating --path."
    },
    descriptionHeading = "%nDescription:%n",
    optionListHeading = "%nOptions:%n",
    commandListHeading = "%nCommands:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect open /warehouse/db/orders/metadata",
        "  iceberg-inspect open hdfs://nameservice1/warehouse/db/orders/metadata --fs hdfs --hadoop-conf-dir /etc/hadoop/conf",
        "  iceberg-inspect open s3a://bucket/warehouse/db/orders/metadata --fs s3a --s3-endpoint http://minio:9000 --s3-region us-east-1 --s3-path-style",
        "  iceberg-inspect stat table --scope all --group-by snapshot",
        "  iceberg-inspect show manifests --snapshot-id 123456789",
        "",
        "  iceberg-inspect open /warehouse",
        "  iceberg-inspect use table db/orders",
        "  iceberg-inspect scan warehouse --format json"
    },
    subcommands = {
        OpenCommand.class,
        CurrentCommand.class,
        CloseCommand.class,
        UseCommand.class,
        ShowCommand.class,
        StatCommand.class,
        ScanCommand.class
    },
    subcommandsRepeatable = false)
public final class RootCommand implements Runnable {

  private final SessionStore sessionStore;
  private final TargetResolver targetResolver;
  private final WarehouseScanner warehouseScanner;
  private final VersionDetector versionDetector;
  private final TableLoader tableLoader;

  public RootCommand() {
    this(defaultSessionStore(), new TargetResolver(), new VersionDetector(), new DefaultFileIOProvider());
  }

  RootCommand(
      SessionStore sessionStore,
      TargetResolver targetResolver,
      VersionDetector versionDetector,
      FileIOProvider fileIOProvider) {
    this.sessionStore = Objects.requireNonNull(sessionStore, "sessionStore");
    this.targetResolver = Objects.requireNonNull(targetResolver, "targetResolver");
    this.versionDetector = Objects.requireNonNull(versionDetector, "versionDetector");
    this.warehouseScanner = new WarehouseScanner(targetResolver);
    this.tableLoader = new TableLoader(fileIOProvider);
  }

  public static RootCommand forSessionFile(Path sessionFile) {
    return new RootCommand(
        new SessionStore(sessionFile),
        new TargetResolver(),
        new VersionDetector(),
        new DefaultFileIOProvider());
  }

  public SessionStore sessionStore() {
    return sessionStore;
  }

  public TargetResolver targetResolver() {
    return targetResolver;
  }

  public WarehouseScanner warehouseScanner() {
    return warehouseScanner;
  }

  public VersionDetector versionDetector() {
    return versionDetector;
  }

  public TableContext requireCurrentTable() {
    return requireCurrentTable(StorageOptions.defaults(), false);
  }

  public TableContext requireCurrentTable(
      StorageOptions overrideOptions, boolean hasExplicitOverrides) {
    SessionState sessionState = requireCurrentSession();
    if (sessionState.currentMetadataFile() == null) {
      throw new IllegalStateException("Current target is not a table. Use open <table> or use table <name> first.");
    }
    StorageOptions storageOptions =
        effectiveStorageOptions(null, overrideOptions, hasExplicitOverrides);
    return loadResolvedTable(requireCurrentResolvedTarget(overrideOptions, hasExplicitOverrides), storageOptions);
  }

  public SessionState requireCurrentSession() {
    return new SessionResolver(sessionStore).requireCurrent();
  }

  public ResolvedTarget requireCurrentResolvedTarget() {
    return requireCurrentResolvedTarget(StorageOptions.defaults(), false);
  }

  public ResolvedTarget requireCurrentResolvedTarget(
      StorageOptions overrideOptions, boolean hasExplicitOverrides) {
    SessionState sessionState = requireCurrentSession();
    StorageOptions storageOptions =
        effectiveStorageOptions(null, overrideOptions, hasExplicitOverrides);
    if (sessionState.currentMetadataFile() != null) {
      return targetResolver.resolve(sessionState.currentMetadataFile(), storageOptions);
    }
    return targetResolver.resolve(sessionState.metadataRoot(), storageOptions);
  }

  public TableContext loadTable(String path, StorageOptions storageOptions) {
    return loadResolvedTable(targetResolver.resolve(path, storageOptions), storageOptions);
  }

  public TableContext loadResolvedTable(ResolvedTarget target) {
    return loadResolvedTable(target, StorageOptions.defaults());
  }

  public TableContext loadResolvedTable(ResolvedTarget target, StorageOptions storageOptions) {
    SessionState sessionState = newSessionState(target, null, storageOptions);
    if (sessionState.currentMetadataFile() == null) {
      throw new IllegalArgumentException("Target is not a table: " + target.inputPath());
    }
    return tableLoader.load(sessionState);
  }

  public String resolveWarehousePath(String overridePath, StorageOptions overrideOptions) {
    if (overridePath != null) {
      return targetResolver.explorerFor(overridePath, overrideOptions).normalize(overridePath);
    }

    SessionState sessionState = requireCurrentSession();
    if (sessionState.warehouseRoot() != null) {
      return sessionState.warehouseRoot();
    }
    if (sessionState.targetType() == com.wayblink.iceberg.discovery.ResolvedTargetType.WAREHOUSE) {
      return sessionState.metadataRoot();
    }
    throw new IllegalStateException("No current warehouse. Run open <warehouse-path> first or pass --path.");
  }

  StorageOptions effectiveStorageOptions(
      String overridePath, StorageOptions overrideOptions, boolean hasExplicitOverrides) {
    StorageOptions resolvedOverride = overrideOptions == null ? StorageOptions.defaults() : overrideOptions;
    if (overridePath != null) {
      return resolvedOverride;
    }

    SessionState sessionState = requireCurrentSession();
    if (hasExplicitOverrides) {
      return resolvedOverride.overlayOn(sessionState.storageOptions());
    }
    return sessionState.storageOptions();
  }

  public void printSessionState(SessionState state, PrintWriter out) {
    out.println("target-path: " + state.inputPath());
    out.println("target-type: " + state.targetType());
    if (state.tableRoot() != null) {
      out.println("table-root: " + state.tableRoot());
    }
    out.println("metadata-root: " + state.metadataRoot());
    if (state.currentMetadataFile() != null) {
      out.println("metadata-file: " + state.currentMetadataFile());
    }
    if (state.formatVersion() != null) {
      out.println("format-version: " + state.formatVersion());
    }
    if (state.warehouseRoot() != null) {
      out.println("warehouse-root: " + state.warehouseRoot());
    }
    out.println("storage-backend: " + state.storageBackend());
    if (state.hadoopConfDir() != null) {
      out.println("hadoop-conf-dir: " + state.hadoopConfDir());
    }
    if (state.s3Endpoint() != null) {
      out.println("s3-endpoint: " + state.s3Endpoint());
    }
    if (state.s3Region() != null) {
      out.println("s3-region: " + state.s3Region());
    }
    if (state.s3PathStyle()) {
      out.println("s3-path-style: true");
    }
    if (state.s3CredentialsProvider() != null) {
      out.println("s3-credentials-provider: " + state.s3CredentialsProvider());
    }
  }

  public SessionState newSessionState(ResolvedTarget target, String warehouseRoot) {
    return newSessionState(target, warehouseRoot, StorageOptions.defaults());
  }

  public SessionState newSessionState(
      ResolvedTarget target, String warehouseRoot, StorageOptions storageOptions) {
    Integer formatVersion = target.currentMetadataFile() == null
        ? null
        : versionDetector.detect(target.currentMetadataFile(), storageOptions);
    long nowMillis = System.currentTimeMillis();
    return SessionState.fromResolvedTarget(target, formatVersion, nowMillis, warehouseRoot, storageOptions);
  }

  private static SessionStore defaultSessionStore() {
    return new SessionStore(
        Paths.get(System.getProperty("user.home"), ".config", "iceberg-inspect", "session.json"));
  }

  @Override
  public void run() {
    // Picocli handles usage output for help requests.
  }
}
