package com.wayblink.iceberg.cli;

import com.wayblink.iceberg.discovery.ResolvedTarget;
import com.wayblink.iceberg.discovery.TargetResolver;
import com.wayblink.iceberg.discovery.WarehouseScanner;
import com.wayblink.iceberg.io.LocalFileIOProvider;
import com.wayblink.iceberg.loader.TableContext;
import com.wayblink.iceberg.loader.TableLoader;
import com.wayblink.iceberg.loader.VersionDetector;
import com.wayblink.iceberg.session.SessionResolver;
import com.wayblink.iceberg.session.SessionState;
import com.wayblink.iceberg.session.SessionStore;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import picocli.CommandLine.Command;

@Command(
    name = "iceberg-inspect",
    mixinStandardHelpOptions = true,
    description = {
        "Inspect local Iceberg metadata targets from a local metadata directory, metadata JSON file, or warehouse root.",
        "Use `open` once to establish a current session, then run `show`, `stat`, or `scan` commands without repeating --path."
    },
    descriptionHeading = "%nDescription:%n",
    optionListHeading = "%nOptions:%n",
    commandListHeading = "%nCommands:%n",
    footerHeading = "%nExamples:%n",
    footer = {
        "  iceberg-inspect open /warehouse/db/orders/metadata",
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
    this(defaultSessionStore(), new TargetResolver(), new VersionDetector(), new LocalFileIOProvider());
  }

  RootCommand(
      SessionStore sessionStore,
      TargetResolver targetResolver,
      VersionDetector versionDetector,
      LocalFileIOProvider fileIOProvider) {
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
        new LocalFileIOProvider());
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
    SessionState sessionState = requireCurrentSession();
    if (sessionState.currentMetadataFile() == null) {
      throw new IllegalStateException("Current target is not a table. Use open <table> or use table <name> first.");
    }
    return tableLoader.load(sessionState);
  }

  public SessionState requireCurrentSession() {
    return new SessionResolver(sessionStore).requireCurrent();
  }

  public ResolvedTarget requireCurrentResolvedTarget() {
    SessionState sessionState = requireCurrentSession();
    if (sessionState.currentMetadataFile() != null) {
      return targetResolver.resolve(Paths.get(sessionState.currentMetadataFile()));
    }
    return targetResolver.resolve(Paths.get(sessionState.metadataRoot()));
  }

  public TableContext loadTable(Path path) {
    return loadResolvedTable(targetResolver.resolve(path));
  }

  public TableContext loadResolvedTable(ResolvedTarget target) {
    SessionState sessionState = newSessionState(target, null);
    if (sessionState.currentMetadataFile() == null) {
      throw new IllegalArgumentException("Target is not a table: " + target.inputPath());
    }
    return tableLoader.load(sessionState);
  }

  public Path resolveWarehousePath(Path overridePath) {
    if (overridePath != null) {
      return overridePath.toAbsolutePath().normalize();
    }

    SessionState sessionState = requireCurrentSession();
    if (sessionState.warehouseRoot() != null) {
      return Paths.get(sessionState.warehouseRoot()).toAbsolutePath().normalize();
    }
    if (sessionState.targetType() == com.wayblink.iceberg.discovery.ResolvedTargetType.WAREHOUSE) {
      return Paths.get(sessionState.metadataRoot()).toAbsolutePath().normalize();
    }
    throw new IllegalStateException("No current warehouse. Run open <warehouse-path> first or pass --path.");
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
  }

  public SessionState newSessionState(ResolvedTarget target, String warehouseRoot) {
    Integer formatVersion = target.currentMetadataFile() == null
        ? null
        : versionDetector.detect(target.currentMetadataFile());
    long nowMillis = System.currentTimeMillis();
    return SessionState.fromResolvedTarget(target, formatVersion, nowMillis, warehouseRoot);
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
