package com.wayblink.iceberg.analyzer;

import com.wayblink.iceberg.loader.TableContext;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;

public final class ManifestTraversalService {

  public FileMetrics summarizeDataFiles(TableContext tableContext, Snapshot snapshot) {
    return summarizeDataFiles(tableContext, snapshot.dataManifests(tableContext.fileIO()));
  }

  public FileMetrics summarizeDeleteFiles(TableContext tableContext, Snapshot snapshot) {
    return summarizeDeleteFiles(tableContext, snapshot.deleteManifests(tableContext.fileIO()));
  }

  public Map<String, PartitionMetrics> summarizeDataPartitions(TableContext tableContext, Snapshot snapshot) {
    return summarizeDataPartitions(tableContext, snapshot.dataManifests(tableContext.fileIO()));
  }

  public Map<String, PartitionMetrics> summarizeDeletePartitions(TableContext tableContext, Snapshot snapshot) {
    return summarizeDeletePartitions(tableContext, snapshot.deleteManifests(tableContext.fileIO()));
  }

  public Map<String, ContentFile<?>> uniqueDataFiles(TableContext tableContext, Snapshot snapshot) {
    return uniqueDataFiles(tableContext, snapshot.dataManifests(tableContext.fileIO()));
  }

  public Map<String, ContentFile<?>> uniqueDeleteFiles(TableContext tableContext, Snapshot snapshot) {
    return uniqueDeleteFiles(tableContext, snapshot.deleteManifests(tableContext.fileIO()));
  }

  public FileMetrics summarizeFiles(Map<String, ? extends ContentFile<?>> filesByLocation) {
    FileMetrics metrics = new FileMetrics();
    filesByLocation.values().forEach(metrics::add);
    return metrics;
  }

  private FileMetrics summarizeDataFiles(TableContext tableContext, List<ManifestFile> manifests) {
    FileMetrics metrics = new FileMetrics();
    for (ManifestFile manifest : manifests) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, tableContext.fileIO(), tableContext.metadata().specsById())) {
        for (DataFile file : reader) {
          metrics.add(file);
        }
      } catch (IOException exception) {
        throw new UncheckedIOException("Failed to read data manifest " + manifest.path(), exception);
      }
    }
    return metrics;
  }

  private FileMetrics summarizeDeleteFiles(TableContext tableContext, List<ManifestFile> manifests) {
    FileMetrics metrics = new FileMetrics();
    for (ManifestFile manifest : manifests) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, tableContext.fileIO(), tableContext.metadata().specsById())) {
        for (DeleteFile file : reader) {
          metrics.add(file);
        }
      } catch (IOException exception) {
        throw new UncheckedIOException("Failed to read delete manifest " + manifest.path(), exception);
      }
    }
    return metrics;
  }

  private Map<String, PartitionMetrics> summarizeDataPartitions(TableContext tableContext, List<ManifestFile> manifests) {
    Map<String, PartitionMetrics> partitions = new LinkedHashMap<>();
    for (ManifestFile manifest : manifests) {
      try (ManifestReader<DataFile> reader =
          ManifestFiles.read(manifest, tableContext.fileIO(), tableContext.metadata().specsById())) {
        for (DataFile file : reader) {
          String key = partitionKey(tableContext, file);
          partitions.computeIfAbsent(key, ignored -> new PartitionMetrics()).addDataFile(file);
        }
      } catch (IOException exception) {
        throw new UncheckedIOException("Failed to read data manifest " + manifest.path(), exception);
      }
    }
    return partitions;
  }

  private Map<String, PartitionMetrics> summarizeDeletePartitions(
      TableContext tableContext,
      List<ManifestFile> manifests) {
    Map<String, PartitionMetrics> partitions = new LinkedHashMap<>();
    for (ManifestFile manifest : manifests) {
      try (ManifestReader<DeleteFile> reader =
          ManifestFiles.readDeleteManifest(manifest, tableContext.fileIO(), tableContext.metadata().specsById())) {
        for (DeleteFile file : reader) {
          String key = partitionKey(tableContext, file);
          partitions.computeIfAbsent(key, ignored -> new PartitionMetrics()).addDeleteFile(file);
        }
      } catch (IOException exception) {
        throw new UncheckedIOException("Failed to read delete manifest " + manifest.path(), exception);
      }
    }
    return partitions;
  }

  private Map<String, ContentFile<?>> uniqueDataFiles(TableContext tableContext, List<ManifestFile> manifests) {
    return collectUniqueFiles(tableContext, manifests, manifest ->
        ManifestFiles.read(manifest, tableContext.fileIO(), tableContext.metadata().specsById()));
  }

  private Map<String, ContentFile<?>> uniqueDeleteFiles(TableContext tableContext, List<ManifestFile> manifests) {
    return collectUniqueFiles(tableContext, manifests, manifest ->
        ManifestFiles.readDeleteManifest(manifest, tableContext.fileIO(), tableContext.metadata().specsById()));
  }

  private <F extends ContentFile<F>> Map<String, ContentFile<?>> collectUniqueFiles(
      TableContext tableContext,
      List<ManifestFile> manifests,
      Function<ManifestFile, ManifestReader<F>> readerFactory) {
    Map<String, ContentFile<?>> filesByLocation = new LinkedHashMap<>();
    for (ManifestFile manifest : manifests) {
      try (ManifestReader<F> reader = readerFactory.apply(manifest)) {
        for (F file : reader) {
          filesByLocation.putIfAbsent(file.location(), file);
        }
      } catch (IOException exception) {
        throw new UncheckedIOException("Failed to read manifest " + manifest.path(), exception);
      }
    }
    return filesByLocation;
  }

  private String partitionKey(TableContext tableContext, ContentFile<?> file) {
    PartitionSpec partitionSpec = tableContext.metadata().specsById().get(file.specId());
    return partitionSpec.partitionToPath(file.partition());
  }

  public static final class FileMetrics {
    private long dataFileCount;
    private long deleteFileCount;
    private long positionDeleteFileCount;
    private long equalityDeleteFileCount;
    private long deletionVectorCount;
    private long dataBytes;
    private long deleteBytes;
    private long dataRecordCount;
    private long deleteRecordCount;

    void add(ContentFile<?> file) {
      if (file.content() == FileContent.DATA) {
        dataFileCount += 1;
        dataBytes += file.fileSizeInBytes();
        dataRecordCount += file.recordCount();
      } else {
        deleteFileCount += 1;
        deleteBytes += file.fileSizeInBytes();
        deleteRecordCount += file.recordCount();
        if (file.content() == FileContent.POSITION_DELETES) {
          positionDeleteFileCount += 1;
          if (isDeletionVector(file)) {
            deletionVectorCount += 1;
          }
        } else if (file.content() == FileContent.EQUALITY_DELETES) {
          equalityDeleteFileCount += 1;
        }
      }
    }

    private boolean isDeletionVector(ContentFile<?> file) {
      if (!(file instanceof DeleteFile)) {
        return false;
      }
      DeleteFile deleteFile = (DeleteFile) file;
      return deleteFile.format() == FileFormat.PUFFIN
          && deleteFile.content() == FileContent.POSITION_DELETES
          && deleteFile.referencedDataFile() != null;
    }

    public long dataFileCount() {
      return dataFileCount;
    }

    public long deleteFileCount() {
      return deleteFileCount;
    }

    public long positionDeleteFileCount() {
      return positionDeleteFileCount;
    }

    public long equalityDeleteFileCount() {
      return equalityDeleteFileCount;
    }

    public long deletionVectorCount() {
      return deletionVectorCount;
    }

    public long dataBytes() {
      return dataBytes;
    }

    public long deleteBytes() {
      return deleteBytes;
    }

    public long dataRecordCount() {
      return dataRecordCount;
    }

    public long deleteRecordCount() {
      return deleteRecordCount;
    }
  }

  public static final class PartitionMetrics {
    private long dataFileCount;
    private long dataBytes;
    private long dataRecordCount;
    private long positionDeleteFileCount;
    private long equalityDeleteFileCount;
    private long deletionVectorCount;
    private long deleteRecordCount;
    private long deleteBytes;

    void addDataFile(ContentFile<?> file) {
      dataFileCount += 1;
      dataBytes += file.fileSizeInBytes();
      dataRecordCount += file.recordCount();
    }

    void addDeleteFile(ContentFile<?> file) {
      deleteBytes += file.fileSizeInBytes();
      deleteRecordCount += file.recordCount();
      if (file.content() == FileContent.POSITION_DELETES) {
        positionDeleteFileCount += 1;
        if (isDeletionVector(file)) {
          deletionVectorCount += 1;
        }
      } else if (file.content() == FileContent.EQUALITY_DELETES) {
        equalityDeleteFileCount += 1;
      }
    }

    private boolean isDeletionVector(ContentFile<?> file) {
      if (!(file instanceof DeleteFile)) {
        return false;
      }
      DeleteFile deleteFile = (DeleteFile) file;
      return deleteFile.format() == FileFormat.PUFFIN
          && deleteFile.content() == FileContent.POSITION_DELETES
          && deleteFile.referencedDataFile() != null;
    }

    void merge(PartitionMetrics other) {
      dataFileCount += other.dataFileCount;
      dataBytes += other.dataBytes;
      dataRecordCount += other.dataRecordCount;
      positionDeleteFileCount += other.positionDeleteFileCount;
      equalityDeleteFileCount += other.equalityDeleteFileCount;
      deletionVectorCount += other.deletionVectorCount;
      deleteRecordCount += other.deleteRecordCount;
      deleteBytes += other.deleteBytes;
    }

    public long dataFileCount() {
      return dataFileCount;
    }

    public long dataBytes() {
      return dataBytes;
    }

    public long dataRecordCount() {
      return dataRecordCount;
    }

    public long positionDeleteFileCount() {
      return positionDeleteFileCount;
    }

    public long equalityDeleteFileCount() {
      return equalityDeleteFileCount;
    }

    public long deletionVectorCount() {
      return deletionVectorCount;
    }

    public long deleteRecordCount() {
      return deleteRecordCount;
    }

    public long deleteBytes() {
      return deleteBytes;
    }
  }
}
