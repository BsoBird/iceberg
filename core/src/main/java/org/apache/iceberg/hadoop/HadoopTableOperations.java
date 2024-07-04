/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableOperations implementation for file systems.
 *
 * <p>For normal file-system(support atomic non-overwriting rename). Users can use it directly
 * without configuring additional lockManager.
 *
 * <p>For object storage(not support atomic non-overwriting rename), user should choose a suitable
 * lockManager implementation.
 *
 * <p>This maintains metadata in a "metadata" folder under the table location.
 */
public class HadoopTableOperations implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopTableOperations.class);
  private static final Pattern VERSION_PATTERN = Pattern.compile("v([^\\.]*)\\..*");
  private static final TableMetadataParser.Codec[] TABLE_METADATA_PARSER_CODEC_VALUES =
      TableMetadataParser.Codec.values();

  private final Configuration conf;
  private final Path location;
  private final FileIO fileIO;
  private final LockManager lockManager;

  private volatile TableMetadata currentMetadata = null;
  private volatile Integer version = null;
  private volatile boolean shouldRefresh = true;

  protected HadoopTableOperations(
      Path location, FileIO fileIO, Configuration conf, LockManager lockManager) {
    this.conf = conf;
    this.location = location;
    this.fileIO = fileIO;
    this.lockManager = lockManager;
  }

  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  private synchronized Pair<Integer, TableMetadata> versionAndMetadata() {
    return Pair.of(version, currentMetadata);
  }

  private synchronized void updateVersionAndMetadata(int newVersion, String metadataFile) {
    // update if the current version is out of date
    if (version == null || version != newVersion) {
      this.version = newVersion;
      this.currentMetadata =
          checkUUID(currentMetadata, TableMetadataParser.read(io(), metadataFile));
    }
  }

  @Override
  public TableMetadata refresh() {
    int ver = version != null ? version : findVersion();
    try {
      Path metadataFile = getMetadataFile(ver);
      if (version == null && metadataFile == null && ver == 0) {
        // no v0 metadata means the table doesn't exist yet
        return null;
      } else if (metadataFile == null) {
        throw new ValidationException("Metadata file for version %d is missing", ver);
      }

      Path nextMetadataFile = getMetadataFile(ver + 1);
      while (nextMetadataFile != null) {
        ver += 1;
        metadataFile = nextMetadataFile;
        nextMetadataFile = getMetadataFile(ver + 1);
      }

      updateVersionAndMetadata(ver, metadataFile.toString());

      this.shouldRefresh = false;
      return currentMetadata;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to refresh the table");
    }
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    Pair<Integer, TableMetadata> current = versionAndMetadata();
    if (base != current.second()) {
      throw new CommitFailedException("Cannot commit changes based on stale table metadata");
    }

    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    Preconditions.checkArgument(
        base == null || base.location().equals(metadata.location()),
        "Hadoop path-based tables cannot be relocated");
    Preconditions.checkArgument(
        !metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION),
        "Hadoop path-based tables cannot relocate metadata");

    String codecName =
        metadata.property(
            TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromName(codecName);
    String fileExtension = TableMetadataParser.getFileExtension(codec);
    Path tempMetadataFile = metadataPath(UUID.randomUUID().toString() + fileExtension);
    TableMetadataParser.write(metadata, io().newOutputFile(tempMetadataFile.toString()));

    int nextVersion = (current.first() != null ? current.first() : 0) + 1;
    Path finalMetadataFile = metadataFilePath(nextVersion, codec);
    FileSystem fs = getFileSystem(tempMetadataFile, conf);
    boolean versionCommitSuccess = false;
    boolean useObjectStore =
        metadata.propertyAsBoolean(
            TableProperties.OBJECT_STORE_ENABLED, TableProperties.OBJECT_STORE_ENABLED_DEFAULT);
    int previousVersionsMax =
        metadata.propertyAsInt(
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX,
            TableProperties.METADATA_PREVIOUS_VERSIONS_MAX_DEFAULT);
    // todo:Currently, if the user is using an object store, we assume that he must be using the
    //  global locking service. But we should support add some other conditions in future.
    boolean supportGlobalLocking = useObjectStore;
    try {
      tryLock(tempMetadataFile, metadataRoot());
      versionCommitSuccess =
          commitNewVersion(
              fs, tempMetadataFile, finalMetadataFile, nextVersion, supportGlobalLocking);
      if (!versionCommitSuccess) {
        throw new CommitFailedException(
            "Can not commit newMetaData because version [%s] has already been committed. tempMetaData=[%s],finalMetaData=[%s].Are there other clients running in parallel with the current task?",
            nextVersion, tempMetadataFile, finalMetadataFile);
      }
      validate(supportGlobalLocking, previousVersionsMax, nextVersion, fs, finalMetadataFile);
      this.shouldRefresh = true;
      LOG.info("Committed a new metadata file {}", finalMetadataFile);
      // update the best-effort version pointer
      writeVersionHint(fs, nextVersion);
      deleteRemovedMetadataFiles(base, metadata);
    } catch (CommitStateUnknownException e) {
      this.shouldRefresh = true;
      throw e;
    } catch (Exception e) {
      this.shouldRefresh = versionCommitSuccess;
      if (!versionCommitSuccess) {
        tryDelete(tempMetadataFile);
        throw new CommitFailedException(e);
      }
    } finally {
      unlock(tempMetadataFile, metadataRoot());
    }
  }

  private void tryDelete(Path path) {
    try {
      io().deleteFile(path.toString());
    } catch (Exception ignored) {
      // do nothing
    }
  }

  @VisibleForTesting
  void tryLock(Path src, Path dst) {
    if (!lockManager.acquire(dst.toString(), src.toString())) {
      throw new CommitFailedException(
          "Failed to acquire lock on file: %s with owner: %s", dst, src);
    }
  }

  void unlock(Path src, Path dst) {
    try {
      if (!lockManager.release(dst.toString(), src.toString())) {
        LOG.warn("Failed to release lock on file: {} with owner: {}", dst, src);
      }
    } catch (Exception ignored) {
      // do nothing.
    }
  }

  private void validate(
      boolean supportGlobalLocking,
      int previousVersionsMax,
      int nextVersion,
      FileSystem fs,
      Path finalMetadataFile)
      throws IOException {
    if (!supportGlobalLocking) {
      fastFailIfDirtyCommit(previousVersionsMax, nextVersion, fs, finalMetadataFile);
      cleanAllTooOldDirtyCommit(fs, previousVersionsMax);
    }
  }

  @VisibleForTesting
  void fastFailIfDirtyCommit(
      int previousVersionsMax, int nextVersion, FileSystem fs, Path finalMetadataFile)
      throws IOException {
    int currentMaxVersion = findVersionWithOutVersionHint(fs);
    if ((currentMaxVersion - nextVersion) > previousVersionsMax && fs.exists(finalMetadataFile)) {
      tryDelete(finalMetadataFile);
      throw new CommitStateUnknownException(
          new RejectedExecutionException(
              String.format(
                  "Commit rejected by server!The current commit version [%d] is much smaller than the latest version [%d].Are there other clients running in parallel with the current task?",
                  nextVersion, currentMaxVersion)));
    }
  }

  void cleanAllTooOldDirtyCommit(FileSystem fs, int previousVersionsMax) throws IOException {
    FileStatus[] files =
        fs.listStatus(metadataRoot(), name -> VERSION_PATTERN.matcher(name.getName()).matches());
    List<Path> dirtyCommits = Lists.newArrayList();
    int currentMaxVersion = findVersionWithOutVersionHint(fs);
    long now = System.currentTimeMillis();
    // We only clean up dirty commits that are some time old. This ensures that other clients can
    // find out as soon as possible if their current commit is dirty.

    // todo:Currently, dirty commits from seven days ago are deleted by default.
    //  There is no need to configure this for now.
    long ttl = 3600L * 24 * 1000 * 7;
    for (FileStatus file : files) {
      long modificationTime = file.getModificationTime();
      Path path = file.getPath();
      if ((currentMaxVersion - version(path.getName()) > previousVersionsMax)
          && (now - modificationTime) > ttl) {
        dirtyCommits.add(path);
      }
    }
    for (Path dirtyCommit : dirtyCommits) {
      io().deleteFile(dirtyCommit.toString());
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return metadataPath(fileName).toString();
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return uncommittedMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException(
            "Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return HadoopTableOperations.this.metadataFileLocation(fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        return HadoopTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return HadoopTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return HadoopTableOperations.this.newSnapshotId();
      }
    };
  }

  @VisibleForTesting
  Path getMetadataFile(int metadataVersion) throws IOException {
    for (TableMetadataParser.Codec codec : TABLE_METADATA_PARSER_CODEC_VALUES) {
      Path metadataFile = metadataFilePath(metadataVersion, codec);
      FileSystem fs = getFileSystem(metadataFile, conf);
      if (fs.exists(metadataFile)) {
        return metadataFile;
      }

      if (codec.equals(TableMetadataParser.Codec.GZIP)) {
        // we have to be backward-compatible with .metadata.json.gz files
        metadataFile = oldMetadataFilePath(metadataVersion, codec);
        fs = getFileSystem(metadataFile, conf);
        if (fs.exists(metadataFile)) {
          return metadataFile;
        }
      }
    }

    return null;
  }

  private Path metadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
    return metadataPath("v" + metadataVersion + TableMetadataParser.getFileExtension(codec));
  }

  private Path oldMetadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
    return metadataPath("v" + metadataVersion + TableMetadataParser.getOldFileExtension(codec));
  }

  private Path metadataPath(String filename) {
    return new Path(metadataRoot(), filename);
  }

  private Path metadataRoot() {
    return new Path(location, "metadata");
  }

  private int version(String fileName) {
    Matcher matcher = VERSION_PATTERN.matcher(fileName);
    if (!matcher.matches()) {
      return -1;
    }
    String versionNumber = matcher.group(1);
    try {
      return Integer.parseInt(versionNumber);
    } catch (NumberFormatException ne) {
      return -1;
    }
  }

  @VisibleForTesting
  Path versionHintFile() {
    return metadataPath(Util.VERSION_HINT_FILENAME);
  }

  @VisibleForTesting
  void writeVersionHint(FileSystem fs, Integer versionToWrite) throws Exception {
    Path versionHintFile = versionHintFile();
    Path tempVersionHintFile = metadataPath(UUID.randomUUID() + "-version-hint.temp");
    try {
      writeVersionToPath(fs, tempVersionHintFile, versionToWrite);
      fs.rename(tempVersionHintFile, versionHintFile);
    } catch (IOException e) {
      // Cleaning up temporary files.
      if (fs.exists(tempVersionHintFile)) {
        io().deleteFile(tempVersionHintFile.toString());
      }
      throw e;
    }
  }

  @VisibleForTesting
  boolean nextVersionIsLatest(int nextVersion, int currentMaxVersion) {
    return nextVersion == (currentMaxVersion + 1);
  }

  private void writeVersionToPath(FileSystem fs, Path path, int versionToWrite) {
    try (FSDataOutputStream out = fs.create(path, false /* overwrite */)) {
      out.write(String.valueOf(versionToWrite).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @VisibleForTesting
  int findVersionByUsingVersionHint(FileSystem fs, Path versionHintFile) throws IOException {
    try (InputStreamReader fsr =
            new InputStreamReader(fs.open(versionHintFile), StandardCharsets.UTF_8);
        BufferedReader in = new BufferedReader(fsr)) {
      return Integer.parseInt(in.readLine().replace("\n", ""));
    }
  }

  @VisibleForTesting
  int findVersionWithOutVersionHint(FileSystem fs) {
    try {
      if (!fs.exists(metadataRoot())) {
        // Either the table has just been created, or it has been corrupted, but either way, we have
        // to start at version 0.
        LOG.warn("Metadata for table not found in directory [{}]", metadataRoot());
        return 0;
      }
      // List the metadata directory to find the version files, and try to recover the max
      // available version
      FileStatus[] files =
          fs.listStatus(metadataRoot(), name -> VERSION_PATTERN.matcher(name.getName()).matches());
      int maxVersion = 0;
      for (FileStatus file : files) {
        int currentVersion = version(file.getPath().getName());
        if (currentVersion > maxVersion && getMetadataFile(currentVersion) != null) {
          maxVersion = currentVersion;
        }
      }
      return maxVersion;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  int findVersion() {
    Path versionHintFile = versionHintFile();
    FileSystem fs = getFileSystem(versionHintFile, conf);
    try {
      return fs.exists(versionHintFile)
          ? findVersionByUsingVersionHint(fs, versionHintFile)
          : findVersionWithOutVersionHint(fs);
    } catch (Exception e) {
      // try one last time
      return findVersionWithOutVersionHint(fs);
    }
  }

  /**
   * Renames the source file to destination, using the provided file system.
   *
   * @param fs the filesystem used for the rename
   * @param src the source file
   * @param dst the destination file
   * @return If it returns true, then the commit was successful.
   */
  @VisibleForTesting
  boolean commitNewVersion(
      FileSystem fs, Path src, Path dst, Integer nextVersion, boolean supportGlobalLocking)
      throws IOException {
    if (fs.exists(dst)) {
      throw new CommitFailedException("Version %d already exists: %s", nextVersion, dst);
    }
    int maxVersion = supportGlobalLocking ? findVersion() : findVersionWithOutVersionHint(fs);
    if (!nextVersionIsLatest(nextVersion, maxVersion)) {
      if (!supportGlobalLocking) {
        io().deleteFile(versionHintFile().toString());
      }
      throw new CommitFailedException(
          "Cannot commit version [%d] because it is smaller or much larger than the current latest version [%d].Are there other clients running in parallel with the current task?",
          nextVersion, maxVersion);
    }
    io().deleteFile(versionHintFile().toString());
    return renameMetaDataFileAndCheck(fs, src, dst, supportGlobalLocking);
  }

  protected FileSystem getFileSystem(Path path, Configuration hadoopConf) {
    return Util.getFs(path, hadoopConf);
  }

  @VisibleForTesting
  boolean checkMetaDataFileRenameSuccess(
      FileSystem fs, Path tempMetaDataFile, Path finalMetaDataFile, boolean supportGlobalLocking)
      throws IOException {
    if (!supportGlobalLocking) {
      return fs.exists(finalMetaDataFile) && !fs.exists(tempMetaDataFile);
    } else {
      return fs.exists(finalMetaDataFile);
    }
  }

  @VisibleForTesting
  boolean renameMetaDataFile(FileSystem fs, Path tempMetaDataFile, Path finalMetaDataFile)
      throws IOException {
    return fs.rename(tempMetaDataFile, finalMetaDataFile);
  }

  private boolean renameCheck(
      FileSystem fs,
      Path tempMetaDataFile,
      Path finalMetaDataFile,
      Throwable rootError,
      boolean supportGlobalLocking) {
    try {
      return checkMetaDataFileRenameSuccess(
          fs, tempMetaDataFile, finalMetaDataFile, supportGlobalLocking);
    } catch (Exception e) {
      throw new CommitStateUnknownException(rootError != null ? rootError : e);
    }
  }

  @VisibleForTesting
  boolean renameMetaDataFileAndCheck(
      FileSystem fs, Path tempMetaDataFile, Path finalMetaDataFile, boolean supportGlobalLocking) {
    try {
      return renameMetaDataFile(fs, tempMetaDataFile, finalMetaDataFile);
    } catch (IOException e) {
      // Server-side error, we need to try to recheck it again
      return renameCheck(fs, tempMetaDataFile, finalMetaDataFile, e, supportGlobalLocking);
    } catch (Exception e) {
      // Maybe Client-side error,Since the rename command may have already been committed and has
      // not yet been executed.There is no point in performing a check operation at this point.throw
      // CommitStateUnknownException and stop everything.
      throw new CommitStateUnknownException(e);
    }
  }
  /**
   * Deletes the oldest metadata files if {@link
   * TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED} is true.
   *
   * @param base table metadata on which previous versions were based
   * @param metadata new table metadata with updated previous versions
   */
  @VisibleForTesting
  void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return;
    }

    boolean deleteAfterCommit =
        metadata.propertyAsBoolean(
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

    if (deleteAfterCommit) {
      Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataFiles =
          Sets.newHashSet(base.previousFiles());
      removedPreviousMetadataFiles.removeAll(metadata.previousFiles());
      Tasks.foreach(removedPreviousMetadataFiles)
          .executeWith(ThreadPools.getWorkerPool())
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure(
              (previousMetadataFile, exc) ->
                  LOG.warn(
                      "Delete failed for previous metadata file: {}", previousMetadataFile, exc))
          .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
    }
  }

  private static TableMetadata checkUUID(TableMetadata currentMetadata, TableMetadata newMetadata) {
    String newUUID = newMetadata.uuid();
    if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
      Preconditions.checkState(
          newUUID.equals(currentMetadata.uuid()),
          "Table UUID does not match: current=%s != refreshed=%s",
          currentMetadata.uuid(),
          newUUID);
    }
    return newMetadata;
  }
}
