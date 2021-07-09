/*
 * Copyright 2017 Wikimedia and BigData Boutique
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wikimedia.elasticsearch.swift.repositories;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.threadpool.ThreadPool;
import org.javaswift.joss.model.Account;
import org.wikimedia.elasticsearch.swift.SwiftPerms;
import org.wikimedia.elasticsearch.swift.repositories.account.SwiftAccountFactory;
import org.wikimedia.elasticsearch.swift.repositories.blobstore.SwiftBlobStore;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The blob store repository. A glorified settings wrapper.
 */
public class SwiftRepository extends BlobStoreRepository {
    // The internal "type" for Elasticsearch Swift repository
    public static final String TYPE = "swift";

    /**
     * Swift repository settings
     */
    public interface Swift {
        String PREFIX = "repository_swift";

        Setting<String> CONTAINER_SETTING = Setting.simpleString("swift_container");
        Setting<String> URL_SETTING = Setting.simpleString("swift_url");
        Setting<String> AUTHMETHOD_SETTING = Setting.simpleString("swift_authmethod");
        Setting<String> PASSWORD_SETTING = Setting.simpleString("swift_password");
        Setting<String> TENANTNAME_SETTING = Setting.simpleString("swift_tenantname");
        Setting<String> DOMAINNAME_SETTING = Setting.simpleString("swift_domainname");
        Setting<String> USERNAME_SETTING = Setting.simpleString("swift_username");
        Setting<String> PREFERRED_REGION_SETTING = Setting.simpleString("swift_preferred_region");
        Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting("chunk_size",
                                                                            new ByteSizeValue(5, ByteSizeUnit.GB));
        Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);
        Setting<Boolean> MINIMIZE_BLOB_EXISTS_CHECKS_SETTING = Setting.boolSetting(PREFIX+".minimize_blob_exists_checks",
                                                                                   true,
                                                                                    Setting.Property.NodeScope);
        Setting<Boolean> ALLOW_CACHING_SETTING = Setting.boolSetting(PREFIX+".allow_caching",
                                                                     true,
                                                                     Setting.Property.NodeScope);

        Setting<Integer> MAX_IO_REQUESTS = Setting.intSetting(PREFIX+".max_io_requests",
            10,
            Setting.Property.NodeScope);

        Setting<TimeValue> DELETE_TIMEOUT_SETTING = Setting.timeSetting(PREFIX+".delete_timeout",
            new TimeValue(60, TimeUnit.MINUTES),
            Setting.Property.NodeScope);

        Setting<TimeValue> SNAPSHOT_TIMEOUT_SETTING = Setting.timeSetting(PREFIX+".snapshot_timeout",
            new TimeValue(360, TimeUnit.MINUTES),
            Setting.Property.NodeScope);

        Setting<TimeValue> SHORT_OPERATION_TIMEOUT_SETTING = Setting.timeSetting(PREFIX+".short_operation_timeout",
            new TimeValue(2, TimeUnit.MINUTES),
            Setting.Property.NodeScope);

        Setting<TimeValue> LONG_OPERATION_TIMEOUT_SETTING = Setting.timeSetting(PREFIX+".long_operation_timeout",
            new TimeValue(20, TimeUnit.MINUTES),
            Setting.Property.NodeScope);

        Setting<TimeValue> RETRY_INTERVAL_SETTING = Setting.timeSetting(PREFIX+".retry_interval",
            new TimeValue(10, TimeUnit.SECONDS),
            Setting.Property.NodeScope);

        Setting<Integer> RETRY_COUNT_SETTING = Setting.intSetting(PREFIX+".retry_count",
            3,
            Setting.Property.NodeScope);

        Setting<Boolean> ALLOW_CONCURRENT_IO_SETTING = Setting.boolSetting(PREFIX+".allow_concurrent_io",
            true,
            Setting.Property.NodeScope);

        Setting<Boolean> STREAM_WRITE_SETTING = Setting.boolSetting(PREFIX+".stream_write",
            false,
            Setting.Property.NodeScope);

        Setting<String> BLOB_LOCAL_DIR_SETTING = Setting.simpleString(PREFIX + ".blob_local_dir",
            "/var/"+PREFIX,
            Setting.Property.NodeScope);
    }

    private static final Logger logger = LogManager.getLogger(SwiftRepository.class);

    // Base path for blobs
    private final BlobPath basePath;

    // Chunk size.
    private final ByteSizeValue chunkSize;

    private final Settings repoSettings;
    private final Settings envSettings;

    private final ConcurrentHashMap<String, Future<DeleteResult>> blobDeletionTasks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Future<Void>> blobWriteTasks = new ConcurrentHashMap<>();
    private final SwiftAccountFactory accountFactory;

    /**
     * Constructs new BlobStoreRepository
     *
     * @param metadata
     *            repository meta data
     * @param repoSettings
     *            repo settings
     * @param envSettings
     *            global settings
     * @param namedXContentRegistry
     *            an instance of NamedXContentRegistry
     * @param threadPool
     *            an elastic search ThreadPool
     * @param accountFactory
     *            account factory
     */
    @Inject
    public SwiftRepository(final RepositoryMetaData metadata,
                           final Settings repoSettings,
                           final Settings envSettings,
                           final NamedXContentRegistry namedXContentRegistry,
                           final ThreadPool threadPool,
                           final SwiftAccountFactory accountFactory) {
        super(metadata, Swift.COMPRESS_SETTING.get(repoSettings), namedXContentRegistry, threadPool);
        this.repoSettings = repoSettings;
        this.envSettings = envSettings;
        this.chunkSize = Swift.CHUNK_SIZE_SETTING.get(repoSettings);
        this.basePath = BlobPath.cleanPath();
        this.accountFactory = accountFactory;
    }

    @Override
    protected void doStart() {
        super.doStart();
        clearStoredBlobs();
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData clusterMetaData) {
        initializeBlobTasks();
        super.initializeSnapshot(snapshotId, indices, clusterMetaData);
    }

    @Override
    public void deleteSnapshot(SnapshotId snapshotId, long repositoryStateId, ActionListener<Void> listener) {
        initializeBlobTasks();
        try{
            super.deleteSnapshot(snapshotId, repositoryStateId, listener);
        }
        finally {
            finalizeBlobTasks(snapshotId.toString(), listener);
        }
    }

    private void initializeBlobTasks() {
        blobDeletionTasks.clear();
        blobWriteTasks.clear();
    }

    public void addDeletion(String blobName, Future<DeleteResult> task) {
        blobDeletionTasks.put(blobName, task);
    }

    //
    // Intent of this method is to provide a wait that delays completion of potentially mutually exclusive operations
    // in Elasticsearch
    //
    private void finalizeBlobDeletion(String operationId, ActionListener<?> listener, final TimeValue timeout) {
        long failedCount = 0;
        final long nanoTimeLimit = System.nanoTime() + timeout.nanos();

        for (Map.Entry<String, Future<DeleteResult>> entry: blobDeletionTasks.entrySet()) {
            String blobName = entry.getKey();
            Future<DeleteResult> task = entry.getValue();

            try {
                long remaining_ns = Math.max(nanoTimeLimit - System.nanoTime(), 0);

                FutureUtils.get(task, remaining_ns, TimeUnit.NANOSECONDS);
            }
            catch (Exception e) {
                logger.warn("Failed to delete blob [" + blobName + "], snapshot ["+ operationId + "]. Cancelling task", e);
                FutureUtils.cancel(task);
                failedCount++;
            }
        }
        blobDeletionTasks.clear();

        if (failedCount > 0 && listener != null){
            listener.onFailure(new RepositoryException(metadata.name(),
                "failed to delete snapshot [" + operationId + "]: failed to delete " + failedCount + " blobs"));
        }
    }

    private void clearStoredBlobs(){
        String blobDir = Swift.BLOB_LOCAL_DIR_SETTING.get(envSettings);
        try {
            SwiftPerms.execThrows(() ->
                FileUtils.cleanDirectory(FileUtils.getFile(blobDir)));
        }
        catch (Exception e){
            logger.warn("Unable to clean directory ["+blobDir+"]", e);
        }
    }

    private void finalizeBlobTasks(String operationId, ActionListener<?> listener) {
        finalizeBlobWrite(operationId, listener, Swift.SNAPSHOT_TIMEOUT_SETTING.get(envSettings));
        finalizeBlobDeletion(operationId, listener, Swift.DELETE_TIMEOUT_SETTING.get(envSettings));
    }

    public void addWrite(String blobName, Future<Void> task) {
        blobWriteTasks.put(blobName, task);
    }

    //
    // Intent of this method is to provide a wait that delays completion of potentially mutually exclusive operations
    // in Elasticsearch
    //
    private void finalizeBlobWrite(String operationId, ActionListener<?> listener, final TimeValue timeout) {
        long failedCount = 0;
        final long nanoTimeLimit = System.nanoTime() + timeout.nanos();

        for (Map.Entry<String, Future<Void>> entry: blobWriteTasks.entrySet()) {
            String blobName = entry.getKey();
            Future<Void> task = entry.getValue();

            try {
                long remaining_ns = Math.max(nanoTimeLimit - System.nanoTime(), 0);

                FutureUtils.get(task, remaining_ns, TimeUnit.NANOSECONDS);
            }
            catch (Exception e) {
                logger.warn("Failed to write blob [" + blobName + "], snapshot ["+ operationId + "]. Cancelling task", e);
                FutureUtils.cancel(task);
                failedCount++;
            }
        }
        blobWriteTasks.clear();

        if (failedCount > 0 && listener != null){
            listener.onFailure(new RepositoryException(metadata.name(),
                    "failed to complete snapshot [" + operationId + "]: failed to write " + failedCount + " blobs"));
        }
    }

    @Override
    public void cleanup(long repositoryStateId, ActionListener<RepositoryCleanupResult> listener) {
        initializeBlobTasks();
        try {
            super.cleanup(repositoryStateId, listener);
        } finally {
            finalizeBlobTasks(String.valueOf(repositoryStateId), listener);
        }
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, List<IndexId> indices, long startTime, String failure,
                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                 boolean includeGlobalState, MetaData clusterMetaData, Map<String, Object> userMetadata,
                                 ActionListener<SnapshotInfo> listener) {
        try {
            super.finalizeSnapshot(snapshotId, indices, startTime, failure, totalShards, shardFailures, repositoryStateId,
                    includeGlobalState, clusterMetaData, userMetadata, listener);
        } finally {
            finalizeBlobTasks(snapshotId.toString(), listener);
        }
    }

    @Override
    public String startVerification() {
        initializeBlobTasks();
        return super.startVerification();
    }

    @Override
    public void endVerification(String seed) {
        try {
            super.endVerification(seed);
        } finally {
            finalizeBlobTasks("verification", null);
        }
    }

    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus,
                              ActionListener<Void> listener) {
        super.snapshotShard(store, mapperService, snapshotId, indexId, snapshotIndexCommit, snapshotStatus, listener);
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, Version version, IndexId indexId, ShardId shardId) {
        return super.getShardSnapshotStatus(snapshotId, version, indexId, shardId);
    }

    protected BlobStore createBlobStore() {
        String username = Swift.USERNAME_SETTING.get(repoSettings);
        String password = Swift.PASSWORD_SETTING.get(repoSettings);
        String tenantName = Swift.TENANTNAME_SETTING.get(repoSettings);
        String domainName = Swift.DOMAINNAME_SETTING.get(repoSettings);
        String authMethod = Swift.AUTHMETHOD_SETTING.get(repoSettings);
        String preferredRegion = Swift.PREFERRED_REGION_SETTING.get(repoSettings);

        String containerName = Swift.CONTAINER_SETTING.get(repoSettings);
        if (containerName == null) {
            throw new RepositoryException(metadata.name(), "No container defined for swift repository");
        }

        String url = Swift.URL_SETTING.get(repoSettings);
        if (url == null) {
            throw new RepositoryException(metadata.name(), "No url defined for swift repository");
        }

        Account account = accountFactory.createAccount(url,
            username,
            password,
            tenantName,
            domainName,
            authMethod,
            preferredRegion);

        return new SwiftBlobStore(this, repoSettings, envSettings, account, containerName);
    }

    /**
     * Get the base blob path
     */
    @Override
    public BlobPath basePath() {
        return basePath;
    }

    /**
     * Get the chunk size
     */
    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }
}
