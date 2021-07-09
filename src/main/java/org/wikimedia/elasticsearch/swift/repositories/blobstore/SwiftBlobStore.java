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

package org.wikimedia.elasticsearch.swift.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.javaswift.joss.model.Account;
import org.javaswift.joss.model.Container;
import org.wikimedia.elasticsearch.swift.SwiftPerms;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;
import org.wikimedia.elasticsearch.swift.util.blob.SavedBlob;
import org.wikimedia.elasticsearch.swift.util.retry.WithTimeout;

/**
 * Our blob store
 */
public class SwiftBlobStore implements BlobStore {
    private static final Logger logger = LogManager.getLogger(SwiftBlobStore.class);

    // Our Swift container. This is important.
    private final String containerName;

    private final Settings envSettings;

    public Settings getEnvSettings() {
        return envSettings;
    }

    private volatile Container container = null;

    private final Account auth;
    
    private final SwiftRepository repository;

    private final WithTimeout.Factory withTimeoutFactory;
    private final TimeValue retryInterval;
    private final TimeValue shortOperationTimeout;
    private final int retryCount;
    private final String blobLocalDir;

    /**
     * Constructor. Sets up the container mostly.
     * @param repository owning repository
     * @param repoSettings Settings for our repository. Only care about buffer size.
     * @param envSettings global settings
     * @param auth swift account info
     * @param containerName swift container
     */
    public SwiftBlobStore(SwiftRepository repository,
                          Settings repoSettings,
                          Settings envSettings,
                          final Account auth,
                          final String containerName) {
        this.repository = repository;
        this.envSettings = envSettings;
        this.auth = auth;
        this.containerName = containerName;
        withTimeoutFactory = new WithTimeout.Factory(envSettings, logger);
        retryInterval = SwiftRepository.Swift.RETRY_INTERVAL_SETTING.get(envSettings);
        retryCount = SwiftRepository.Swift.RETRY_COUNT_SETTING.get(envSettings);
        shortOperationTimeout = SwiftRepository.Swift.SHORT_OPERATION_TIMEOUT_SETTING.get(envSettings);
        blobLocalDir = SwiftRepository.Swift.BLOB_LOCAL_DIR_SETTING.get(envSettings);
    }

    private WithTimeout withTimeout(){
        return repository != null ? withTimeoutFactory.create(repository.threadPool()) : withTimeoutFactory.createWithoutPool();
    }

    /**
     * Initialize container lazily. Do not produce a storm of Swift requests.
     * @return the container
     * @throws Exception create retry()
     */
    public Container getContainer() throws Exception {
        if (container != null) {
            return container;
        }

        synchronized(this){
            if (container != null) {
                return container;
            }

            container = internalGetContainer();
            return container;
        }
    }

    private Container internalGetContainer() throws Exception {
        return withTimeout().retry(retryInterval, shortOperationTimeout, retryCount, () -> {
            try {
                return SwiftPerms.exec(() -> {
                    Container container = auth.getContainer(containerName);
                    if (!container.exists()) {
                        container.create();
                        container.makePublic();
                    }
                    return container;
                });
            }
            catch (Exception e) {
                logger.warn("cannot get container [" + containerName + "]", e);
                throw e;
            }
        });
    }

    /**
     * Factory for getting blob containers for a path
     * @param path The blob path to search
     */
    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new SwiftBlobContainer(this, path, new SavedBlob.Factory(blobLocalDir));
    }

    /**
     * Close the store. No-op for us.
     */
    @Override
    public void close() {
    }

    public SwiftRepository getRepository() {
        return repository;
    }
}
