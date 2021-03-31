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

package org.wikimedia.elasticsearch.swift;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;
import org.wikimedia.elasticsearch.swift.repositories.SwiftService;
import org.wikimedia.elasticsearch.swift.repositories.account.SwiftAccountFactoryImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Our base plugin stuff.
 */
public class SwiftRepositoryPlugin extends Plugin implements RepositoryPlugin {
    @Override
    public Map<String, Repository.Factory> getRepositories(final Environment env,
                                                           final NamedXContentRegistry registry,
                                                           final ThreadPool threadPool) {
        return Collections.singletonMap(SwiftRepository.TYPE, repositoryFactory(env, registry, threadPool));
    }

    // for testability
    protected Repository.Factory repositoryFactory(final Environment env,
                                                   final NamedXContentRegistry registry,
                                                   final ThreadPool threadPool){
        return metadata -> {
            SwiftService swiftService = new SwiftService(env.settings(), threadPool);
            SwiftAccountFactoryImpl accountFactory = new SwiftAccountFactoryImpl(swiftService);

            return new SwiftRepository(metadata,
                metadata.settings(),
                env.settings(),
                registry,
                threadPool,
                accountFactory);
        };
    }

    @Override
    public List<String> getSettingsFilter() {
        return Collections.singletonList(SwiftRepository.Swift.PASSWORD_SETTING.getKey());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(SwiftRepository.Swift.MINIMIZE_BLOB_EXISTS_CHECKS_SETTING,
                             SwiftRepository.Swift.ALLOW_CACHING_SETTING,
                             SwiftRepository.Swift.DELETE_TIMEOUT_MIN_SETTING,
                             SwiftRepository.Swift.SNAPSHOT_TIMEOUT_MIN_SETTING,
                             SwiftRepository.Swift.SHORT_OPERATION_TIMEOUT_S_SETTING,
                             SwiftRepository.Swift.LONG_OPERATION_TIMEOUT_S_SETTING,
                             SwiftRepository.Swift.RETRY_INTERVAL_S_SETTING,
                             SwiftRepository.Swift.RETRY_COUNT_SETTING,
                             SwiftRepository.Swift.ALLOW_CONCURRENT_IO_SETTING,
                             SwiftRepository.Swift.STREAM_READ_SETTING,
                             SwiftRepository.Swift.STREAM_WRITE_SETTING);
    }

    public static final String SwiftExecutorName = "swift";

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        int halfOfProcessorCount = Runtime.getRuntime().availableProcessors() / 2;
        int maxPoolSize = Math.min(5, halfOfProcessorCount); //based on ES docs for snapshot thread pool size
        return new ArrayList<ExecutorBuilder<?>>() {{
            add(new ScalingExecutorBuilder(SwiftExecutorName,
                    1,
                    maxPoolSize,
                    TimeValue.timeValueMinutes(1),
                    SwiftExecutorName));
        }};
    }
}
