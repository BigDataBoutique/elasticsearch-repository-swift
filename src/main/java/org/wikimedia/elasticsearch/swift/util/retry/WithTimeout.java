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

package org.wikimedia.elasticsearch.swift.util.retry;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.wikimedia.elasticsearch.swift.repositories.SwiftRepository;

import java.util.concurrent.Callable;

public interface WithTimeout {
    <T> T retry(TimeValue interval, TimeValue timeout, Callable<T> callable) throws Exception;

    <T> T retry(TimeValue interval, TimeValue timeout, int attempts, Callable<T> callable) throws Exception;

    <T> T timeout(TimeValue timeout, Callable<T> callable) throws Exception;

    class Factory {
        private final Logger logger;
        private final Settings settings;

        public Factory(Settings settings, Logger logger){
            this.settings = settings;
            this.logger = logger;
        }

        public ExecutorBuilder<?> createExecutorBuilder() {
            return new ScalingExecutorBuilder(SwiftRepository.Swift.PREFIX,
                1,
                SwiftRepository.Swift.MAX_IO_REQUESTS.get(settings),
                TimeValue.timeValueMinutes(1));
        }

        public WithTimeout create(ThreadPool threadPool){
            return new WithTimeoutExecutorImpl(threadPool.executor(SwiftRepository.Swift.PREFIX), logger);
        }

        public WithTimeout createWithoutPool(){
            return new WithTimeoutDirectImpl();
        }
    }
}
