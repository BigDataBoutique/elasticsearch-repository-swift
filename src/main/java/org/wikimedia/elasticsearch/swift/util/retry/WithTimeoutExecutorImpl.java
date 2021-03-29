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
import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


class WithTimeoutExecutorImpl implements WithTimeout {
    private final ExecutorService executorService;
    private final Logger logger;

    WithTimeoutExecutorImpl(ExecutorService executorService, Logger logger){
        this.executorService = executorService;
        this.logger = logger;
    }

    @Override
    public <T> T retry(long interval, long timeout, TimeUnit timeUnit, Callable<T> callable) {
        Future<T> task = executorService.submit(() -> internalRetry(interval, timeout, timeUnit, Integer.MAX_VALUE, callable));
        try{
            return FutureUtils.get(task, timeout, timeUnit);
        }
        catch (Exception e){
            FutureUtils.cancel(task);
            throw e;
        }
    }

    @Override
    public <T> T retry(long interval, long timeout, TimeUnit timeUnit, int attempts, Callable<T> callable) {
        Future<T> task = executorService.submit(() -> internalRetry(interval, timeout, timeUnit, attempts, callable));
        try{
            return FutureUtils.get(task, timeout, timeUnit);
        }
        catch (Exception e){
            FutureUtils.cancel(task);
            throw e;
        }
    }

    @Override
    public <T> T timeout(long timeout, TimeUnit timeUnit, Callable<T> callable) {
        Future<T> task = executorService.submit(callable);
        try{
            return FutureUtils.get(task, timeout, timeUnit);
        }
        catch (Exception e){
            FutureUtils.cancel(task);
            throw e;
        }
    }

    private <T> T internalRetry(long interval, long timeout, TimeUnit timeUnit, final int attempts, Callable<T> callable)
            throws TimeoutException, InterruptedException {
        final long sleepMillis = TimeUnit.MILLISECONDS.convert(interval, timeUnit);
        final int sleepNanos = (int)(TimeUnit.NANOSECONDS.convert(interval, timeUnit) - sleepMillis * 1_000_000);
        final long nanoTimeLimit = System.nanoTime() + TimeUnit.NANOSECONDS.convert(timeout, timeUnit);

        int count = 0;
        while (count++ < attempts && System.nanoTime() < nanoTimeLimit) {
            try {
                return callable.call();
            }
            catch (InterruptedException e) {
                logger.error("Execution interrupted", e);
                throw e;
            }
            catch (Exception e) {
                if (count < attempts){
                    logger.error("Exception occurred, will retry", e);
                    //noinspection BusyWait
                    Thread.sleep(sleepMillis, sleepNanos);
                }
                else {
                    logger.error("Exception occurred, will not retry", e);
                }
            }
        }

        throw new TimeoutException("retry timed out");
    }
}
