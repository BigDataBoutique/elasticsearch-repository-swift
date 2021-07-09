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

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.javaswift.joss.client.factory.AccountConfig;
import org.javaswift.joss.client.factory.AccountFactory;
import org.javaswift.joss.client.factory.AuthenticationMethod;
import org.javaswift.joss.client.factory.AuthenticationMethodScope;
import org.javaswift.joss.model.Account;
import org.wikimedia.elasticsearch.swift.SwiftPerms;
import org.wikimedia.elasticsearch.swift.util.retry.WithTimeout;

public class SwiftService extends AbstractLifecycleComponent {
    private static final String V3_AUTH_URL_SUFFIX = "/auth/tokens";

    private static final Logger logger = LogManager.getLogger(SwiftService.class);

    private final boolean allowCaching;
    private final WithTimeout.Factory withTimeoutFactory;
    private final ThreadPool threadPool;
    private final TimeValue retryInterval;
    private final TimeValue shortOperationTimeout;
    private final int retryCount;

    /**
     * Constructor
     * 
     * @param envSettings
     *            Settings for our repository. Injected.
     *
     * @param threadPool for retry()
     */
    @Inject
    public SwiftService(Settings envSettings, ThreadPool threadPool) {
        this.threadPool = threadPool;
        allowCaching = SwiftRepository.Swift.ALLOW_CACHING_SETTING.get(envSettings);
        withTimeoutFactory = new WithTimeout.Factory(envSettings, logger);
        retryInterval = SwiftRepository.Swift.RETRY_INTERVAL_SETTING.get(envSettings);
        shortOperationTimeout = SwiftRepository.Swift.SHORT_OPERATION_TIMEOUT_SETTING.get(envSettings);
        retryCount = SwiftRepository.Swift.RETRY_COUNT_SETTING.get(envSettings);
    }

    private WithTimeout withTimeout() {
        return threadPool != null ? withTimeoutFactory.create(threadPool) : withTimeoutFactory.createWithoutPool();
    }

    /**
     * Create a Swift account object and connect it to Swift
     * 
     * @param url
     *            The auth url (eg: localhost:8080/auth/v1.0/)
     * @param username
     *            The username
     * @param password
     *            The password
     * @param preferredRegion
     *            The preferred region set
     * @return swift Account
     */
    public Account swiftBasic(String url, String username, String password, String preferredRegion) {
        try {
            AccountConfig conf = getStandardConfig(url, username, password, AuthenticationMethod.BASIC,
                    preferredRegion);
            return createAccount(conf);
        }
        catch (Exception ce) {
            throw new ElasticsearchException("Unable to authenticate to Swift Basic " + url + "/" + username +
                "/" + password, ce);
        }
    }

    private Account createAccount(final AccountConfig conf) throws Exception {
        return withTimeout().retry(retryInterval, shortOperationTimeout, retryCount, () -> {
            try {
                return SwiftPerms.exec(() -> new AccountFactory(conf).createAccount());
            }
            catch (Exception e) {
                logger.warn("cannot authenticate", e);
                throw e;
            }
        });
    }

    public Account swiftKeyStone(String url,
                                 String username,
                                 String password,
                                 String tenantName,
                                 String preferredRegion) {
        try {
            AccountConfig conf = getStandardConfig(url,
                username,
                password,
                AuthenticationMethod.KEYSTONE,
                preferredRegion);
            conf.setTenantName(tenantName);
            return createAccount(conf);
        }
        catch (Exception ce) {
            String msg = "Unable to authenticate to Swift Keystone " + url + "/" + username + "/" + password + "/" + tenantName;
            throw new ElasticsearchException(msg, ce);
        }
    }

    public Account swiftKeyStoneV3(String url,
                                                String username,
                                                String password,
                                                String tenantName,
                                                String domainName,
                                                String preferredRegion) {
        try {
            if (!url.endsWith(V3_AUTH_URL_SUFFIX)) {
                url = StringUtils.chomp(url,"/") + V3_AUTH_URL_SUFFIX;
            }

            AccountConfig conf = getStandardConfig(url,
                username,
                password,
                AuthenticationMethod.KEYSTONE_V3,
                preferredRegion);

            if (StringUtils.isNotEmpty(tenantName)) {
                conf.setTenantName(tenantName);
                conf.setAuthenticationMethodScope(AuthenticationMethodScope.PROJECT_NAME);
            }
            else if (StringUtils.isNotEmpty(domainName) && !"Default".equalsIgnoreCase(domainName)) {
                conf.setDomain(domainName);
                conf.setAuthenticationMethodScope(AuthenticationMethodScope.DOMAIN_NAME);
            }

            return createAccount(conf);
        }
        catch (Exception ce) {
            String msg = "Unable to authenticate to Swift Keystone V3" + url + "/" + username + "/" + password + "/" +
                tenantName + "/" + domainName;
            throw new ElasticsearchException(msg, ce);
        }
    }

    public Account swiftTempAuth(String url, String username, String password, String preferredRegion) {
        try {
            AccountConfig conf = getStandardConfig(url,
                username,
                password,
                AuthenticationMethod.TEMPAUTH,
                preferredRegion);
            return createAccount(conf);
        }
        catch (Exception ce) {
            throw new ElasticsearchException("Unable to authenticate to Swift Temp", ce);
        }
    }

    private AccountConfig getStandardConfig(String url,
                                            String username,
                                            String password,
                                            AuthenticationMethod method,
                                            String preferredRegion) {
        AccountConfig conf = new AccountConfig();
        conf.setAuthUrl(url);
        conf.setUsername(username);
        conf.setPassword(password);
        conf.setAuthenticationMethod(method);
        conf.setAllowContainerCaching(allowCaching);
        conf.setAllowCaching(allowCaching);

        if (StringUtils.isNotEmpty(preferredRegion)) {
            conf.setPreferredRegion(preferredRegion);
        }

        return conf;
    }

    /**
     * Start the service. No-op here.
     */
    @Override
    protected void doStart() throws ElasticsearchException {
    }

    /**
     * Stop the service. No-op here.
     */
    @Override
    protected void doStop() throws ElasticsearchException {
    }

    /**
     * Close the service. No-op here.
     */
    @Override
    protected void doClose() throws ElasticsearchException {
    }
}
