/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.http.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@SuppressWarnings("MagicNumber")
public class HttpParameter implements Serializable {
    protected String url;
    protected HttpRequestMethod method;
    protected Map<String, String> headers;
    protected Map<String, String> params;
    protected String body;
    protected int pollIntervalMillis;
    protected int retry;
    protected int retryBackoffMultiplierMillis = HttpConfig.DEFAULT_RETRY_BACKOFF_MULTIPLIER_MS;
    protected int retryBackoffMaxMillis = HttpConfig.DEFAULT_RETRY_BACKOFF_MAX_MS;
    protected boolean enableMultilines;
    protected boolean skipSslVerification;
    protected boolean paramsAdd;
    protected String paramsPath;
    protected boolean cookiesAdd;
    protected String cookiesKey;
    protected String cookiesPath;
    protected boolean bodyAdd;
    protected String bodyPath;
    protected boolean headersAdd;
    protected String headersPath;
    protected List<String> paramsConvert;
    protected List<String> bodyConvert;
    protected List<String> headersConvert;
    protected List<String> paramsBanLooP;
    protected List<String> bodyParsingArrays;
    protected List<String> paramsParsingArrays;
    protected List<String> headersParsingArrays;
    protected Map<String, String> headersEncrypt;
    protected Map<String, String> bodyEncrypt;
    protected Map<String, String> paramsEncrypt;
    protected Map<String, String> HikvisionApi;
    protected Map<String, String> sangForApi;
    protected int connectTimeoutMs = HttpConfig.DEFAULT_CONNECT_TIMEOUT_MS;
    protected int socketTimeoutMs = HttpConfig.DEFAULT_SOCKET_TIMEOUT_MS;
    protected int cycleIntervalMillis;

    public void buildWithConfig(Config pluginConfig) {
        // set url
        this.setUrl(pluginConfig.getString(HttpConfig.URL.key()));
        // set method
        if (pluginConfig.hasPath(HttpConfig.METHOD.key())) {
            HttpRequestMethod httpRequestMethod =
                    HttpRequestMethod.valueOf(
                            pluginConfig.getString(HttpConfig.METHOD.key()).toUpperCase());
            this.setMethod(httpRequestMethod);
        } else {
            this.setMethod(HttpConfig.METHOD.defaultValue());
        }
        // set headers
        if (pluginConfig.hasPath(HttpConfig.HEADERS.key())) {
            this.setHeaders(
                    pluginConfig.getConfig(HttpConfig.HEADERS.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        // set params
        if (pluginConfig.hasPath(HttpConfig.PARAMS.key())) {
            this.setParams(
                    pluginConfig.getConfig(HttpConfig.PARAMS.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        // header_encrypt
        if (pluginConfig.hasPath(HttpConfig.HEADERS_ENCRYPT.key())) {
            this.setHeadersEncrypt(
                    pluginConfig.getConfig(HttpConfig.HEADERS_ENCRYPT.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        // body_encrypt
        if (pluginConfig.hasPath(HttpConfig.BODY_ENCRYPT.key())) {
            this.setBodyEncrypt(
                    pluginConfig.getConfig(HttpConfig.BODY_ENCRYPT.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        if (pluginConfig.hasPath(HttpConfig.PARAMS_ENCRYPT.key())) {
            this.setParamsEncrypt(
                    pluginConfig.getConfig(HttpConfig.PARAMS_ENCRYPT.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        if (pluginConfig.hasPath(HttpConfig.HIKVISION_API.key())) {
            this.setHikvisionApi(
                    pluginConfig.getConfig(HttpConfig.HIKVISION_API.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        // sangForApi
        if (pluginConfig.hasPath(HttpConfig.SANGFOR_API.key())) {
            this.setSangForApi(
                    pluginConfig.getConfig(HttpConfig.SANGFOR_API.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        // set body
        if (pluginConfig.hasPath(HttpConfig.BODY.key())) {
            this.setBody(pluginConfig.getString(HttpConfig.BODY.key()));
        }
        if (pluginConfig.hasPath(HttpConfig.POLL_INTERVAL_MILLS.key())) {
            this.setPollIntervalMillis(pluginConfig.getInt(HttpConfig.POLL_INTERVAL_MILLS.key()));
        }
        this.setRetryParameters(pluginConfig);
        if (pluginConfig.hasPath(HttpConfig.CONNECT_TIMEOUT_MS.key())) {
            this.setConnectTimeoutMs(pluginConfig.getInt(HttpConfig.CONNECT_TIMEOUT_MS.key()));
        }
        if (pluginConfig.hasPath(HttpConfig.SOCKET_TIMEOUT_MS.key())) {
            this.setSocketTimeoutMs(pluginConfig.getInt(HttpConfig.SOCKET_TIMEOUT_MS.key()));
        }
        // set enableMultilines
        if (pluginConfig.hasPath(HttpConfig.ENABLE_MULTI_LINES.key())) {
            this.setEnableMultilines(pluginConfig.getBoolean(HttpConfig.ENABLE_MULTI_LINES.key()));
        } else {
            this.setEnableMultilines(HttpConfig.ENABLE_MULTI_LINES.defaultValue());
        }
        // set skipSslVerification
        if (pluginConfig.hasPath(HttpConfig.SKIP_SSL_VERIFICATION.key())) {
            this.setSkipSslVerification(
                    pluginConfig.getBoolean(HttpConfig.SKIP_SSL_VERIFICATION.key()));
        } else {
            this.setSkipSslVerification(HttpConfig.SKIP_SSL_VERIFICATION.defaultValue());
        }
        // set params_add
        if (pluginConfig.hasPath(HttpConfig.PARAMS_ADD.key())) {
            this.setParamsAdd(pluginConfig.getBoolean(HttpConfig.PARAMS_ADD.key()));
        } else {
            this.setParamsAdd(HttpConfig.PARAMS_ADD.defaultValue());
        }
        // set params_path
        if (pluginConfig.hasPath(HttpConfig.PARAMS_PATH.key())) {
            this.setParamsPath(pluginConfig.getString(HttpConfig.PARAMS_PATH.key()));
        } else {
            this.setParamsPath(HttpConfig.PARAMS_PATH.defaultValue());
        }
        // set cookies_add
        if (pluginConfig.hasPath(HttpConfig.COOKIES_ADD.key())) {
            this.setCookiesAdd(pluginConfig.getBoolean(HttpConfig.COOKIES_ADD.key()));
        } else {
            this.setCookiesAdd(HttpConfig.COOKIES_ADD.defaultValue());
        }
        // set cookies_key
        if (pluginConfig.hasPath(HttpConfig.COOKIES_KEY.key())) {
            this.setCookiesKey(pluginConfig.getString(HttpConfig.COOKIES_KEY.key()));
        } else {
            this.setCookiesKey(HttpConfig.COOKIES_KEY.defaultValue());
        }
        // set cookies_path
        if (pluginConfig.hasPath(HttpConfig.COOKIES_PATH.key())) {
            this.setCookiesPath(pluginConfig.getString(HttpConfig.COOKIES_PATH.key()));
        } else {
            this.setCookiesPath(HttpConfig.COOKIES_PATH.defaultValue());
        }
        // bodyAdd
        if (pluginConfig.hasPath(HttpConfig.BODY_ADD.key())) {
            this.setBodyAdd(pluginConfig.getBoolean(HttpConfig.BODY_ADD.key()));
        } else {
            this.setBodyAdd(HttpConfig.BODY_ADD.defaultValue());
        }
        // set body_path
        if (pluginConfig.hasPath(HttpConfig.BODY_PATH.key())) {
            this.setBodyPath(pluginConfig.getString(HttpConfig.BODY_PATH.key()));
        } else {
            this.setBodyPath(HttpConfig.BODY_PATH.defaultValue());
        }
        // set headers_add
        if (pluginConfig.hasPath(HttpConfig.HEADERS_ADD.key())) {
            this.setHeadersAdd(pluginConfig.getBoolean(HttpConfig.HEADERS_ADD.key()));
        } else {
            this.setHeadersAdd(HttpConfig.HEADERS_ADD.defaultValue());
        }
        // set headers_path
        if (pluginConfig.hasPath(HttpConfig.HEADERS_PATH.key())) {
            this.setHeadersPath(pluginConfig.getString(HttpConfig.HEADERS_PATH.key()));
        } else {
            this.setHeadersPath(HttpConfig.HEADERS_PATH.defaultValue());
        }
        // set params_conversion
        if (pluginConfig.hasPath(HttpConfig.PARAMS_CONVERT.key())) {
            this.setParamsConvert(pluginConfig.getStringList(HttpConfig.PARAMS_CONVERT.key()));
        }
        // set body_conversion
        if (pluginConfig.hasPath(HttpConfig.BODY_CONVERT.key())) {
            this.setBodyConvert(pluginConfig.getStringList(HttpConfig.BODY_CONVERT.key()));
        }
        // headersConvert
        if (pluginConfig.hasPath(HttpConfig.HEADERS_CONVERT.key())) {
            this.setHeadersConvert(pluginConfig.getStringList(HttpConfig.HEADERS_CONVERT.key()));
        }
        // set params_ban_loop
        if (pluginConfig.hasPath(HttpConfig.PARAMS_BAN_LOOP.key())) {
            this.setParamsBanLooP(pluginConfig.getStringList(HttpConfig.PARAMS_BAN_LOOP.key()));
        }
        if (pluginConfig.hasPath(HttpConfig.BODY_PARSING_ARRAYS.key())) {
            this.setBodyParsingArrays(
                    pluginConfig.getStringList(HttpConfig.BODY_PARSING_ARRAYS.key()));
        }
        if (pluginConfig.hasPath(HttpConfig.PARAMS_PARSING_ARRAYS.key())) {
            this.setParamsParsingArrays(
                    pluginConfig.getStringList(HttpConfig.PARAMS_PARSING_ARRAYS.key()));
        }
        // HEADERS_PARSING_ARRAYS
        if (pluginConfig.hasPath(HttpConfig.HEADERS_PARSING_ARRAYS.key())) {
            this.setHeadersParsingArrays(
                    pluginConfig.getStringList(HttpConfig.HEADERS_PARSING_ARRAYS.key()));
        }
        // cycle_interval_ms
        if (pluginConfig.hasPath(HttpConfig.CYCLE_INTERVAL_MS.key())) {
            this.setCycleIntervalMillis(pluginConfig.getInt(HttpConfig.CYCLE_INTERVAL_MS.key()));
        }
    }

    public void setRetryParameters(Config pluginConfig) {
        if (pluginConfig.hasPath(HttpConfig.RETRY.key())) {
            this.setRetry(pluginConfig.getInt(HttpConfig.RETRY.key()));
            if (pluginConfig.hasPath(HttpConfig.RETRY_BACKOFF_MULTIPLIER_MS.key())) {
                this.setRetryBackoffMultiplierMillis(
                        pluginConfig.getInt(HttpConfig.RETRY_BACKOFF_MULTIPLIER_MS.key()));
            }
            if (pluginConfig.hasPath(HttpConfig.RETRY_BACKOFF_MAX_MS.key())) {
                this.setRetryBackoffMaxMillis(
                        pluginConfig.getInt(HttpConfig.RETRY_BACKOFF_MAX_MS.key()));
            }
        }
    }
}
