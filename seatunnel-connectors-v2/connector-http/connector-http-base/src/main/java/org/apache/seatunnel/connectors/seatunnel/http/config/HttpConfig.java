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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.format.json.JsonField;

import java.util.List;
import java.util.Map;

public class HttpConfig {
    public static final String BASIC = "Basic";
    public static final String CONNECTOR_IDENTITY = "Http";
    public static final int DEFAULT_CONNECT_TIMEOUT_MS = 6000 * 2;
    public static final int DEFAULT_SOCKET_TIMEOUT_MS = 6000 * 10;
    public static final int DEFAULT_RETRY_BACKOFF_MULTIPLIER_MS = 100;
    public static final int DEFAULT_RETRY_BACKOFF_MAX_MS = 10000;
    public static final boolean DEFAULT_ENABLE_MULTI_LINES = false;
    public static final Option<Boolean> SKIP_SSL_VERIFICATION =
            Options.key("skip_ssl_verification")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to skip signing certificates");

    public static final Option<String> URL =
            Options.key("url").stringType().noDefaultValue().withDescription("Http request url");
    public static final Option<Long> TOTAL_PAGE_SIZE =
            Options.key("total_page_size")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("total page size");
    public static final Option<Integer> BATCH_SIZE =
            Options.key("batch_size")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            "the batch size returned per request is used to determine whether to continue when the total number of pages is unknown");
    public static final Option<Long> START_PAGE_NUMBER =
            Options.key("start_page_number")
                    .longType()
                    .defaultValue(1L)
                    .withDescription("which page to start synchronizing from");
    public static final Option<String> PAGE_FIELD =
            Options.key("page_field")
                    .stringType()
                    .defaultValue("page")
                    .withDescription(
                            "this parameter is used to specify the page field name in the request parameter");
    public static final Option<String> PAGE_PATH =
            Options.key("page_path")
                    .stringType()
                    .defaultValue("params")
                    .withDescription("指定page循环字段在params或者body里");
    public static final Option<Map<String, String>> PAGEING =
            Options.key("pageing").mapType().noDefaultValue().withDescription("pageing");
    public static final Option<HttpRequestMethod> METHOD =
            Options.key("method")
                    .enumType(HttpRequestMethod.class)
                    .defaultValue(HttpRequestMethod.GET)
                    .withDescription("Http request method");
    public static final Option<Map<String, String>> HEADERS =
            Options.key("headers")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("Http request headers");
    public static final Option<Map<String, String>> PARAMS =
            Options.key("params").mapType().noDefaultValue().withDescription("Http request params");
    public static final Option<String> BODY =
            Options.key("body").stringType().noDefaultValue().withDescription("Http request body");
    public static final Option<ResponseFormat> FORMAT =
            Options.key("format")
                    .enumType(ResponseFormat.class)
                    .defaultValue(ResponseFormat.JSON)
                    .withDescription("Http response format");
    public static final Option<Integer> POLL_INTERVAL_MILLS =
            Options.key("poll_interval_millis")
                    .intType()
                    .noDefaultValue()
                    .withDescription("Request http api interval(millis) in stream mode");
    public static final Option<Integer> RETRY =
            Options.key("retry")
                    .intType()
                    .noDefaultValue()
                    .withDescription("The max retry times if request http return to IOException");
    public static final Option<Integer> RETRY_BACKOFF_MULTIPLIER_MS =
            Options.key("retry_backoff_multiplier_ms")
                    .intType()
                    .defaultValue(DEFAULT_RETRY_BACKOFF_MULTIPLIER_MS)
                    .withDescription(
                            "The retry-backoff times(millis) multiplier if request http failed");
    public static final Option<Integer> RETRY_BACKOFF_MAX_MS =
            Options.key("retry_backoff_max_ms")
                    .intType()
                    .defaultValue(DEFAULT_RETRY_BACKOFF_MAX_MS)
                    .withDescription(
                            "The maximum retry-backoff times(millis) if request http failed");

    public static final Option<JsonField> JSON_FIELD =
            Options.key("json_field")
                    .objectType(JsonField.class)
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel json field.When partial json data is required, this parameter can be configured to obtain data");
    public static final Option<String> CONTENT_FIELD =
            Options.key("content_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "SeaTunnel content field.This parameter can get some json data, and there is no need to configure each field separately.");

    public static final Option<Boolean> ENABLE_MULTI_LINES =
            Options.key("enable_multi_lines")
                    .booleanType()
                    .defaultValue(DEFAULT_ENABLE_MULTI_LINES)
                    .withDescription(
                            "SeaTunnel enableMultiLines.This parameter can support http splitting response text by line.");

    public static final Option<Boolean> PARAMS_ADD =
            Options.key("params_add")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否追加请求参数params到所读取json的指定路径下，需要与params_path配合使用，默认追加到根路径");
    public static final Option<String> PARAMS_PATH =
            Options.key("params_path")
                    .stringType()
                    .defaultValue("$")
                    .withDescription("指定请求参数params追加到所读取的json的路径");
    public static final Option<Boolean> COOKIES_ADD =
            Options.key("cookies_add")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否追加cookies到所读取json的指定路径下，需要与cookies_path配合使用，默认追加到根路径");
    public static final Option<String> COOKIES_KEY =
            Options.key("cookies_key")
                    .stringType()
                    .defaultValue("JSESSIONID")
                    .withDescription("header返回的cookie的key值，默认是JSESSIONID");
    public static final Option<String> COOKIES_PATH =
            Options.key("cookies_path")
                    .stringType()
                    .defaultValue("$")
                    .withDescription("指定请求参数cookies追加到所读取的json的路径");
    public static final Option<Boolean> BODY_ADD =
            Options.key("body_add")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否追加请求body到所读取json的指定路径下，需要与body_path配合使用，默认追加到根路径");
    public static final Option<String> BODY_PATH =
            Options.key("body_path")
                    .stringType()
                    .defaultValue("$")
                    .withDescription("指定请求body追加到所读取的json的路径");
    // headerAdd
    public static final Option<Boolean> HEADERS_ADD =
            Options.key("headers_add")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("是否追加请求header到所读取json的指定路径下，需要与headers_path配合使用，默认追加到根路径");
    // headerPath
    public static final Option<String> HEADERS_PATH =
            Options.key("headers_path")
                    .stringType()
                    .defaultValue("$")
                    .withDescription("指定请求header追加到所读取的json的路径");

    public static final Option<List<String>> PARAMS_CONVERT =
            Options.key("params_convert")
                    .listType()
                    .noDefaultValue()
                    .withDescription("特定需求，将时间参数转换成时间戳");
    public static final Option<List<String>> BODY_CONVERT =
            Options.key("body_convert")
                    .listType()
                    .noDefaultValue()
                    .withDescription("特定需求，将时间参数转换成时间戳");
    public static final Option<List<String>> HEADERS_CONVERT =
            Options.key("headers_convert")
                    .listType()
                    .noDefaultValue()
                    .withDescription("特定需求，将请求头转换成时间戳");

    public static final Option<List<String>> PARAMS_BAN_LOOP =
            Options.key("params_ban_loop")
                    .listType()
                    .noDefaultValue()
                    .withDescription("请求参数指定字段禁止走list拆分参数");
    public static final Option<List<String>> PARAMS_PARSING_ARRAYS =
            Options.key("params_parsing_arrays")
                    .listType()
                    .noDefaultValue()
                    .withDescription("请求参数指定字段进行数组解析，然后循环调用http请求");
    public static final Option<List<String>> BODY_PARSING_ARRAYS =
            Options.key("body_parsing_arrays")
                    .listType()
                    .noDefaultValue()
                    .withDescription("请求参数指定字段进行数组解析，然后循环调用http请求");
    public static final Option<Map<String, String>> HEADERS_ENCRYPT =
            Options.key("headers_encrypt").mapType().noDefaultValue().withDescription("请求头加密");
    public static final Option<Map<String, String>> BODY_ENCRYPT =
            Options.key("body_encrypt").mapType().noDefaultValue().withDescription("请求体加密");
    public static final Option<Map<String, String>> PARAMS_ENCRYPT =
            Options.key("params_encrypt").mapType().noDefaultValue().withDescription("请求参数加密");
    // HikvisionApi
    public static final Option<Map<String, String>> HIKVISION_API =
            Options.key("hikvision_api").mapType().noDefaultValue().withDescription("海康特殊sdk处理");
    // sangForApi
    public static final Option<Map<String, String>> SANGFOR_API =
            Options.key("sangfor_api").mapType().noDefaultValue().withDescription("深信服特殊sdk处理");
    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(DEFAULT_CONNECT_TIMEOUT_MS)
                    .withDescription("Connection timeout setting, default 12s.");

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(DEFAULT_SOCKET_TIMEOUT_MS)
                    .withDescription("Socket timeout setting, default 60s.");
    // cycle_interval_ms
    public static final Option<Integer> CYCLE_INTERVAL_MS =
            Options.key("cycle_interval_ms")
                    .intType()
                    .defaultValue(0)
                    .withDescription("循环调用接口时的时间间隔");

    public enum ResponseFormat {
        JSON("json");

        private String format;

        ResponseFormat(String format) {
            this.format = format;
        }

        public String getFormat() {
            return format;
        }

        @Override
        public String toString() {
            return format;
        }
    }
}
