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

package org.apache.seatunnel.connectors.seatunnel.http.source;



import com.jayway.jsonpath.*;
import com.sangfor.ngsoc.common.aksk.service.impl.SigSignerJavaImpl;
import net.minidev.json.JSONArray;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptHandler;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.Factory.DefaultEncryptStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.Factory.EncryptStrategyFactory;
import org.apache.seatunnel.connectors.seatunnel.http.util.EncryptUtil;
import org.apache.seatunnel.connectors.seatunnel.http.util.TimeUtils;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;

import com.google.common.base.Strings;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider.APPLICATION_JSON;
import static org.apache.seatunnel.connectors.seatunnel.http.source.encrypt.EncryptRequest.*;

@Slf4j
@Setter
public class HttpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    protected final SingleSplitReaderContext context;
    protected final HttpParameter httpParameter;
    protected HttpClientProvider httpClient;
    private final DeserializationCollector deserializationCollector;
    private static final Option[] DEFAULT_OPTIONS = {
        Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST, Option.DEFAULT_PATH_LEAF_TO_NULL
    };
    private JsonPath[] jsonPaths;
    private final JsonField jsonField;
    private final String contentJson;
    private final Configuration jsonConfiguration =
            Configuration.defaultConfiguration().addOptions(DEFAULT_OPTIONS);
    private boolean noMoreElementFlag = true;
    private Optional<PageInfo> pageInfoOptional = Optional.empty();
    private boolean hasListUrl= false;
    private Long pageIndex = 1L;
    public static final String SANGFOR = "sangfor";
    //sdk
    public static final String SDK = "sdk";
    //authCode
    public static final String AUTHCODE = "authCode";


    public HttpSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentJson) {
        this.context = context;
        this.httpParameter = httpParameter;
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
        this.jsonField = jsonField;
        this.contentJson = contentJson;
    }

    public HttpSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            JsonField jsonField,
            String contentJson,
            PageInfo pageInfo) {
        this.context = context;
        this.httpParameter = httpParameter;
        this.deserializationCollector = new DeserializationCollector(deserializationSchema);
        this.jsonField = jsonField;
        this.contentJson = contentJson;
        this.pageInfoOptional = Optional.ofNullable(pageInfo);
    }

    @Override
    public void open() {
        httpClient = new HttpClientProvider(httpParameter);
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }

    public void pollAndCollectData(Collector<SeaTunnelRow> output) throws Exception {

        String url = this.httpParameter.getUrl();
        Map<String, String> params = this.httpParameter.getParams();
        String body = replaceBody(this.httpParameter.getBody());
        Map<String, String> headers = this.httpParameter.getHeaders();
        List<String> banLooPKeys = this.httpParameter.getParamsBanLooP();
        //检查是否存在params_convert,body_convert
        if (!Optional.ofNullable(this.httpParameter.getParamsConvert()).map(List::isEmpty).orElse(true)
                || !Optional.ofNullable(this.httpParameter.getBodyConvert()).map(List::isEmpty).orElse(true)) {

            //获取httpParameter的params字段
            List<String> paramsConvert = this.httpParameter.getParamsConvert();
            params = paramsConvertTime(paramsConvert, params);
            //获取httpParameter的body字段
            List<String> bodyConvert = this.httpParameter.getBodyConvert();
            try {
                body = BodyConvertTime(bodyConvert, body);
            } catch (DateTimeParseException e){
                throw new SeaTunnelException("时间(yyyy-MM-dd HH:mm:ss)转时间戳失败",e);
            } catch (Exception e){
                throw new SeaTunnelException("body转json失败，请保证body是正常的json字符串",e);
            }
            //获取httpParameter的header字段
            List<String> headerConvert = this.httpParameter.getHeadersConvert();
            headers = headerConvertTime(headerConvert, headers);
        }
        // 检查url是否存在list标识
        if(url.contains("[") && url.contains("]") ){
            hasListUrl = true;
        }
        // 没有参数和body直接调用该请求，同时检查url是否存在list标识，然后并返回
        if (MapUtils.isEmpty(params) && StringUtils.isBlank(body)) {
            checkUrl(output, url, new HashMap<>(),body,headers);
            return;
        }
        // 检查是否有列表参数，是的话isParamsArrays=true
        boolean isParamsArrays = false;
        List<String> paramsListKey = this.httpParameter.getParamsParsingArrays();
        if (MapUtils.isNotEmpty(params)) {
            isParamsArrays = CollectionUtils.isNotEmpty(paramsListKey)
                    && params.keySet().stream().anyMatch(paramsListKey::contains);
        }

        boolean isBodyArrays = false;
        List<String> bodyPListKey = this.httpParameter.getBodyParsingArrays();
        if (StringUtils.isNotBlank(body)) {
            try {

                DocumentContext jsonContext = JsonPath.using(Configuration.defaultConfiguration()).parse(body);
                isBodyArrays = CollectionUtils.isNotEmpty(bodyPListKey)
                        && bodyPListKey.stream()
                        .allMatch(key ->
                                isKeyPresent(jsonContext,key));
            } catch (Exception e){
                throw new SeaTunnelException("body转json失败，请保证body是正常的json字符串",e);
            }
        }



        // 更新列表参数
        if (isParamsArrays) {
            updateListParams(output, url, params, body, paramsListKey,headers);
        } else if (isBodyArrays) {
            updateListBody(output, url, params, body, bodyPListKey,headers);
        } else {
            // 没有参数时，先检查url是否存在list标识后，调用该请求
            checkUrl(output, url, params, body,headers);
        }
    }

    public boolean isKeyPresent(DocumentContext jsonContext, String key){
        try{
            jsonContext.read(key);
            return true;
        }catch(Exception e){
            return false;
        }
    }

    // TODO 更新body为list的情况
    private void updateListBody(Collector<SeaTunnelRow> output, String url, Map<String, String> params, String body, List<String> bodyPListKey,Map<String, String> headers) throws Exception {

        //解析body
        DocumentContext jsonContext = JsonPath.using(Configuration.defaultConfiguration()).parse(body);
        //拿到每个列表参数的value
        List<Object> valueList = new ArrayList<>();
        Map<String, List<Object>> updateBody = new HashMap<>();
        for (String jsonPathKey : bodyPListKey) {
            //通过JsonPath解析深层次JSON结构
            Object value = jsonContext.read(jsonPathKey);
            if (value instanceof JSONArray){
                valueList = ((List<Object>) value).stream()
                        .collect(Collectors.toList());
                updateBody.put(jsonPathKey, valueList);
            } else {
                throw new SeaTunnelException("Value for key " + jsonPathKey + " is not an Array. 请检查您的json数据，可能是由于您的数组被双引号包裹起来了");
            }
        }
        //根据相同索引拼接每个参数体
        List<String> result = new ArrayList<>();
        if(CollectionUtils.isEmpty(valueList)){
            result.add(jsonContext.jsonString());
        }
        else {
            int initSize = updateBody.get(bodyPListKey.get(0)).size();
            for (int i = 0; i < valueList.size(); i++) {
                DocumentContext singleUpdateBodyNode = JsonPath.using(Configuration.defaultConfiguration()).parse(jsonContext.jsonString());

                for (String jsonPathKey : bodyPListKey) {
                    String[] keys = jsonPathKey.split("\\.");
                    String rightKey = keys[keys.length - 1];
                    String leftKey = jsonPathKey.substring(0, jsonPathKey.length() - rightKey.length() - 1);
                    int size = updateBody.get(jsonPathKey).size();
                    if (size != initSize) {
                        throw new SeaTunnelException("传入的数组大小不匹配，请检查配置");
                    }
                    singleUpdateBodyNode.delete(jsonPathKey);
                    singleUpdateBodyNode.put(leftKey, rightKey, updateBody.get(jsonPathKey).get(i));
                }
                result.add(singleUpdateBodyNode.jsonString());
            }
        }
        //循环执行
        if (CollectionUtils.isEmpty(result)) {
            pageIndex = 1L;
            executePageRequest(output, params, url, null,headers);
        } else {
            for (String s : result) {
                pageIndex = 1L;
                executePageRequest(output, params, url, s,headers);
            }
        }



    }

    private void updateListParams(Collector<SeaTunnelRow> output, String url, Map<String, String> params, String body,List<String> paramsListKey,Map<String, String> headers) throws Exception {
        Map<String, List<String>> updateParams = new HashMap();
        //拿到每个列表参数的value
        List<String> valueList = new ArrayList();
        for (String key : paramsListKey) {
            String value = params.get(key);
            valueList = Arrays.asList(value.substring(1, value.length() - 1).split(","));
            updateParams.put(key, valueList);
        }
        //根据相同索引拼接每个参数体 //
        List<Map<String, String>> result = new ArrayList();
        if(Optional.ofNullable(valueList).map(List::isEmpty).orElse(true)){
            result.add(params);
        }else {
            int InitSize = updateParams.get(paramsListKey.get(0)).size();
            for (int i = 0; i < valueList.size(); i++) {
                Map<String, String> SingleUpdateParams = new HashMap(params);
                for (String key : paramsListKey) {
                    int size = updateParams.get(key).size();
                    if (size != InitSize) {
                        throw new SeaTunnelException("传入的数组参数大小不匹配，请检查配置");
                    }
                    SingleUpdateParams.put(key, updateParams.get(key).get(i));
                }
                result.add(SingleUpdateParams);
            }
        }
        //循环执行
        if (CollectionUtils.isEmpty(result)) {
            pageIndex = 1L;
            executePageRequest(output, null, url, body,headers);
        } else {
            String finalBody = body;
            result.forEach(p -> {
                try {
                    pageIndex = 1L;
                    executePageRequest(output, p, url, finalBody,headers);
                } catch (Exception e) {
                    throw new SeaTunnelException(e);
                }
            });
        }
    }

    public void executePageRequest(Collector<SeaTunnelRow> output, Map<String, String> params, String url, String body,Map<String, String> headers)   {
        try {
            //TODO page分页判断
            String pageBody = body;
            Map<String, String> pageParams = params;
            if (pageInfoOptional.isPresent()) {
                noMoreElementFlag = false;
                while (!noMoreElementFlag) {
                    PageInfo info = pageInfoOptional.get();
                    // increment page
                    info.setPageIndex(pageIndex);

                    if("body".equals(info.getPagePath())){
                        // set request body
                        pageBody = updateBodyPage(info,body);
                    }else {
                        // set request param
                        pageParams = updateParamPage(info, params);
                    }
                    try {
                        executeRequest(output, pageParams, url, pageBody,headers);
                    }catch (Exception e){
                        noMoreElementFlag = true;
                        throw new SeaTunnelException("http requests error",e );
                    }
                    pageIndex += 1;
                }
            } else {
                executeRequest(output, pageParams, url, pageBody,headers);
            }
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }
    private Map<String, String> paramsConvertTime(List<String> convert, Map<String, String> map ) throws Exception{
        if (convert == null) {
            return map;
        }
        convert.stream()
                //只对存在于params map中的key进行操作
                .filter(map::containsKey)
                .forEach(key -> {
                    String timeStr = map.get(key); //获取对应key的时间字符串
                    //将时间字符串转换为时间戳
                    LocalDateTime localDateTime;
                    try {
                        //尝试使用格式带毫秒的格式解析日期时间字符串
                        localDateTime = TimeUtils.parseDateTime(timeStr, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);
                    } catch (Exception e1) {
                        //如果失败，尝试使用不带毫秒的格式解析日期时间字符串
                        localDateTime = TimeUtils.parseDateTime(timeStr, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
                    }
                    long timeStamp = TimeUtils.toEpochSecond(localDateTime);
                    //将转换的时间戳值放回params map
                    map.put(key, String.valueOf(timeStamp));
                });
        Map<String, String> result = new HashMap<>(map);
        return result;
    }


    private String BodyConvertTime(List<String> convert, String contentJson) throws Exception {
        if (convert == null || convert.isEmpty()) {
            return contentJson;
        }
        JsonNode rootNode = JsonUtils.parseObject(contentJson);
        // 使用递归函数检查和更新节点
        checkAndUpdateNodeTime(rootNode, convert);
        // 将更新后的JsonNode转回为字符串
        contentJson = JsonUtils.toJsonString(rootNode);
        return contentJson;
    }

    private Map<String, String> headerConvertTime(List<String> convert, Map<String, String> map ) throws Exception{
        if (convert == null) {
            return map;
        }
        convert.stream()
                //只对存在于params map中的key进行操作
                .filter(map::containsKey)
                .forEach(key -> {
                    String timeStr = map.get(key); //获取对应key的时间字符串
                    LocalDateTime localDateTime;
                    try {
                        //尝试使用格式带毫秒的格式解析日期时间字符串
                        localDateTime = TimeUtils.parseDateTime(timeStr, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);
                    } catch (Exception e1) {
                        //如果失败，尝试使用不带毫秒的格式解析日期时间字符串
                        localDateTime = TimeUtils.parseDateTime(timeStr, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
                    }

                    long timeStamp = TimeUtils.toEpochMill(localDateTime);
                    //将转换的时间戳值放回params map
                    map.put(key, String.valueOf(timeStamp));
                });
        Map<String, String> result = new HashMap<>(map);
        return result;
    }

    private  String addQuotesToJsonValues(String json) {
        // Regex to find all values in square brackets ([]), that are not already surrounded by quotes
        String regex = "\\[(.*?)\\]";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(json);
        StringBuffer jsonBuffer = new StringBuffer();

        while (matcher.find()) {
            String group = matcher.group(1);
            // Regex to find all individual values that are not already surrounded by quotes
            String subRegex = "([^,]*)";
            Pattern subPattern = Pattern.compile(subRegex);
            Matcher subMatcher = subPattern.matcher(group);
            StringBuffer groupBuffer = new StringBuffer();
            while (subMatcher.find()) {
                String replacement = subMatcher.group(1);
                // add quotes around the value
                String replacement2 = new String("\"" + replacement + "\"");
                subMatcher.appendReplacement(groupBuffer, replacement2);
            }
            subMatcher.appendTail(groupBuffer);
            // append the result to the original string
            matcher.appendReplacement(jsonBuffer, groupBuffer.toString());
        }

        matcher.appendTail(jsonBuffer);
        return jsonBuffer.toString();
    }

    private  String replaceBody(String json) {
        if (StringUtils.isNotBlank(json)) {
            return json.replace("#+", "\"");
        }else {
            return json;
        }
    }

    private void checkAndUpdateNodeTime(JsonNode node, List<String> keys) throws Exception{

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();

            if (entry.getValue().isObject()) {
                checkAndUpdateNodeTime(entry.getValue(), keys);
            } else {
                if (keys.contains(entry.getKey())) {
                    String timeString = entry.getValue().asText();
                    LocalDateTime localDateTime;
                    try {
                        //尝试使用格式带毫秒的格式解析日期时间字符串
                        localDateTime = TimeUtils.parseDateTime(timeString, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS_SSS);
                    } catch (Exception e1) {
                        //如果失败，尝试使用不带毫秒的格式解析日期时间字符串
                        localDateTime = TimeUtils.parseDateTime(timeString, TimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS);
                    }
                    long timeStamp = TimeUtils.toEpochSecond(localDateTime);
                    // 此处需要强转为ObjectNode以调用put方法
                    ((ObjectNode) node).put(entry.getKey(), timeStamp);
                }
            }
        }
    }
    private void checkUrl(Collector<SeaTunnelRow> output,String url,Map<String, String> params,String body,Map<String, String> headers) throws Exception {
        if(!hasListUrl){
            pageIndex = 1L;
            executePageRequest(output,params ,url,body,headers);
        } else {
            //url存在list标识则循环调用
            String leftUrl = url.split("\\[")[0];
            String[] pathParamsList = url.split("\\[")[1]
                    .replace("]", "")
                    .split(",");
            for (String pathParams : pathParamsList) {
                String modifiedUrl = leftUrl + pathParams.trim();
                pageIndex = 1L;
                executePageRequest(output, params,modifiedUrl,body,headers);
            }
        }
    }
    /**
     * 执行HTTP请求 并将结果写入Collector
     * @param output
     * @param params
     * @throws Exception
     */
    private void executeRequest(Collector<SeaTunnelRow> output,Map<String, String> params,String url,String body,Map<String, String> headers) throws Exception {
        //加密拦截
        EncryptStrategyFactory factory = new DefaultEncryptStrategyFactory();
        EncryptHandler handler = new EncryptHandler(factory);
        Map<String, String> encryptParams = handler.handleParams(params, this.httpParameter.getParamsEncrypt());
        String encryptBody = handler.handleBody(body, this.httpParameter.getBodyEncrypt(),params);
        Map<String, String> encryptHeaders = handler.handleHeader(headers, this.httpParameter.getHeadersEncrypt(), encryptBody);

        HttpResponse response;
        //其他sdk处理
        Map<String, String> otherSdk = new HashMap<>();
        Map<String, String> hikvisionApi = this.httpParameter.getHikvisionApi();
        Map<String, String> sangForApi = this.httpParameter.getSangForApi();
        if (MapUtils.isNotEmpty(sangForApi)){
            String authCode = sangForApi.get(AUTHCODE);
            if (StringUtils.isNotBlank(authCode)){
                otherSdk.put(SDK, SANGFOR);
                otherSdk.put(AUTHCODE,authCode);
            }
        }
        //实际执行
        if (MapUtils.isNotEmpty(hikvisionApi)){
            response = handleHikvisionApi(url,
                    encryptBody,
                    encryptParams,
                    null,
                    APPLICATION_JSON,
                    encryptHeaders,
                    hikvisionApi);
        } else {
            response = httpClient.execute(
                    url,
                    this.httpParameter.getMethod().getMethod(),
                    encryptHeaders,
                    encryptParams,
                    encryptBody,
                    otherSdk);
        }

        //状态拦截
        if (HttpResponse.STATUS_OK == response.getCode() || HttpResponse.STATUS_CREATED == response.getCode()) {
            String content = "";
            boolean paramsAdd = this.httpParameter.isParamsAdd();
            String paramsPath = this.httpParameter.getParamsPath();
            boolean cookiesAdd = this.httpParameter.isCookiesAdd();
            String cookiesPath = this.httpParameter.getCookiesPath();
            boolean bodyAdd = this.httpParameter.isBodyAdd();
            String bodyPath = this.httpParameter.getBodyPath();
            boolean headersAdd = this.httpParameter.isHeadersAdd();
            String headersPath = this.httpParameter.getHeadersPath();
            String responseString = response.getContent();

            if (MapUtils.isNotEmpty(this.httpParameter.getParamsEncrypt())&& "RSA".equals(this.httpParameter.getParamsEncrypt().get("encrypt_type"))){
                String privateKey = this.httpParameter.getParamsEncrypt().get("privateKey");
                Map<String, String> contentMap = JsonUtils.toMap(responseString);
                String data = contentMap.get("data");
                responseString =  EncryptUtil.decryptByPrivateKey(data, privateKey);
                if (StringUtils.isBlank(responseString)){
                    log.error("http client execute exception, request url:[{}], request headers:[{}], request param:[{}], request body:[{}],http response status code:[{}], content:[{}]",
                            url,
                            encryptHeaders,
                            encryptParams,
                            encryptBody,
                            response.getCode(),
                            responseString);
                    throw new SeaTunnelException("未正确获取到RSA加密数据");
                }
            }
            DocumentContext context = JsonPath.parse(responseString);

            //params追加
            JsonNode paramsJsonNode = JsonUtils.toJsonNode(params);
            content = processAddition(content, context, paramsAdd, paramsJsonNode, paramsPath, "params");
            //headers追加
            JsonNode headersJsonNode = JsonUtils.toJsonNode(headers);
            content =  processAddition(content, context, headersAdd, headersJsonNode, headersPath, "headers");
            //body追加

            JsonNode bodyJsonNode = JsonUtils.stringToJsonNode(StringUtils.isBlank(body)?"":body);
            content = processAddition(content, context, bodyAdd, bodyJsonNode, bodyPath, "body");
//            //TODO 去除数组影响,可能影响性能
//            content = content.replace("\"[","[")
//                    .replace("]\"","]")
//                    .replace("\\\"","\"");
            //cookies追加
            String cookies = response.getCookies();
            content =  processAdditionCookies(content, context, cookiesAdd, cookies, cookiesPath);

            if (StringUtils.isBlank(content)){content = responseString;}

            if (!Strings.isNullOrEmpty(content)) {
                if (this.httpParameter.isEnableMultilines()) {
                    StringReader stringReader = new StringReader(content);
                    BufferedReader bufferedReader = new BufferedReader(stringReader);
                    String lineStr;
                    while ((lineStr = bufferedReader.readLine()) != null) {
                        collect(output, lineStr);
                    }
                } else {
                    collect(output, content);
                }
            }
            if (content.length() > 500) {
                StringBuilder sb = new StringBuilder(content);
                if (sb.length() > 500) {
                    sb.setLength(500);
                    sb.append("......");
                }
                content = sb.toString();
            }
            log.info("http client execute success request url:[{}], request headers:[{}] ,request param:[{}], request body:[{}], http response status code:[{}], content:[{}]",
                    url,
                    encryptHeaders,
                    encryptParams,
                    encryptBody,
                    response.getCode(),
                    content);
            log.debug(
                    "http client execute success request url:[{}], request headers:[{}], request param:[{}],request body:[{}], http response status code:[{}], content:[{}]",
                    url,
                    encryptHeaders,
                    encryptParams,
                    encryptBody,
                    response.getCode(),
                    content);
        } else {
            noMoreElementFlag = true;
            log.error("http client execute exception, request url:[{}], request headers:[{}], request param:[{}], request body:[{}],http response status code:[{}], content:[{}]",
                    url,
                    encryptHeaders,
                    encryptParams,
                    encryptBody,
                    response.getCode(),
                    response.getContent());
        }
    }


    /**
     * 拦截深信服sdk
     */
    private HttpResponse processSangfor(String content, DocumentContext context, boolean isAdd, JsonNode dataJsonNode, String dataPath, String prefix
    ) throws Exception {
        SigSignerJavaImpl sigSignerJava = new SigSignerJavaImpl("35623565623538362D643234302D346339372D393532342D3335333266643761666435397C7C7C73616E67666F727C76317C3132372E302E302E317C7C7C7C33323239424638333141414146384542423930384542343344383141343033454344393643323132443530324145353231443643304637314137413030423642303545413934303242463838314246454636383346304641323135444234434138303737423834324439454642383230313833454532434445394337363730367C36363834384345354234363033423243424239323434394534443742313341303233323330434342413543433232313146323135333333464545313545393641433534383242383542353030333339314435373037413144433946343645364639313032423444364637444443434146434534453045443636424431303837327C7C307C");
        return null;
    }

    /**
     * 将参数追加到响应体
     * @param content
     * @param context
     * @param isAdd
     * @param dataJsonNode
     * @param dataPath
     * @param prefix
     * @throws JsonProcessingException
     */

    private String processAddition(String content, DocumentContext context, boolean isAdd, JsonNode dataJsonNode, String dataPath, String prefix) throws JsonProcessingException {
        if (isAdd && !dataJsonNode.isNull() && !dataJsonNode.isEmpty()) {
            handleJsonNode(dataPath, context, prefix, dataJsonNode);
            content = context.jsonString();
        }
        return content;
    }
    void handleJsonNode(String parentPath, DocumentContext context, String prefix, JsonNode jsonNode) {
            Iterator<Map.Entry<String, JsonNode>> fields = jsonNode.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> entry = fields.next();
                String modifiedKey = prefix + "_" + entry.getKey();
                String newPath = parentPath;  // 连接父路径和当前键名
                JsonNode value = entry.getValue();

                if (value.isObject()) {
                    context.put(newPath,modifiedKey, new HashMap<>());
                    newPath =  newPath + "." + modifiedKey;
                    handleJsonNode(newPath, context, prefix, value);
                } else if (value.isArray()){
                    context.put(newPath,modifiedKey, value.toString());
                }else {
                    context.put(newPath,modifiedKey, value.asText());
                }
            }
    }

    /**
     * 将cookies追加到响应体
     * @param content
     * @param context
     * @param isAdd
     * @param cookies
     * @param dataPath
     * @throws JsonProcessingException
     */
    private String processAdditionCookies(String content, DocumentContext context, boolean isAdd, String cookies, String dataPath) throws JsonProcessingException {
        if (isAdd && StringUtils.isNotBlank(cookies)) {
            JsonPath jsonPathExpr = JsonPath.compile(dataPath);

            String[] splitCookies = cookies.split("=");
            String[] keys = {"cookies_key", "cookies_value"};

            for (int i = 0; i < splitCookies.length; i++) {
                String value = splitCookies[i];
                context.put(jsonPathExpr, keys[i], value);
            }

            content = context.jsonString();
        }
        return content;
    }
    private Map<String, String> updateParamPage(PageInfo pageInfo, Map<String, String> params) {
        Map<String, String> pageParams = params != null ? params : new HashMap<>();
        pageParams.put(pageInfo.getPageField(), String.valueOf(pageInfo.getPageIndex()));
        return pageParams;
    }

    private String updateBodyPage(PageInfo pageInfo,String body) throws Exception {
        String pageBody = null;
        String replaceBody = replaceBody(body);
        if (body == null) {
            return pageBody;
        } else {
            try {
                JsonNode bodyNode = JsonUtils.parseObject(replaceBody);
                String pageField = pageInfo.getPageField();
                Long index = pageInfo.getPageIndex();
                checkAndUpdateNodePage(bodyNode,pageField,index);
                pageBody = JsonUtils.toJsonString(bodyNode);
            } catch (Exception e){
                noMoreElementFlag = true;
                throw new SeaTunnelException("body转换失败，请保证body是正常的json字符串",e);
            }
        }
        return pageBody;
    }
    private void checkAndUpdateNodePage(JsonNode node, String pageField, Long index) throws Exception{

        Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> entry = fields.next();

            if (entry.getValue().isObject()) {
                checkAndUpdateNodePage(entry.getValue(), pageField,index);
            } else {
                if (pageField.equals(entry.getKey())) {
                    // 此处需要强转为ObjectNode以调用put方法
                    ((ObjectNode) node).put(entry.getKey(), index);
                }
            }
        }
    }


    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            pollAndCollectData(output);
//            if (pageInfoOptional.isPresent()) {
//                noMoreElementFlag = false;
//                while (!noMoreElementFlag) {
//                    PageInfo info = pageInfoOptional.get();
//                    // increment page
//                    info.setPageIndex(pageIndex);
//                    if("body".equals(info.getPagePath())){
//                        // set request body
//                        updateBodyPage(info);
//                    }else {
//                        // set request param
//                      updateParamPage(info);
//                    }
//                    try {
//                        pollAndCollectData(output);
//                    }catch (Exception e){
//                        noMoreElementFlag = true;
//                        throw new SeaTunnelException("http requests error",e );
//                    }
//                    pageIndex += 1;
//                }
//            } else {
//                pollAndCollectData(output);
//            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (Boundedness.BOUNDED.equals(context.getBoundedness()) && noMoreElementFlag) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded http source");
                context.signalNoMoreElement();
            } else {
                if (httpParameter.getPollIntervalMillis() > 0) {
                    Thread.sleep(httpParameter.getPollIntervalMillis());
                }
            }
        }
    }

    private void collect(Collector<SeaTunnelRow> output, String data) throws IOException {
        if (contentJson != null) {
            data = JsonUtils.stringToJsonNode(getPartOfJson(data)).toString();
        }
        if (jsonField != null) {
            this.initJsonPath(jsonField);
            data = JsonUtils.toJsonNode(parseToMap(decodeJSON(data), jsonField)).toString();
        }
        // page increase
        if (pageInfoOptional.isPresent()) {
            // Determine whether the task is completed by specifying the presence of the 'total
            // page' field
            PageInfo pageInfo = pageInfoOptional.get();
            if (pageInfo.getTotalPageSize() > 0) {
                noMoreElementFlag = pageInfo.getPageIndex() >= pageInfo.getTotalPageSize();
            } else {
                // no 'total page' configured
                int readSize = JsonUtils.stringToJsonNode(data).size();
                // if read size < BatchSize : read finish
                // if read size = BatchSize : read next page.
                noMoreElementFlag = readSize < pageInfo.getBatchSize();
            }
        }
        if (StringUtils.isNotBlank(data)){
            deserializationCollector.collect(data.getBytes(), output);
        }
    }

    private List<Map<String, String>> parseToMap(List<List<String>> datas, JsonField jsonField) {

        List<Map<String, String>> decodeDatas = new ArrayList<>(datas.size());
        if (Optional.ofNullable(datas).map(List::isEmpty).orElse(true)) {
            return decodeDatas;
        }
        String[] keys = jsonField.getFields().keySet().toArray(new String[] {});

        for (List<String> data : datas) {
            Map<String, String> decodeData = new HashMap<>(jsonField.getFields().size());
            final int[] index = {0};
            data.forEach(
                    field -> {
                        decodeData.put(keys[index[0]], field);
                        index[0]++;
                    });
            decodeDatas.add(decodeData);
        }

        return decodeDatas;
    }

    private List<List<String>> decodeJSON(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        List<List<String>> results = new ArrayList<>(jsonPaths.length);
        HashMap<Integer, Integer> resultMaps = new HashMap<>();
        for (JsonPath jsonPath : jsonPaths) {
            List<String> result = jsonReadContext.read(jsonPath);
            List<String> resultStrings = new ArrayList<>();

            for (Object obj : result) {
                if (obj == null) {
                    resultStrings.add(null);
                } else if (obj instanceof String) {
                    resultStrings.add((String) obj);
                } else {
                    resultStrings.add(JsonUtils.toJsonString(obj));
                }
            }
            //TODO 测试jsonpath解析失败场景
            results.add(resultStrings);
            //记录每个jsonpath的长度，相同长度的只有一个存在,但是0不能放进去
            String[] split = jsonPath.getPath().toString().split("\\]\\[");
            int length = split.length;
            //判断是当前jsonpath否为数组路径
//            if (length >= 2){
//                //拿到倒数第二个数组元素
//                String secondLastDot = split[split.length - 2];
//                boolean isArrayPath =  secondLastDot.endsWith("]");
//                //TODO
//                if (!isArrayPath){
//                    System.out.println("----------------------");
//                }
//            }

            if (resultStrings.size() >= 1 && null != resultStrings){
                resultMaps.put(result.size(), length);
            }

        }
        boolean allEmpty = results.stream()
                .allMatch(List::isEmpty);
        // 如果所有的子列表都是空的，返回空集合
        if (allEmpty){return Collections.emptyList();}
        //如果数据长度一样则无须复制数据，直接返回
        if (resultMaps.size() == 1){
            return dataFlip2(results);
        } else {
            //如果数据长度一样，那么按照大小相乘计算复制的倍数
            List<List<String>> copyResults = copyResults(results, resultMaps);
            return dataFlip2(copyResults);
        }
    }

    private List<List<String>> copyResults(List<List<String>> results,Map<Integer, Integer> resultMaps) {

        final int[] totalSize = {1};
        resultMaps.forEach((key, value) -> totalSize[0] *= key);
        int initSize = 1;
        for (List<String> result : results) {
            initSize = result.size()==0?1:result.size();
            //每个结果的实际复制次数
            int repeatTimes = totalSize[0] / initSize;
            //当result的大小，小于总大小时，复制其结果
            while (result.size() < totalSize[0]) {
                if (initSize>1){
                    List<String> initResult = new ArrayList<>(result.size());
                    for (String element : result) {
                        if (element != null) {
                            initResult.add(new String(element));
                        } else {
                            initResult.add(null);
                        }
                    }
                    for (String element : initResult) {
                        int index = result.indexOf(element);
                        for (int i = 1; i < repeatTimes; i++) {
                            result.add(index + 1,element);
                        }
                    }
                }else{
                    String firstResult = null;
                    try {
                        firstResult = result.get(0);
                        result.add(firstResult);
                    }catch (Exception e){
                        result.add(null);
                    }

                }
            }

        }
        return  results;
    }
    private String getPartOfJson(String data) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        return JsonUtils.toJsonString(jsonReadContext.read(JsonPath.compile(contentJson)));
    }

    private List<List<String>> dataFlip(List<List<String>> results) {


        List<List<String>> datas = new ArrayList<>();
        if (results.isEmpty()) {
            return datas;
        }
        // TODO 如果results前三个没有数据，最后一个有数据，也就是pagetotal=0，那么datas.get(j)报错
        for (int i = 0; i < results.size(); i++) {
            List<String> result = results.get(i);
            if (i == 0) {
                for (Object o : result) {
                    String val = o == null ? null : o.toString();
                    List<String> row = new ArrayList<>(jsonPaths.length);
                    row.add(val);
                    datas.add(row);
                }
            } else {
                for (int j = 0; j < result.size(); j++) {
                    Object o = result.get(j);
                    String val = o == null ? null : o.toString();
                    List<String> row = datas.get(j);
                    row.add(val);
                    datas.add(row);
                }
            }
        }
        return datas;
    }


    /**
     * 二维列表转置
     * @param results
     * @return List<List<String>>
     */
    private List<List<String>> dataFlip2(List<List<String>> results) {
        List<List<String>> datas = new ArrayList<>();
        if (results.isEmpty()) {
            return datas;
        }
        int maxRowSize = results.stream().mapToInt(List::size).max().orElse(0);
        for (int i = 0; i < maxRowSize; i++) {
            List<String> newRow = new ArrayList<>();
            for (List<String> result : results) {
                String val = (i < result.size()) ? result.get(i) : null;
                newRow.add(val);
            }
            datas.add(newRow);
        }
        return datas;
    }

    private void initJsonPath(JsonField jsonField) {
        jsonPaths = new JsonPath[jsonField.getFields().size()];
        for (int index = 0; index < jsonField.getFields().keySet().size(); index++) {
            jsonPaths[index] =
                    JsonPath.compile(
                            jsonField.getFields().values().toArray(new String[] {})[index]);
        }
    }
}
