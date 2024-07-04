/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.format.json;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.json.JsonReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.NullNode;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.CompositeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.format.json.exception.SeaTunnelJsonFormatException;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.jayway.jsonpath.ReadContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonDeserializationSchema implements DeserializationSchema<SeaTunnelRow> {
    private static final long serialVersionUID = 1L;

    private static final String FORMAT = "Common";

    /** Flag indicating whether to fail if a field is missing. */
    private final boolean failOnMissingField;

    /** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
    private final boolean ignoreParseErrors;

    /** The row type of the produced {@link SeaTunnelRow}. */
    private final SeaTunnelRowType rowType;

    /**
     * Runtime converter that converts {@link JsonNode}s into objects of internal data structures.
     */
    private JsonToRowConverters.JsonToObjectConverter runtimeConverter;

    /** Object mapper for parsing the JSON. */
    private final ObjectMapper objectMapper = new ObjectMapper();

    private CatalogTable catalogTable;

    private JsonPath[] jsonPaths;

    public JsonDeserializationSchema(
            boolean failOnMissingField, boolean ignoreParseErrors, SeaTunnelRowType rowType) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.rowType = checkNotNull(rowType);
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter =
                new JsonToRowConverters(failOnMissingField, ignoreParseErrors)
                        .createRowConverter(checkNotNull(rowType));

        if (hasDecimalType(rowType)) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    }

    public JsonDeserializationSchema(
            CatalogTable catalogTable, boolean failOnMissingField, boolean ignoreParseErrors) {
        if (ignoreParseErrors && failOnMissingField) {
            throw new SeaTunnelJsonFormatException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "JSON format doesn't support failOnMissingField and ignoreParseErrors are both enabled.");
        }
        this.catalogTable = catalogTable;
        this.rowType = checkNotNull(catalogTable.getSeaTunnelRowType());
        this.failOnMissingField = failOnMissingField;
        this.ignoreParseErrors = ignoreParseErrors;
        this.runtimeConverter =
                new JsonToRowConverters(failOnMissingField, ignoreParseErrors)
                        .createRowConverter(checkNotNull(rowType));

        if (hasDecimalType(rowType)) {
            objectMapper.enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
        }
        objectMapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true);
    }

    private static boolean hasDecimalType(SeaTunnelDataType<?> dataType) {
        if (dataType.getSqlType() == SqlType.DECIMAL) {
            return true;
        }
        if (dataType instanceof CompositeType) {
            CompositeType<?> compositeType = (CompositeType<?>) dataType;
            for (SeaTunnelDataType<?> child : compositeType.getChildren()) {
                if (hasDecimalType(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] message) throws IOException {
        if (message == null) {
            return null;
        }
        return convertJsonNode(convertBytes(message));
    }

    public SeaTunnelRow deserialize(String message) throws IOException {
        if (message == null) {
            return null;
        }
        return convertJsonNode(convert(message));
    }

    public void collect(byte[] message, Collector<SeaTunnelRow> out) throws IOException {
        JsonNode jsonNode = convertBytes(message);
        if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                SeaTunnelRow deserialize = convertJsonNode(arrayNode.get(i));
                setCollectorTablePath(deserialize, catalogTable);
                out.collect(deserialize);
            }
        } else {
            SeaTunnelRow deserialize = convertJsonNode(jsonNode);
            setCollectorTablePath(deserialize, catalogTable);
            out.collect(deserialize);
        }
    }

    public void collect(
            byte[] message, Collector<SeaTunnelRow> out, JsonField jsonField, String contentJson)
            throws IOException {
        JsonNode jsonNode = convertBytes(message);
        if (jsonNode.isNull()) {
            collect(message, out);
        }
        String data = JsonUtils.toJsonString(jsonNode);
        Configuration jsonConfiguration =
                Configuration.defaultConfiguration()
                        .addOptions(
                                Option.SUPPRESS_EXCEPTIONS,
                                Option.ALWAYS_RETURN_LIST,
                                Option.DEFAULT_PATH_LEAF_TO_NULL);
        if (contentJson != null) {
            data =
                    JsonUtils.stringToJsonNode(getPartOfJson(data, contentJson, jsonConfiguration))
                            .toString();
        }
        if (jsonField != null && contentJson == null) {
            this.initJsonPath(jsonField);
            data =
                    JsonUtils.toJsonNode(parseToMap(decodeJSON(data, jsonConfiguration), jsonField))
                            .toString();
        }
        collect(data.getBytes(), out);
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

    private List<List<String>> decodeJSON(String data, Configuration jsonConfiguration) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        List<List<String>> results = new ArrayList<>(jsonPaths.length);
        HashMap<Integer, Integer> resultMaps = new HashMap<>();
        for (JsonPath jsonPath : jsonPaths) {
            List<Object> result = jsonReadContext.read(jsonPath);

            // 深拷贝
            List<String> resultStrings =
                    result.stream()
                            .map(
                                    obj ->
                                            obj == null
                                                    ? null
                                                    : obj instanceof String
                                                            ? obj.toString()
                                                            : JsonUtils.toJsonString(obj))
                            .collect(Collectors.toList());

            // TODO 测试jsonpath解析失败场景
            results.add(resultStrings);
            // 记录每个jsonpath的长度，相同长度的只有一个存在,但是0不能放进去
            String[] split = jsonPath.getPath().split("\\]\\[");
            int length = split.length;

            if (!resultStrings.isEmpty()) {
                resultMaps.put(result.size(), length);
            }
        }
        boolean allEmpty = results.stream().allMatch(List::isEmpty);
        // 如果所有的子列表都是空的，返回空集合
        if (allEmpty) {
            return Collections.emptyList();
        }
        // 如果数据长度一样则无须复制数据，直接返回
        if (resultMaps.size() == 1) {
            return dataFlip(results);
        } else {
            // 如果数据长度不一样，那么按照大小相乘计算复制的倍数
            List<List<String>> copyResults = copyResults(results, resultMaps);
            return dataFlip(copyResults);
        }
    }

    private List<List<String>> copyResults(
            List<List<String>> results, Map<Integer, Integer> resultMaps) {

        final int[] totalSize = {1};
        resultMaps.forEach((key, value) -> totalSize[0] *= key);
        int initSize = 1;
        for (List<String> result : results) {
            initSize = result.isEmpty() ? 1 : result.size();
            // 每个结果的实际复制次数
            int repeatTimes = totalSize[0] / initSize;
            // 当result的大小，小于总大小时，复制其结果
            while (result.size() < totalSize[0]) {
                if (initSize > 1) {
                    // 伪深拷贝
                    List<String> initResult = new ArrayList<>(result.size());
                    for (String element : result) {
                        if (element != null) {
                            initResult.add(element);
                        } else {
                            initResult.add(null);
                        }
                    }
                    for (String element : initResult) {
                        int index = result.indexOf(element);
                        for (int i = 1; i < repeatTimes; i++) {
                            result.add(index + 1, element);
                        }
                    }
                } else {
                    String firstResult;
                    try {
                        firstResult = result.get(0);
                        result.add(firstResult);
                    } catch (Exception e) {
                        result.add(null);
                    }
                }
            }
        }
        return results;
    }

    private String getPartOfJson(String data, String contentJson, Configuration jsonConfiguration) {
        ReadContext jsonReadContext = JsonPath.using(jsonConfiguration).parse(data);
        return JsonUtils.toJsonString(jsonReadContext.read(JsonPath.compile(contentJson)));
    }

    /**
     * 二维列表转置
     *
     * @param results
     * @return List<List<String>>
     */
    private List<List<String>> dataFlip(List<List<String>> results) {
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

    public void setCollectorTablePath(SeaTunnelRow deserialize, CatalogTable catalogTable) {
        Optional<TablePath> tablePath =
                Optional.ofNullable(catalogTable).map(CatalogTable::getTablePath);
        if (tablePath.isPresent()) {
            deserialize.setTableId(tablePath.toString());
        }
    }

    private SeaTunnelRow convertJsonNode(JsonNode jsonNode) {
        if (jsonNode.isNull()) {
            return null;
        }
        try {
            return (SeaTunnelRow) runtimeConverter.convert(jsonNode, null);
        } catch (RuntimeException e) {
            if (ignoreParseErrors) {
                return null;
            }
            throw CommonError.jsonOperationError(FORMAT, jsonNode.toString(), e);
        }
    }

    public JsonNode deserializeToJsonNode(byte[] message) throws IOException {
        return objectMapper.readTree(message);
    }

    public SeaTunnelRow convertToRowData(JsonNode message) {
        return (SeaTunnelRow) runtimeConverter.convert(message, null);
    }

    private JsonNode convertBytes(byte[] message) {
        try {
            return objectMapper.readTree(message);
        } catch (IOException | RuntimeException e) {
            if (ignoreParseErrors) {
                return NullNode.getInstance();
            }
            throw CommonError.jsonOperationError(FORMAT, new String(message), e);
        }
    }

    private JsonNode convert(String message) {
        try {
            return objectMapper.readTree(message);
        } catch (JsonProcessingException | RuntimeException e) {
            if (ignoreParseErrors) {
                return NullNode.getInstance();
            }
            throw CommonError.jsonOperationError(FORMAT, new String(message), e);
        }
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowType;
    }
}
