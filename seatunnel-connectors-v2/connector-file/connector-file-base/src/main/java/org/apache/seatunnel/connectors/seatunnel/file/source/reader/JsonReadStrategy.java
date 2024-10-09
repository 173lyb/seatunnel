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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import io.airlift.compress.lzo.LzopCodec;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.format.json.JsonField;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class JsonReadStrategy extends AbstractReadStrategy {
    private DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private CompressFormat compressFormat = BaseSourceConfigOptions.COMPRESS_CODEC.defaultValue();
    private String encoding = BaseSourceConfigOptions.ENCODING.defaultValue();
    private  JsonField jsonField;
    private  String contentJson;
    @Override
    public void init(HadoopConf conf) {
        super.init(conf);
        if (pluginConfig.hasPath(BaseSourceConfigOptions.COMPRESS_CODEC.key())) {
            String compressCodec =
                    pluginConfig.getString(BaseSourceConfigOptions.COMPRESS_CODEC.key());
            compressFormat = CompressFormat.valueOf(compressCodec.toUpperCase());
        }
        encoding =
                ReadonlyConfig.fromConfig(pluginConfig)
                        .getOptional(BaseSourceConfigOptions.ENCODING)
                        .orElse(StandardCharsets.UTF_8.name());
        jsonField = (JsonField) pluginConfig.getObject("json_field");
        contentJson = pluginConfig.getString("content_field");
    }

    @Override
    public void setSeaTunnelRowTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        super.setSeaTunnelRowTypeInfo(seaTunnelRowType);
        if (isMergePartition) {
            deserializationSchema =
                    new JsonDeserializationSchema(false, false, this.seaTunnelRowTypeWithPartition);
        } else {
            deserializationSchema =
                    new JsonDeserializationSchema(false, false, this.seaTunnelRowType);
        }
    }

    @Override
    public void read(String path, String tableId, Collector<SeaTunnelRow> output)
            throws FileConnectorException, IOException {
        Map<String, String> partitionsMap = parsePartitionsByPath(path);
        InputStream inputStream;
        switch (compressFormat) {
            case LZO:
                LzopCodec lzo = new LzopCodec();
                inputStream = lzo.createInputStream(hadoopFileSystemProxy.getInputStream(path));
                break;
            case NONE:
                inputStream = hadoopFileSystemProxy.getInputStream(path);
                break;
            default:
                log.warn(
                        "Text file does not support this compress type: {}",
                        compressFormat.getCompressCodec());
                inputStream = hadoopFileSystemProxy.getInputStream(path);
                break;
        }
        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(inputStream, encoding))) {
            reader.lines()
                    .forEach(
                            line -> {
                                try {
                                    SeaTunnelRow seaTunnelRow =
                                            deserializationSchema.deserialize(
                                                    line.getBytes(StandardCharsets.UTF_8));
                                    if (isMergePartition) {
                                        int index = seaTunnelRowType.getTotalFields();
                                        for (String value : partitionsMap.values()) {
                                            seaTunnelRow.setField(index++, value);
                                        }
                                    }
                                    seaTunnelRow.setTableId(tableId);
                                    output.collect(seaTunnelRow);
                                } catch (IOException e) {
                                    throw CommonError.fileOperationFailed(
                                            "JsonFile", "read", path, e);
                                }
                            });
        } catch (Exception e) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024]; // 缓冲区大小，可以根据实际情况调整
            int bytesRead;
            try {
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    byteArrayOutputStream.write(buffer, 0, bytesRead);
                }
                byte[] bytes = byteArrayOutputStream.toByteArray();
                JsonDeserializationSchema schema = (JsonDeserializationSchema) deserializationSchema;
                schema.collect(
                        bytes,
                        output,
                        jsonField,
                        contentJson);
            } catch (IOException e1) {
                e1.printStackTrace();
            } finally {
                try {
                    inputStream.close(); // 关闭原始输入流
                    byteArrayOutputStream.close(); // 关闭ByteArrayOutputStream
                } catch (IOException e2) {
                    e2.printStackTrace();
                }
            }
        }
    }

    @Override
    public SeaTunnelRowType getSeaTunnelRowTypeInfo(String path) throws FileConnectorException {
        throw new FileConnectorException(
                CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                "User must defined schema for json file type");
    }
}
