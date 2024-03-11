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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import com.google.common.collect.Lists;
import org.apache.commons.collections4.MapUtils;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;

public class JdbcSink
        implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink {
    private final SeaTunnelRowType seaTunnelRowType;

    private final TableSchema tableSchema;

    private JobContext jobContext;

    private final JdbcSinkConfig jdbcSinkConfig;

    private final JdbcDialect dialect;

    private final ReadonlyConfig config;

    private final DataSaveMode dataSaveMode;

    private final SchemaSaveMode schemaSaveMode;

    private final CatalogTable catalogTable;

    private List<Integer> needReaderColIndex;

    public JdbcSink(
            ReadonlyConfig config,
            JdbcSinkConfig jdbcSinkConfig,
            JdbcDialect dialect,
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            CatalogTable catalogTable,
            SeaTunnelRowType seaTunnelRowTypeFull) {
        this.config = config;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.dialect = dialect;
        this.schemaSaveMode = schemaSaveMode;
        this.dataSaveMode = dataSaveMode;
//        if (MapUtils.isNotEmpty(jdbcSinkConfig.getFieldMapper())){
//            this.catalogTable = transformCatalogTable(catalogTable);
//        } else {
//            this.catalogTable = catalogTable;
//        }
        this.seaTunnelRowType = seaTunnelRowTypeFull;
//        if (MapUtils.isNotEmpty(jdbcSinkConfig.getFieldMapper())){
//            this.tableSchema = transformTableSchema();
//        }else {this.tableSchema = catalogTable.getTableSchema();}
        this.catalogTable = catalogTable;
        this.tableSchema = this.catalogTable.getTableSchema();
    }

    @Override
    public String getPluginName() {
        return "MappingJdbc";
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> createWriter(
            SinkWriter.Context context) {
        SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> sinkWriter;
        if (jdbcSinkConfig.isExactlyOnce()) {
            sinkWriter =
                    new JdbcExactlyOnceSinkWriter(
                            context,
                            jobContext,
                            dialect,
                            jdbcSinkConfig,
                            tableSchema,
                            new ArrayList<>());
        } else {
            if (catalogTable != null && catalogTable.getTableSchema().getPrimaryKey() != null) {
                String keyName = tableSchema.getPrimaryKey().getColumnNames().get(0);
                int index = tableSchema.toPhysicalRowDataType().indexOf(keyName);
                if (index > -1) {
                    return new JdbcSinkWriter(dialect, jdbcSinkConfig, tableSchema, index,seaTunnelRowType);
                }
            }
            sinkWriter = new JdbcSinkWriter(dialect, jdbcSinkConfig, tableSchema, null,seaTunnelRowType);
        }
        return sinkWriter;
    }

    @Override
    public SinkWriter<SeaTunnelRow, XidInfo, JdbcSinkState> restoreWriter(
            SinkWriter.Context context, List<JdbcSinkState> states) throws IOException {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return new JdbcExactlyOnceSinkWriter(
                    context, jobContext, dialect, jdbcSinkConfig, tableSchema, states);
        }
        return SeaTunnelSink.super.restoreWriter(context, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<XidInfo, JdbcAggregatedCommitInfo>>
            createAggregatedCommitter() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new JdbcSinkAggregatedCommitter(jdbcSinkConfig));
        }
        return Optional.empty();
    }

    @Override
    public Optional<Serializer<JdbcAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Optional<Serializer<XidInfo>> getCommitInfoSerializer() {
        if (jdbcSinkConfig.isExactlyOnce()) {
            return Optional.of(new DefaultSerializer<>());
        }
        return Optional.empty();
    }

    @Override
    public Optional<SaveModeHandler> getSaveModeHandler() {
        if (catalogTable != null) {
            if (StringUtils.isBlank(jdbcSinkConfig.getDatabase())) {
                return Optional.empty();
            }
            if (StringUtils.isBlank(jdbcSinkConfig.getTable())) {
                return Optional.empty();
            }
            // use query to write data can not support savemode
            if (StringUtils.isNotBlank(jdbcSinkConfig.getSimpleSql())) {
                return Optional.empty();
            }
            Optional<Catalog> catalogOptional =
                    JdbcCatalogUtils.findCatalog(jdbcSinkConfig.getJdbcConnectionConfig(), dialect);
            if (catalogOptional.isPresent()) {
                try {
                    Catalog catalog = catalogOptional.get();
                    catalog.open();
                    FieldIdeEnum fieldIdeEnumEnum = config.get(JdbcOptions.FIELD_IDE);
                    String fieldIde =
                            fieldIdeEnumEnum == null
                                    ? FieldIdeEnum.ORIGINAL.getValue()
                                    : fieldIdeEnumEnum.getValue();
                    TablePath tablePath =
                            TablePath.of(
                                    catalogTable.getTableId().getDatabaseName(),
                                    catalogTable.getTableId().getSchemaName(),
                                    CatalogUtils.quoteTableIdentifier(
                                            catalogTable.getTableId().getTableName(), fieldIde));
                    catalogTable.getOptions().put("fieldIde", fieldIde);
                    return Optional.of(
                            new DefaultSaveModeHandler(
                                    schemaSaveMode,
                                    dataSaveMode,
                                    catalog,
                                    tablePath,
                                    catalogTable,
                                    config.get(JdbcOptions.CUSTOM_SQL)));
                } catch (Exception e) {
                    throw new JdbcConnectorException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        }
        return Optional.empty();
    }
    // tableSchema转换
    private TableSchema transformTableSchema() {
        JdbcSinkConfig jdbcSinkConfig = JdbcSinkConfig.of(config);
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();

        List<Column> inputColumns = catalogTable.getTableSchema().getColumns();
        SeaTunnelRowType seaTunnelRowType =
                catalogTable.getTableSchema().toPhysicalRowDataType();
        List<Column> outputColumns = new ArrayList<>(fieldMapper.size());
        needReaderColIndex = new ArrayList<>(fieldMapper.size());
        ArrayList<String> inputFieldNames = Lists.newArrayList(seaTunnelRowType.getFieldNames());
        ArrayList<String> outputFieldNames = new ArrayList<>();
        fieldMapper.forEach(
                (key, value) -> {
                    int fieldIndex = inputFieldNames.indexOf(key);
                    if (fieldIndex < 0) {
                        throw new JdbcConnectorException(
                                JdbcConnectorErrorCode.INPUT_FIELD_NOT_FOUND,
                                "Can not found field " + key + " from inputRowType");
                    }
                    Column oldColumn = inputColumns.get(fieldIndex);
                    PhysicalColumn outputColumn =
                            PhysicalColumn.of(
                                    value,
                                    oldColumn.getDataType(),
                                    oldColumn.getColumnLength(),
                                    oldColumn.isNullable(),
                                    oldColumn.getDefaultValue(),
                                    oldColumn.getComment());
                    outputColumns.add(outputColumn);
                    outputFieldNames.add(outputColumn.getName());
                    needReaderColIndex.add(fieldIndex);
                });
        List<ConstraintKey> outputConstraintKeys =
                catalogTable.getTableSchema().getConstraintKeys().stream()
                        .filter(
                                key -> {
                                    List<String> constraintColumnNames =
                                            key.getColumnNames().stream()
                                                    .map(
                                                            ConstraintKey.ConstraintKeyColumn
                                                                    ::getColumnName)
                                                    .collect(Collectors.toList());
                                    return outputFieldNames.containsAll(constraintColumnNames);
                                })
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        PrimaryKey copiedPrimaryKey = null;
        if (catalogTable.getTableSchema().getPrimaryKey() != null
                && outputFieldNames.containsAll(
                catalogTable.getTableSchema().getPrimaryKey().getColumnNames())) {
            copiedPrimaryKey = catalogTable.getTableSchema().getPrimaryKey().copy();
        }

        return TableSchema.builder()
                .primaryKey(copiedPrimaryKey)
                .columns(outputColumns)
                .constraintKey(outputConstraintKeys)
                .build();
    }
    public CatalogTable transformCatalogTable(CatalogTable catalogTable) {
        JdbcSinkConfig jdbcSinkConfig = JdbcSinkConfig.of(config);
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();

        List<Column> inputColumns = catalogTable.getTableSchema().getColumns();
        SeaTunnelRowType seaTunnelRowType =
                catalogTable.getTableSchema().toPhysicalRowDataType();
        List<Column> outputColumns = new ArrayList<>(fieldMapper.size());
        needReaderColIndex = new ArrayList<>(fieldMapper.size());
        ArrayList<String> inputFieldNames = Lists.newArrayList(seaTunnelRowType.getFieldNames());
        ArrayList<String> outputFieldNames = new ArrayList<>();
        fieldMapper.forEach(
                (key, value) -> {
                    int fieldIndex = inputFieldNames.indexOf(key);
                    if (fieldIndex < 0) {
                        throw new JdbcConnectorException(
                                JdbcConnectorErrorCode.INPUT_FIELD_NOT_FOUND,
                                "Can not found field " + key + " from inputRowType");
                    }
                    Column oldColumn = inputColumns.get(fieldIndex);
                    PhysicalColumn outputColumn =
                            PhysicalColumn.of(
                                    value,
                                    oldColumn.getDataType(),
                                    oldColumn.getColumnLength(),
                                    oldColumn.isNullable(),
                                    oldColumn.getDefaultValue(),
                                    oldColumn.getComment());
                    outputColumns.add(outputColumn);
                    outputFieldNames.add(outputColumn.getName());
                    needReaderColIndex.add(fieldIndex);
                });
        List<ConstraintKey> outputConstraintKeys =
                catalogTable.getTableSchema().getConstraintKeys().stream()
                        .filter(
                                key -> {
                                    List<String> constraintColumnNames =
                                            key.getColumnNames().stream()
                                                    .map(
                                                            ConstraintKey.ConstraintKeyColumn
                                                                    ::getColumnName)
                                                    .collect(Collectors.toList());
                                    return outputFieldNames.containsAll(constraintColumnNames);
                                })
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        PrimaryKey copiedPrimaryKey = null;
        if (catalogTable.getTableSchema().getPrimaryKey() != null
                && outputFieldNames.containsAll(
                catalogTable.getTableSchema().getPrimaryKey().getColumnNames())) {
            copiedPrimaryKey = catalogTable.getTableSchema().getPrimaryKey().copy();
        }

        TableSchema tableSchema = TableSchema.builder()
                .primaryKey(copiedPrimaryKey)
                .columns(outputColumns)
                .constraintKey(outputConstraintKeys)
                .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                tableSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName()
        );
    }

}
