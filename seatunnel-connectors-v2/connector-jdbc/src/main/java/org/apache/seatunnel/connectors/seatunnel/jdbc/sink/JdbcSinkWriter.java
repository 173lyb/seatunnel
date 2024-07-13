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
import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.JdbcOutputFormatBuilder;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionPoolProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.JdbcSinkState;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class JdbcSinkWriter extends AbstractJdbcSinkWriter
        implements SupportMultiTableSinkWriter<ConnectionPoolManager> {
    private final Integer primaryKeyIndex;
    private SeaTunnelRowType rowTypeFull;

    public JdbcSinkWriter(
            TablePath sinkTablePath,
            JdbcDialect dialect,
            JdbcSinkConfig jdbcSinkConfig,
            TableSchema tableSchema,
            Integer primaryKeyIndex,
            SeaTunnelRowType rowTypeFull) {
        this.sinkTablePath = sinkTablePath;
        this.dialect = dialect;
        this.tableSchema = tableSchema;
        this.jdbcSinkConfig = jdbcSinkConfig;
        this.primaryKeyIndex = primaryKeyIndex;
        this.connectionProvider =
                dialect.getJdbcConnectionProvider(jdbcSinkConfig.getJdbcConnectionConfig());
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect, connectionProvider, jdbcSinkConfig, tableSchema)
                        .build();
        this.rowTypeFull = rowTypeFull;
    }
    private SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        //判空处理
        if (jdbcSinkConfig == null || MapUtils.isEmpty(jdbcSinkConfig.getFieldMapper())) {
            return inputRow;
        }
        Map<String, String> fieldMapper = jdbcSinkConfig.getFieldMapper();
        Object[] outputDataArray = new Object[fieldMapper.size()];
        List<Integer> needReaderColIndex = new ArrayList<>();
        String[] fieldNames = rowTypeFull.getFieldNames();
        ArrayList<String> inputFieldNames = Lists.newArrayList(fieldNames);
        fieldMapper.forEach(
                (key, value) -> {
                    int fieldIndex = inputFieldNames.indexOf(key);
                    if (fieldIndex < 0) {
                        throw new JdbcConnectorException(
                                JdbcConnectorErrorCode.INPUT_FIELD_NOT_FOUND,
                                "Can not found field " + key + " from inputRowType");
                    }
                    needReaderColIndex.add(fieldIndex);
                });
        for (int i = 0; i < outputDataArray.length; i++) {
            outputDataArray[i] = inputRow.getField(needReaderColIndex.get(i));
        }
        SeaTunnelRow outputRow = new SeaTunnelRow(outputDataArray);
        outputRow.setRowKind(inputRow.getRowKind());
        outputRow.setTableId(inputRow.getTableId());
        return outputRow;
    }
    @Override
    public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(
            int tableSize, int queueSize) {
        HikariDataSource ds = new HikariDataSource();
        try {
            Class.forName(jdbcSinkConfig.getJdbcConnectionConfig().getDriverName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        ds.setIdleTimeout(30 * 1000);
        ds.setMaximumPoolSize(queueSize);
        ds.setJdbcUrl(jdbcSinkConfig.getJdbcConnectionConfig().getUrl());
        if (jdbcSinkConfig.getJdbcConnectionConfig().getUsername().isPresent()) {
            ds.setUsername(jdbcSinkConfig.getJdbcConnectionConfig().getUsername().get());
        }
        if (jdbcSinkConfig.getJdbcConnectionConfig().getPassword().isPresent()) {
            ds.setPassword(jdbcSinkConfig.getJdbcConnectionConfig().getPassword().get());
        }
        ds.setAutoCommit(jdbcSinkConfig.getJdbcConnectionConfig().isAutoCommit());
        return new JdbcMultiTableResourceManager(new ConnectionPoolManager(ds));
    }

    @Override
    public void setMultiTableResourceManager(
            MultiTableResourceManager<ConnectionPoolManager> multiTableResourceManager,
            int queueIndex) {
        connectionProvider.closeConnection();
        this.connectionProvider =
                new SimpleJdbcConnectionPoolProviderProxy(
                        multiTableResourceManager.getSharedResource().get(),
                        jdbcSinkConfig.getJdbcConnectionConfig(),
                        queueIndex);
        this.outputFormat =
                new JdbcOutputFormatBuilder(
                                dialect, connectionProvider, jdbcSinkConfig, tableSchema)
                        .build();
    }

    @Override
    public Optional<Integer> primaryKey() {
        return primaryKeyIndex != null ? Optional.of(primaryKeyIndex) : Optional.empty();
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public List<JdbcSinkState> snapshotState(long checkpointId) {
        return Collections.emptyList();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        outputFormat.writeRecord(transformRow(element));
    }

    @Override
    public Optional<XidInfo> prepareCommit() throws IOException {
        tryOpen();
        outputFormat.checkFlushException();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.TRANSACTION_OPERATION_FAILED,
                    "commit failed," + e.getMessage(),
                    e);
        }
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        tryOpen();
        outputFormat.flush();
        try {
            if (!connectionProvider.getConnection().getAutoCommit()) {
                connectionProvider.getConnection().commit();
            }
        } catch (SQLException e) {
            throw new JdbcConnectorException(
                    CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED,
                    "unable to close JDBC sink write",
                    e);
        } finally {
            outputFormat.close();
        }
    }
}
