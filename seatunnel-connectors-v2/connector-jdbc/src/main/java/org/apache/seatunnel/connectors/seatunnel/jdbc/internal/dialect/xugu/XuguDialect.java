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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class XuguDialect implements JdbcDialect {

    private static final int DEFAULT_XUGU_FETCH_SIZE = 500;
    public String fieldIde = FieldIdeEnum.ORIGINAL.getValue();
    public XuguDialect(String fieldIde) {
        this.fieldIde = fieldIde;
    }

    public XuguDialect() {}
    @Override
    public String dialectName() {
        return DatabaseIdentifier.XUGU;
    }

    @Override
    public JdbcRowConverter getRowConverter() {
        return new XuguJdbcRowConverter();
    }
    @Override
    public JdbcDialectTypeMapper getJdbcDialectTypeMapper() {
        return new XuguTypeMapper();
    }

    @Override
    public String tableIdentifier(String database, String tableName) {
        if (tableName.contains(".")) {
            return quoteIdentifier(tableName);
        }
        return quoteDatabaseIdentifier(database) + "." + quoteIdentifier(tableName);
    }

    @Override
    public String quoteIdentifier(String identifier) {
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.");
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < parts.length - 1; i++) {
                sb.append("\"").append(parts[i]).append("\"").append(".");
            }
            return sb.append("\"")
                    .append(getFieldIde(parts[parts.length - 1], fieldIde))
                    .append("\"")
                    .toString();
        }

        return "\"" + getFieldIde(identifier, fieldIde) + "\"";
    }
    @Override
    public String extractTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public TablePath parse(String tablePath) {
        return TablePath.of(tablePath, true);
    }

    @Override
    public String tableIdentifier(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }


    @Override
    public Optional<String> getUpsertStatement(
            String database, String tableName, String[] fieldNames, String[] uniqueKeyFields) {

        List<String> nonUniqueKeyFields =
                Arrays.stream(fieldNames)
                        .filter(fieldName -> !Arrays.asList(uniqueKeyFields).contains(fieldName))
                        .collect(Collectors.toList());
        if (nonUniqueKeyFields.isEmpty()) {
            throw new SeaTunnelException("非主键字段不能为空，请设置其他字段");
        }
        String valuesBinding =
                Arrays.stream(fieldNames)
                        .map(fieldName -> ":" + fieldName + " " + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String usingClause = String.format("SELECT %s FROM DUAL", valuesBinding);
        String onConditions =
                Arrays.stream(uniqueKeyFields)
                        .map(
                                fieldName ->
                                        String.format(
                                                "TARGET.%s=SOURCE.%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(" AND "));

        String updateSetClause =
                nonUniqueKeyFields.stream()
                        .map(
                                fieldName ->
                                        String.format(
                                                "TARGET.%s=SOURCE.%s",
                                                quoteIdentifier(fieldName),
                                                quoteIdentifier(fieldName)))
                        .collect(Collectors.joining(", "));

        String insertFields =
                Arrays.stream(fieldNames)
                        .map(this::quoteIdentifier)
                        .collect(Collectors.joining(", "));
        String insertValues =
                Arrays.stream(fieldNames)
                        .map(fieldName -> "SOURCE." + quoteIdentifier(fieldName))
                        .collect(Collectors.joining(", "));
        String upsertSQL =
                String.format(
                        " MERGE INTO %s TARGET"
                                + " USING (%s) SOURCE"
                                + " ON (%s) "
                                + " WHEN MATCHED THEN"
                                + " UPDATE SET %s"
                                + " WHEN NOT MATCHED THEN"
                                + " INSERT (%s) VALUES (%s)",
                        tableIdentifier(database, tableName),
                        usingClause,
                        onConditions,
                        updateSetClause,
                        insertFields,
                        insertValues);

        return Optional.of(upsertSQL);
    }

    @Override
    public PreparedStatement creatPreparedStatement(
            Connection connection, String queryTemplate, int fetchSize) throws SQLException {
        PreparedStatement statement =
                connection.prepareStatement(
                        queryTemplate, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        if (fetchSize > 0) {
            statement.setFetchSize(fetchSize);
        } else {
            statement.setFetchSize(DEFAULT_XUGU_FETCH_SIZE);
        }
        return statement;
    }

}
