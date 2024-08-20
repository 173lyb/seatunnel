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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sybase;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sybase.SybaseTypeConverter;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class SyBaseCreateTableSqlBuilder {

    private final String tableName;
    private List<Column> columns;

    private String comment;

    private String engine;
    private String charset;
    private String collate;

    private PrimaryKey primaryKey;

    private List<ConstraintKey> constraintKeys;

    private String fieldIde;

    private boolean createIndex;

    private SyBaseCreateTableSqlBuilder(String tableName, boolean createIndex) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
        this.createIndex = createIndex;
    }

    public static SyBaseCreateTableSqlBuilder builder(
            TablePath tablePath, CatalogTable catalogTable, boolean createIndex) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");

        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");

        return new SyBaseCreateTableSqlBuilder(tablePath.getTableName(), createIndex)
                .comment(catalogTable.getComment())
                // todo: set charset and collate
                .engine(null)
                .charset(null)
                .primaryKey(tableSchema.getPrimaryKey())
                .constraintKeys(tableSchema.getConstraintKeys())
                .addColumn(tableSchema.getColumns())
                .fieldIde(catalogTable.getOptions().get("fieldIde"));
    }

    public SyBaseCreateTableSqlBuilder addColumn(List<Column> columns) {
        checkArgument(CollectionUtils.isNotEmpty(columns), "columns must not be empty");
        this.columns = columns;
        return this;
    }

    public SyBaseCreateTableSqlBuilder primaryKey(PrimaryKey primaryKey) {
        this.primaryKey = primaryKey;
        return this;
    }

    public SyBaseCreateTableSqlBuilder fieldIde(String fieldIde) {
        this.fieldIde = fieldIde;
        return this;
    }

    public SyBaseCreateTableSqlBuilder constraintKeys(List<ConstraintKey> constraintKeys) {
        this.constraintKeys = constraintKeys;
        return this;
    }

    public SyBaseCreateTableSqlBuilder engine(String engine) {
        this.engine = engine;
        return this;
    }

    public SyBaseCreateTableSqlBuilder charset(String charset) {
        this.charset = charset;
        return this;
    }

    public SyBaseCreateTableSqlBuilder collate(String collate) {
        this.collate = collate;
        return this;
    }

    public SyBaseCreateTableSqlBuilder comment(String comment) {
        this.comment = comment;
        return this;
    }

    public String build(TablePath tablePath, CatalogTable catalogTable) {
        List<String> sqls = new ArrayList<>();
        String sqlTableName = tablePath.getFullNameWithQuoted("[", "]");
        Map<String, String> columnComments = new HashMap<>();
        sqls.add(
                String.format(
                        "IF OBJECT_ID('%s') IS NULL \n" + "BEGIN \n" + "CREATE TABLE %s ( \n%s\n)",
                        sqlTableName,
                        sqlTableName,
                        buildColumnsIdentifySql(catalogTable.getCatalogName(), columnComments)));
        if (engine != null) {
            sqls.add("ENGINE = " + engine);
        }
        if (charset != null) {
            sqls.add("DEFAULT CHARSET = " + charset);
        }
        if (collate != null) {
            sqls.add("COLLATE = " + collate);
        }
        String sqlTableSql = String.join(" ", sqls);
        sqlTableSql = CatalogUtils.quoteIdentifier(sqlTableSql, fieldIde);
        StringBuilder tableAndColumnComment = new StringBuilder();
        //        if (comment != null) {
        //            sqls.add("COMMENT = '" + comment + "'");
        //            tableAndColumnComment.append(
        //                    String.format(
        //                            "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s',
        // 'schema', N'%s', 'table', N'%s';\n",
        //                            tablePath.getDatabaseName(),
        //                            comment,
        //                            tablePath.getSchemaName(),
        //                            tablePath.getTableName()));
        //        }
        // TODO sybase没有这个sp_addextendedproperty
        String columnComment =
                "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s', 'column', N'%s'\n";
        columnComments.forEach(
                (fieldName, com) -> {
                    tableAndColumnComment.append(
                            String.format(
                                    columnComment,
                                    tablePath.getDatabaseName(),
                                    com,
                                    tablePath.getSchemaName(),
                                    tablePath.getTableName(),
                                    fieldName));
                });
        return String.join("\n", sqlTableSql, tableAndColumnComment.toString(), "END");
    }

    private String buildColumnsIdentifySql(String catalogName, Map<String, String> columnComments) {
        List<String> columnSqls = new ArrayList<>();
        for (Column column : columns) {
            columnSqls.add("\t" + buildColumnIdentifySql(column, catalogName, columnComments));
        }
        if (createIndex && primaryKey != null) {
            columnSqls.add("\t" + buildPrimaryKeySql());
        }
        if (CollectionUtils.isNotEmpty(constraintKeys)) {
            for (ConstraintKey constraintKey : constraintKeys) {
                if (StringUtils.isBlank(constraintKey.getConstraintName())) {
                    continue;
                }
            }
        }
        return String.join(", \n", columnSqls);
    }

    private String buildColumnIdentifySql(
            Column column, String catalogName, Map<String, String> columnComments) {
        final List<String> columnSqls = new ArrayList<>();
        columnSqls.add("[" + column.getName() + "]");
        if (StringUtils.equals(catalogName, DatabaseIdentifier.SQLSERVER)) {
            columnSqls.add(column.getSourceType());
        } else {
            columnSqls.add(SybaseTypeConverter.INSTANCE.reconvert(column).getColumnType());
        }
        // nullable
        if (column.isNullable()) {
            columnSqls.add("NULL");
        } else {
            columnSqls.add("NOT NULL");
        }

        // comment
        if (column.getComment() != null) {
            columnComments.put(column.getName(), column.getComment().replace("'", "''"));
        }

        return String.join(" ", columnSqls);
    }

    private String buildPrimaryKeySql() {
        //                        .map(columnName -> "`" + columnName + "`")
        String key =
                primaryKey.getColumnNames().stream()
                        .map(columnName -> "[" + columnName + "]")
                        .collect(Collectors.joining(", "));
        // add sort type
        return String.format("PRIMARY KEY (%s)", key);
    }

    private String buildConstraintKeySql(ConstraintKey constraintKey) {
        ConstraintKey.ConstraintType constraintType = constraintKey.getConstraintType();
        String indexColumns =
                constraintKey.getColumnNames().stream()
                        .map(
                                constraintKeyColumn -> {
                                    if (constraintKeyColumn.getSortType() == null) {
                                        return String.format(
                                                "`%s`", constraintKeyColumn.getColumnName());
                                    }
                                    return String.format(
                                            "`%s` %s",
                                            constraintKeyColumn.getColumnName(),
                                            constraintKeyColumn.getSortType().name());
                                })
                        .collect(Collectors.joining(", "));
        String keyName = null;
        switch (constraintType) {
            case INDEX_KEY:
                keyName = "KEY";
                break;
            case UNIQUE_KEY:
                keyName = "UNIQUE KEY";
                break;
            case FOREIGN_KEY:
                keyName = "FOREIGN KEY";
                // todo:
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported constraint type: " + constraintType);
        }
        return String.format(
                "%s `%s` (%s)", keyName, constraintKey.getConstraintName(), indexColumns);
    }
}
