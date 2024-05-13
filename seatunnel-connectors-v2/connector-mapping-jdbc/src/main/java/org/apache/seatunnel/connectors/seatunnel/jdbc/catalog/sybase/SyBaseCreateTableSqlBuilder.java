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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

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

    private SyBaseDataTypeConvertor syBaseDataTypeConvertor;

    private String fieldIde;

    private SyBaseCreateTableSqlBuilder(String tableName) {
        checkNotNull(tableName, "tableName must not be null");
        this.tableName = tableName;
        this.syBaseDataTypeConvertor = new SyBaseDataTypeConvertor();
    }

    public static SyBaseCreateTableSqlBuilder builder(
            TablePath tablePath, CatalogTable catalogTable) {
        checkNotNull(tablePath, "tablePath must not be null");
        checkNotNull(catalogTable, "catalogTable must not be null");

        TableSchema tableSchema = catalogTable.getTableSchema();
        checkNotNull(tableSchema, "tableSchema must not be null");

        return new SyBaseCreateTableSqlBuilder(tablePath.getTableName())
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
                        "IF OBJECT_ID('%s') IS NULL \n"
                                + "BEGIN \n"
                                + "CREATE TABLE %s ( \n%s\n)",
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
        String sqlTableSql = String.join(" ", sqls) ;
        sqlTableSql = CatalogUtils.quoteIdentifier(sqlTableSql, fieldIde);
        StringBuilder tableAndColumnComment = new StringBuilder();
//        if (comment != null) {
//            sqls.add("COMMENT = '" + comment + "'");
//            tableAndColumnComment.append(
//                    String.format(
//                            "EXEC %s.sys.sp_addextendedproperty 'MS_Description', N'%s', 'schema', N'%s', 'table', N'%s';\n",
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
        if (primaryKey != null) {
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
        String tyNameDef = "";
        if (StringUtils.equals(catalogName, DatabaseIdentifier.SYBASE)) {
            columnSqls.add(column.getSourceType());
        } else {
            // Column name
            SqlType dataType = column.getDataType().getSqlType();
            boolean isBytes = StringUtils.equals(dataType.name(), SqlType.BYTES.name());
            Long columnLength = column.getLongColumnLength();
            Long bitLen = column.getBitLen();
            bitLen = bitLen == -1 || bitLen <= 8 ? bitLen : bitLen >> 3;
            if (isBytes) {
                if (bitLen > 8000 || bitLen == -1) {
                    columnSqls.add(SyBaseType.VARBINARY.getName());
                } else {
                    columnSqls.add(SyBaseType.BINARY.getName());
                    tyNameDef = SyBaseType.BINARY.getName();
                }
                columnSqls.add("(" + (bitLen == -1 || bitLen > 8000 ? "max)" : bitLen + ")"));
            } else {
                // Add column type
                SyBaseType syBaseType =
                        syBaseDataTypeConvertor.toConnectorType(
                                column.getName(), column.getDataType(), null);
                String typeName = syBaseType.getName();
                String fieldSuffixSql = null;
                tyNameDef = typeName;
                // Add column length
                if (StringUtils.equals(SyBaseType.VARCHAR.getName(), typeName)) {
                    if (columnLength > 8000 || columnLength == -1) {
                        columnSqls.add(typeName);
                        fieldSuffixSql = "(max)";
                    } else if (columnLength > 4000) {
                        columnSqls.add(SyBaseType.VARCHAR.getName());
                        fieldSuffixSql = "(" + columnLength + ")";
                    } else {
                        columnSqls.add(SyBaseType.NVARCHAR.getName());
                        if (columnLength > 0) {
                            fieldSuffixSql = "(" + columnLength + ")";
                        }
                    }
                    columnSqls.add(fieldSuffixSql);
                } else if (StringUtils.equals(SyBaseType.DECIMAL.getName(), typeName)) {
                    columnSqls.add(typeName);
                    DecimalType decimalType = (DecimalType) column.getDataType();
                    columnSqls.add(
                            String.format(
                                    "(%d, %d)",
                                    decimalType.getPrecision(), decimalType.getScale()));
                } else {
                    columnSqls.add(typeName);
                }
            }
        }
        // nullable
        if (column.isNullable()) {
            columnSqls.add("NULL");
        } else {
            columnSqls.add("NOT NULL");
        }

        // comment
        if (column.getComment() != null) {
            columnComments.put(column.getName(), column.getComment());
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
