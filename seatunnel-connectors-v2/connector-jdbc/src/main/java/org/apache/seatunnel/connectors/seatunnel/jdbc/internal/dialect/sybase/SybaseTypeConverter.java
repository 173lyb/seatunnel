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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sybase;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.common.source.TypeDefineUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(TypeConverter.class)
public class SybaseTypeConverter extends SqlServerTypeConverter {
    // SYSNAME
    private static final String SYBASE_SYSNAME = "SYSNAME";

    public static final SybaseTypeConverter INSTANCE = new SybaseTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.SYBASE;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        try {
            return super.convert(typeDefine);
        } catch (SeaTunnelRuntimeException e) {
            PhysicalColumn.PhysicalColumnBuilder builder =
                    PhysicalColumn.builder()
                            .name(typeDefine.getName())
                            .sourceType(typeDefine.getColumnType())
                            .nullable(typeDefine.isNullable())
                            .defaultValue(typeDefine.getDefaultValue())
                            .comment(typeDefine.getComment());

            String sybaseDataType = typeDefine.getDataType().toUpperCase();
            switch (sybaseDataType) {
                case SYBASE_SYSNAME:
                    if (typeDefine.getLength() == -1) {
                        builder.sourceType(MAX_VARCHAR);
                        builder.columnLength(
                                TypeDefineUtils.doubleByteTo4ByteLength(POWER_2_31 - 1));
                    } else {
                        builder.sourceType(
                                String.format("%s(%s)", SQLSERVER_VARCHAR, typeDefine.getLength()));
                        builder.columnLength(
                                TypeDefineUtils.doubleByteTo4ByteLength(typeDefine.getLength()));
                    }
                    builder.dataType(BasicType.STRING_TYPE);
                    break;
                default:
                    throw CommonError.convertToSeaTunnelTypeError(
                            DatabaseIdentifier.SYBASE,
                            typeDefine.getDataType(),
                            typeDefine.getName());
            }
            return builder.build();
        }
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        BasicTypeDefine.BasicTypeDefineBuilder builder =
                BasicTypeDefine.builder()
                        .name(column.getName())
                        .nullable(column.isNullable())
                        .comment(column.getComment())
                        .defaultValue(column.getDefaultValue());
        switch (column.getDataType().getSqlType()) {
            case BOOLEAN:
                builder.columnType(SQLSERVER_BIT);
                builder.dataType(SQLSERVER_BIT);
                break;
            case TINYINT:
                builder.columnType(SQLSERVER_TINYINT);
                builder.dataType(SQLSERVER_TINYINT);
                break;
            case SMALLINT:
                builder.columnType(SQLSERVER_SMALLINT);
                builder.dataType(SQLSERVER_SMALLINT);
                break;
            case INT:
                builder.columnType(SQLSERVER_INT);
                builder.dataType(SQLSERVER_INT);
                break;
            case BIGINT:
                builder.columnType(SQLSERVER_BIGINT);
                builder.dataType(SQLSERVER_BIGINT);
                break;
            case FLOAT:
                builder.columnType(SQLSERVER_REAL);
                builder.dataType(SQLSERVER_REAL);
                break;
            case DOUBLE:
                builder.columnType(SQLSERVER_FLOAT);
                builder.dataType(SQLSERVER_FLOAT);
                break;
            case DECIMAL:
                DecimalType decimalType = (DecimalType) column.getDataType();
                long precision = decimalType.getPrecision();
                int scale = decimalType.getScale();
                if (precision <= 0) {
                    precision = DEFAULT_PRECISION;
                    scale = DEFAULT_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is precision less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (precision > MAX_PRECISION) {
                    scale = (int) Math.max(0, scale - (precision - MAX_PRECISION));
                    precision = MAX_PRECISION;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum precision of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_PRECISION,
                            precision,
                            scale);
                }
                if (scale < 0) {
                    scale = 0;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which is scale less than 0, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            precision,
                            scale);
                } else if (scale > MAX_SCALE) {
                    scale = MAX_SCALE;
                    log.warn(
                            "The decimal column {} type decimal({},{}) is out of range, "
                                    + "which exceeds the maximum scale of {}, "
                                    + "it will be converted to decimal({},{})",
                            column.getName(),
                            decimalType.getPrecision(),
                            decimalType.getScale(),
                            MAX_SCALE,
                            precision,
                            scale);
                }
                builder.columnType(String.format("%s(%s,%s)", SQLSERVER_DECIMAL, precision, scale));
                builder.dataType(SQLSERVER_DECIMAL);
                builder.precision(precision);
                builder.scale(scale);
                break;
            case STRING:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(SQLSERVER_TEXT);
                    builder.dataType(SQLSERVER_TEXT);
                } else if (column.getColumnLength() <= MAX_CHAR_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", SQLSERVER_VARCHAR, column.getColumnLength()));
                    builder.dataType(SQLSERVER_VARCHAR);
                    builder.length(column.getColumnLength());
                } else {
                    builder.columnType(SQLSERVER_TEXT);
                    builder.dataType(SQLSERVER_TEXT);
                    builder.length(column.getColumnLength());
                }
                break;
            case BYTES:
                if (column.getColumnLength() == null || column.getColumnLength() <= 0) {
                    builder.columnType(MAX_VARBINARY);
                    builder.dataType(SQLSERVER_VARBINARY);
                } else if (column.getColumnLength() <= MAX_BINARY_LENGTH) {
                    builder.columnType(
                            String.format("%s(%s)", SQLSERVER_VARBINARY, column.getColumnLength()));
                    builder.dataType(SQLSERVER_VARBINARY);
                    builder.length(column.getColumnLength());
                } else {
                    builder.columnType(MAX_VARBINARY);
                    builder.dataType(SQLSERVER_VARBINARY);
                    builder.length(column.getColumnLength());
                }
                break;
            case DATE:
                builder.columnType(SQLSERVER_DATE);
                builder.dataType(SQLSERVER_DATE);
                break;
            case TIME:
                if (column.getScale() != null && column.getScale() > 0) {
                    int timeScale = column.getScale();
                    if (timeScale > MAX_TIME_SCALE) {
                        timeScale = MAX_TIME_SCALE;
                        log.warn(
                                "The time column {} type time({}) is out of range, "
                                        + "which exceeds the maximum scale of {}, "
                                        + "it will be converted to time({})",
                                column.getName(),
                                column.getScale(),
                                MAX_SCALE,
                                timeScale);
                    }
                    builder.columnType(String.format("%s(%s)", SQLSERVER_TIME, timeScale));
                    builder.scale(timeScale);
                } else {
                    builder.columnType(SQLSERVER_TIME);
                }
                builder.dataType(SQLSERVER_TIME);
                break;
            case TIMESTAMP:
                builder.columnType(SQLSERVER_DATETIME);
                builder.dataType(SQLSERVER_DATETIME);
                break;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.SYBASE,
                        column.getDataType().getSqlType().name(),
                        column.getName());
        }
        return builder.build();
    }
}
