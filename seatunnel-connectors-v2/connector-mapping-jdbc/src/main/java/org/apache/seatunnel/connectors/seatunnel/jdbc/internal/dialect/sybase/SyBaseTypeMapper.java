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

import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

@Slf4j
public class SyBaseTypeMapper implements JdbcDialectTypeMapper {

    // ============================data types=====================

    private static final String SYBASE_UNKNOWN = "UNKNOWN";

    // -------------------------number----------------------------
    private static final String SYBASE_BIT = "BIT";
    private static final String SYBASE_TINYINT = "TINYINT";
    private static final String SYBASE_SMALLINT = "SMALLINT";
    private static final String SYBASE_INTEGER = "INTEGER";
    private static final String SYBASE_INT = "INT";
    private static final String SYBASE_BIGINT = "BIGINT";
    private static final String SYBASE_DECIMAL = "DECIMAL";
    private static final String SYBASE_FLOAT = "FLOAT";
    private static final String SYBASE_REAL = "REAL";
    private static final String SYBASE_NUMERIC = "NUMERIC";
    private static final String SYBASE_MONEY = "MONEY";
    private static final String SYBASE_SMALLMONEY = "SMALLMONEY";
    // -------------------------string----------------------------
    private static final String SYBASE_CHAR = "CHAR";
    private static final String SYBASE_VARCHAR = "VARCHAR";
    private static final String SYBASE_NTEXT = "NTEXT";
    private static final String SYBASE_NCHAR = "NCHAR";
    private static final String SYBASE_NVARCHAR = "NVARCHAR";
    private static final String SYBASE_TEXT = "TEXT";
    private static final String SYBASE_XML = "XML";
    private static final String SYBASE_UNIQUEIDENTIFIER = "UNIQUEIDENTIFIER";
    private static final String SYBASE_SQLVARIANT = "SQL_VARIANT";
    //SYSNAME
    private static final String SYBASE_SYSNAME = "SYSNAME";

    // ------------------------------time-------------------------
    private static final String SYBASE_DATE = "DATE";
    private static final String SYBASE_TIME = "TIME";
    private static final String SYBASE_DATETIME = "DATETIME";
    private static final String SYBASE_DATETIME2 = "DATETIME2";
    private static final String SYBASE_SMALLDATETIME = "SMALLDATETIME";
    private static final String SYBASE_DATETIMEOFFSET = "DATETIMEOFFSET";
    private static final String SYBASE_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    private static final String SYBASE_BINARY = "BINARY";
    private static final String SYBASE_VARBINARY = "VARBINARY";
    private static final String SYBASE_IMAGE = "IMAGE";

    @Override
    public SeaTunnelDataType<?> mapping(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String syBaseType = metadata.getColumnTypeName(colIndex).toUpperCase();
        int precision = metadata.getPrecision(colIndex);
        int scale = metadata.getScale(colIndex);
        switch (syBaseType) {
            case SYBASE_BIT:
                return BasicType.BOOLEAN_TYPE;
            case SYBASE_TINYINT:
            case SYBASE_SMALLINT:
                return BasicType.SHORT_TYPE;
            case SYBASE_INTEGER:
            case SYBASE_INT:
                return BasicType.INT_TYPE;
            case SYBASE_BIGINT:
                return BasicType.LONG_TYPE;
            case SYBASE_DECIMAL:
            case SYBASE_NUMERIC:
            case SYBASE_MONEY:
            case SYBASE_SMALLMONEY:
                return new DecimalType(precision, scale);
            case SYBASE_REAL:
                return BasicType.FLOAT_TYPE;
            case SYBASE_FLOAT:
                return BasicType.DOUBLE_TYPE;
            case SYBASE_CHAR:
            case SYBASE_NCHAR:
            case SYBASE_VARCHAR:
            case SYBASE_NTEXT:
            case SYBASE_NVARCHAR:
            case SYBASE_TEXT:
            case SYBASE_XML:
            case SYBASE_UNIQUEIDENTIFIER:
            case SYBASE_SQLVARIANT:
            case SYBASE_SYSNAME:
                return BasicType.STRING_TYPE;
            case SYBASE_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case SYBASE_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case SYBASE_DATETIME:
            case SYBASE_DATETIME2:
            case SYBASE_SMALLDATETIME:
            case SYBASE_DATETIMEOFFSET:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case SYBASE_TIMESTAMP:
            case SYBASE_BINARY:
            case SYBASE_VARBINARY:
            case SYBASE_IMAGE:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case SYBASE_UNKNOWN:
            default:
                final String jdbcColumnName = metadata.getColumnName(colIndex);
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.SYBASE, syBaseType, jdbcColumnName);
        }
    }
}
