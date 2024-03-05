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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu;

import com.google.auto.service.AutoService;
import org.apache.commons.collections4.MapUtils;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class XuguDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 38;
    public static final Integer DEFAULT_SCALE = 18;

    // ============================data types=====================
    public static final String XUGU_UNKNOWN = "UNKNOWN";
    // -------------------------number----------------------------
    public static final String XUGU_BINARY_DOUBLE = "BINARY_DOUBLE";
    public static final String XUGU_BINARY_FLOAT = "BINARY_FLOAT";
    public static final String XUGU_NUMBER = "NUMBER";
    public static final String XUGU_FLOAT = "FLOAT";
    public static final String XUGU_REAL = "REAL";
    public static final String XUGU_INTEGER = "INTEGER";
    // -------------------------string----------------------------
    public static final String XUGU_CHAR = "CHAR";
    public static final String XUGU_VARCHAR2 = "VARCHAR2";
    public static final String XUGU_NCHAR = "NCHAR";
    public static final String XUGU_NVARCHAR2 = "NVARCHAR2";
    public static final String XUGU_LONG = "LONG";
    public static final String XUGU_ROWID = "ROWID";
    public static final String XUGU_CLOB = "CLOB";
    public static final String XUGU_NCLOB = "NCLOB";
    private static final String XUGU_XML = "XMLTYPE";
    // ------------------------------time-------------------------
    public static final String XUGU_DATE = "DATE";
    public static final String XUGU_TIMESTAMP = "TIMESTAMP";
    public static final String XUGU_TIMESTAMP_WITH_LOCAL_TIME_ZONE =
            "TIMESTAMP WITH LOCAL TIME ZONE";
    // ------------------------------blob-------------------------
    public static final String XUGU_BLOB = "BLOB";
    public static final String XUGU_BFILE = "BFILE";
    public static final String XUGU_RAW = "RAW";
    public static final String XUGU_LONG_RAW = "LONG RAW";

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "XUGU Type cannot be null");
        connectorDataType = normalizeTimestamp(connectorDataType);
        switch (connectorDataType) {
            case XUGU_INTEGER:
                return BasicType.INT_TYPE;
            case XUGU_FLOAT:
                // The float type will be converted to DecimalType(10, -127),
                // which will lose precision in the spark engine
                return new DecimalType(38, 18);
            case XUGU_NUMBER:
                int precision =
                        MapUtils.getInteger(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                int scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                if (scale == 0) {
                    if (precision == 0) {
                        return new DecimalType(38, 18);
                    }
                    if (precision == 1) {
                        return BasicType.BOOLEAN_TYPE;
                    }
                    if (precision <= 9) {
                        return BasicType.INT_TYPE;
                    }
                    if (precision <= 18) {
                        return BasicType.LONG_TYPE;
                    }
                }
                return new DecimalType(38, 18);
            case XUGU_BINARY_DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case XUGU_BINARY_FLOAT:
            case XUGU_REAL:
                return BasicType.FLOAT_TYPE;
            case XUGU_CHAR:
            case XUGU_NCHAR:
            case XUGU_NVARCHAR2:
            case XUGU_VARCHAR2:
            case XUGU_LONG:
            case XUGU_ROWID:
            case XUGU_NCLOB:
            case XUGU_CLOB:
            case XUGU_XML:
                return BasicType.STRING_TYPE;
            case XUGU_DATE:
            case XUGU_TIMESTAMP:
            case XUGU_TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case XUGU_BLOB:
            case XUGU_RAW:
            case XUGU_LONG_RAW:
            case XUGU_BFILE:
                return PrimitiveByteArrayType.INSTANCE;
                // Doesn't support yet
            case XUGU_UNKNOWN:
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.XUGU, connectorDataType, field);
        }
    }

    @Override
    public String toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case TINYINT:
            case SMALLINT:
            case INT:
                return XUGU_INTEGER;
            case BIGINT:
                return XUGU_NUMBER;
            case FLOAT:
                return XUGU_FLOAT;
            case DOUBLE:
                return XUGU_BINARY_DOUBLE;
            case DECIMAL:
                return XUGU_NUMBER;
            case BOOLEAN:
                return XUGU_NUMBER;
            case STRING:
                return XUGU_VARCHAR2;
            case DATE:
                return XUGU_DATE;
            case TIMESTAMP:
                return XUGU_TIMESTAMP_WITH_LOCAL_TIME_ZONE;
            case BYTES:
                return XUGU_BLOB;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.XUGU,
                        seaTunnelDataType.getSqlType().toString(),
                        field);
        }
    }

    public static String normalizeTimestamp(String XUGUType) {
        // Create a pattern to match TIMESTAMP followed by an optional (0-9)
        String pattern = "^TIMESTAMP(\\([0-9]\\))?$";
        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);
        // Now create matcher object.
        Matcher m = r.matcher(XUGUType);
        if (m.find()) {
            return "TIMESTAMP";
        } else {
            return XUGUType;
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.XUGU;
    }
}
