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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.kingbase;

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

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class KingBaseDataTypeConvertor implements DataTypeConvertor<String> {
    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 38;
    public static final Integer DEFAULT_SCALE = 18;
    public static final String KB_SMALLSERIAL = "SMALLSERIAL";
    public static final String KB_SERIAL = "SERIAL";
    public static final String KB_BIGSERIAL = "BIGSERIAL";
    public static final String KB_BYTEA = "BYTEA";
    public static final String KB_BYTEA_ARRAY = "_BYTEA";
    public static final String KB_SMALLINT = "INT2";
    public static final String KB_SMALLINT_ARRAY = "_INT2";
    public static final String KB_INTEGER = "INT4";
    public static final String KB_INTEGER_ARRAY = "_INT4";
    public static final String KB_BIGINT = "INT8";
    public static final String KB_BIGINT_ARRAY = "_INT8";
    public static final String KB_REAL = "FLOAT4";
    public static final String KB_REAL_ARRAY = "_FLOAT4";
    public static final String KB_DOUBLE_PRECISION = "FLOAT8";
    public static final String KB_DOUBLE_PRECISION_ARRAY = "_FLOAT8";
    public static final String KB_NUMERIC = "NUMERIC";
    public static final String KB_NUMERIC_ARRAY = "_NUMERIC";
    //decimal
    public static final String KB_DECIMAL = "DECIMAL";
    public static final String KB_BOOLEAN = "BOOL";
    public static final String KB_BOOLEAN_ARRAY = "_BOOL";
    public static final String KB_TIMESTAMP = "TIMESTAMP";
    public static final String KB_TIMESTAMP_ARRAY = "_TIMESTAMP";
    public static final String KB_TIMESTAMPTZ = "TIMESTAMPTZ";
    public static final String KB_TIMESTAMPTZ_ARRAY = "_TIMESTAMPTZ";
    public static final String KB_DATE = "DATE";
    public static final String KB_DATE_ARRAY = "_DATE";
    public static final String KB_TIME = "TIME";
    public static final String KB_TIME_ARRAY = "_TIME";
    public static final String KB_TEXT = "TEXT";
    public static final String KB_TEXT_ARRAY = "_TEXT";
    public static final String KB_CHAR = "BPCHAR";
    public static final String KB_CHAR_ARRAY = "_BPCHAR";
    public static final String KB_CHARACTER = "CHARACTER";

    public static final String KB_CHARACTER_VARYING = "VARCHAR";
    public static final String KB_CHARACTER_VARYING_ARRAY = "_VARCHAR";
    public static final String KB_JSON = "JSON";
    public static final String KB_JSONB = "JSONB";
    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "Kingbase Type cannot be null");
        //connectorDataType = normalizeTimestamp(connectorDataType);
        switch (connectorDataType) {
            case KB_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case KB_SMALLINT:
                return BasicType.SHORT_TYPE;
            case KB_SMALLSERIAL:
            case KB_INTEGER:
            case KB_SERIAL:
                return BasicType.INT_TYPE;
            case KB_BIGINT:
            case KB_BIGSERIAL:
                return BasicType.LONG_TYPE;
            case KB_REAL:
                return BasicType.FLOAT_TYPE;
            case KB_DOUBLE_PRECISION:
                return BasicType.DOUBLE_TYPE;
            case KB_NUMERIC:
            case KB_DECIMAL:
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
            case KB_CHARACTER_VARYING:
            case KB_CHARACTER:
            case KB_CHAR:
            case KB_TEXT:
            case KB_JSON:
                return BasicType.STRING_TYPE;
            case KB_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case KB_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case KB_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case KB_BYTEA:
                return PrimitiveByteArrayType.INSTANCE;
            case KB_CHAR_ARRAY:
            case KB_CHARACTER_VARYING_ARRAY:
            case KB_TEXT_ARRAY:
            case KB_DOUBLE_PRECISION_ARRAY:
            case KB_REAL_ARRAY:
            case KB_BIGINT_ARRAY:
            case KB_SMALLINT_ARRAY:
            case KB_INTEGER_ARRAY:
            case KB_BYTEA_ARRAY:
            case KB_BOOLEAN_ARRAY:
            case KB_TIMESTAMP_ARRAY:
            case KB_NUMERIC_ARRAY:
            case KB_TIMESTAMPTZ:
            case KB_TIMESTAMPTZ_ARRAY:
            case KB_TIME_ARRAY:
            case KB_DATE_ARRAY:
            case KB_JSONB:
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.KINGBASE, connectorDataType, field);
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
                return KB_SMALLINT;
            case SMALLINT:
                return KB_SMALLINT;
            case INT:
                return KB_INTEGER;
            case BIGINT:
                return KB_BIGINT;
            case FLOAT:
                return KB_REAL;
            case DOUBLE:
                return KB_DOUBLE_PRECISION;
            case DECIMAL:
                return KB_NUMERIC;
            case BOOLEAN:
                return KB_BOOLEAN;
            case STRING:
                return KB_CHARACTER_VARYING;
            case DATE:
                return KB_DATE;
            case TIME:
                return KB_TIME;
            case TIMESTAMP:
                return KB_TIMESTAMP;
            case BYTES:
                return KB_BYTEA;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.KINGBASE,
                        seaTunnelDataType.getSqlType().toString(),
                        field);
        }
    }


    @Override
    public String getIdentity() {
        return DatabaseIdentifier.KINGBASE;
    }
}
