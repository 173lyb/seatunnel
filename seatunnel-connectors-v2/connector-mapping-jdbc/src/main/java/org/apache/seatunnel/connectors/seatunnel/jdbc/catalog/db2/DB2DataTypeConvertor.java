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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

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
public class DB2DataTypeConvertor implements DataTypeConvertor<String> {
    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 38;
    public static final Integer DEFAULT_SCALE = 18;
    // ============================data types=====================
    public static final String DB2_BOOLEAN = "BOOLEAN";

    public static final String DB2_ROWID = "ROWID";
    public static final String DB2_SMALLINT = "SMALLINT";
    public static final String DB2_INTEGER = "INTEGER";
    public static final String DB2_INT = "INT";
    public static final String DB2_BIGINT = "BIGINT";
    // exact
    public static final String DB2_DECIMAL = "DECIMAL";
    public static final String DB2_DEC = "DEC";
    public static final String DB2_NUMERIC = "NUMERIC";
    public static final String DB2_NUM = "NUM";
    // float
    public static final String DB2_REAL = "REAL";
    public static final String DB2_FLOAT = "FLOAT";
    public static final String DB2_DOUBLE = "DOUBLE";
    public static final String DB2_DOUBLE_PRECISION = "DOUBLE PRECISION";
    public static final String DB2_DECFLOAT = "DECFLOAT";
    // string
    public static final String DB2_CHAR = "CHAR";
    public static final String DB2_VARCHAR = "VARCHAR";
    public static final String DB2_LONG_VARCHAR = "LONG VARCHAR";
    public static final String DB2_CLOB = "CLOB";
    // graphic
    public static final String DB2_GRAPHIC = "GRAPHIC";
    public static final String DB2_VARGRAPHIC = "VARGRAPHIC";
    public static final String DB2_LONG_VARGRAPHIC = "LONG VARGRAPHIC";
    public static final String DB2_DBCLOB = "DBCLOB";

    // ---------------------------binary---------------------------
    public static final String DB2_BINARY = "BINARY";
    public static final String DB2_VARBINARY = "VARBINARY";

    // ------------------------------time-------------------------
    public static final String DB2_DATE = "DATE";
    public static final String DB2_TIME = "TIME";
    public static final String DB2_TIMESTAMP = "TIMESTAMP";

    // ------------------------------blob-------------------------
    public static final String DB2_BLOB = "BLOB";

    // other
    public static final String DB2_XML = "XML";
    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "Db2 Type cannot be null");
        //connectorDataType = normalizeTimestamp(connectorDataType);
        switch (connectorDataType) {
            case DB2_BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case DB2_SMALLINT:
                return BasicType.SHORT_TYPE;
            case DB2_INT:
            case DB2_INTEGER:
                return BasicType.INT_TYPE;
            case DB2_BIGINT:
                return BasicType.LONG_TYPE;
            case DB2_DECIMAL:
            case DB2_DEC:
            case DB2_NUMERIC:
            case DB2_NUM:
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
            case DB2_REAL:
                return BasicType.FLOAT_TYPE;
            case DB2_FLOAT:
            case DB2_DOUBLE:
            case DB2_DOUBLE_PRECISION:
            case DB2_DECFLOAT:
                return BasicType.DOUBLE_TYPE;
            case DB2_CHAR:
            case DB2_VARCHAR:
            case DB2_LONG_VARCHAR:
            case DB2_CLOB:
            case DB2_GRAPHIC:
            case DB2_VARGRAPHIC:
            case DB2_LONG_VARGRAPHIC:
            case DB2_DBCLOB:
                return BasicType.STRING_TYPE;
            case DB2_BINARY:
            case DB2_VARBINARY:
            case DB2_BLOB:
                return PrimitiveByteArrayType.INSTANCE;
            case DB2_DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case DB2_TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DB2_TIMESTAMP:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case DB2_ROWID:
                // maybe should support
            case DB2_XML:
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.DB_2, connectorDataType, field);
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
                return DB2_SMALLINT;
            case SMALLINT:
                return DB2_SMALLINT;
            case INT:
                return DB2_INTEGER;
            case BIGINT:
                return DB2_BIGINT;
            case FLOAT:
                return DB2_REAL;
            case DOUBLE:
                return DB2_DOUBLE;
            case DECIMAL:
                return DB2_NUMERIC;
            case BOOLEAN:
                return DB2_BOOLEAN;
            case STRING:
                return DB2_VARCHAR;
            case DATE:
                return DB2_DATE;
            case TIME:
                return DB2_TIME;
            case TIMESTAMP:
                return DB2_TIMESTAMP;
            case BYTES:
                return DB2_BLOB;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.DB_2,
                        seaTunnelDataType.getSqlType().toString(),
                        field);
        }
    }


    @Override
    public String getIdentity() {
        return DatabaseIdentifier.DB_2;
    }
}
