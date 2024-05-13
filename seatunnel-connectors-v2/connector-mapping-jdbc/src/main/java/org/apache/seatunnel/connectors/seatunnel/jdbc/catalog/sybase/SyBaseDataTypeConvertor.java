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

import com.google.auto.service.AutoService;
import lombok.NonNull;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import java.util.Map;

@AutoService(DataTypeConvertor.class)
public class SyBaseDataTypeConvertor implements DataTypeConvertor<SyBaseType> {
    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 10;
    public static final Integer DEFAULT_SCALE = 0;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, @NonNull String connectorDataType) {
        Pair<SyBaseType, Map<String, Object>> syBaseType =
                SyBaseType.parse(connectorDataType);
        return toSeaTunnelType(field, syBaseType.getLeft(), syBaseType.getRight());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field,
            @NonNull SyBaseType connectorDataType,
            Map<String, Object> dataTypeProperties) {
        switch (connectorDataType) {
            case BIT:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INTEGER:
            case INT_IDENTITY:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case DECIMAL:
            case NUMERIC:
            case MONEY:
            case SMALLMONEY:
                int precision = (int) dataTypeProperties.getOrDefault(PRECISION, DEFAULT_PRECISION);
                int scale = (int) dataTypeProperties.getOrDefault(SCALE, DEFAULT_SCALE);
                return new DecimalType(precision, scale);
            case REAL:
                return BasicType.FLOAT_TYPE;
            case FLOAT:
                return BasicType.DOUBLE_TYPE;
            case CHAR:
            case NCHAR:
            case VARCHAR:
            case NTEXT:
            case NVARCHAR:
            case TEXT:
            case XML:
            case GUID:
            case SQL_VARIANT:
                return BasicType.STRING_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case DATETIME:
            case DATETIME2:
            case SMALLDATETIME:
            case DATETIMEOFFSET:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case TIMESTAMP:
            case BINARY:
            case VARBINARY:
            case IMAGE:
                return PrimitiveByteArrayType.INSTANCE;
            case UNKNOWN:
            default:
                throw CommonError.convertToSeaTunnelTypeError(
                        DatabaseIdentifier.SYBASE, connectorDataType.toString(), field);
        }
    }

    @Override
    public SyBaseType toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        SqlType sqlType = seaTunnelDataType.getSqlType();
        switch (sqlType) {
            case STRING:
                return SyBaseType.VARCHAR;
            case BOOLEAN:
                return SyBaseType.BIT;
            case TINYINT:
                return SyBaseType.TINYINT;
            case SMALLINT:
                return SyBaseType.SMALLINT;
            case INT:
                return SyBaseType.INTEGER;
            case BIGINT:
                return SyBaseType.BIGINT;
            case FLOAT:
                return SyBaseType.REAL;
            case DOUBLE:
                return SyBaseType.FLOAT;
            case DECIMAL:
                return SyBaseType.DECIMAL;
            case BYTES:
                return SyBaseType.BINARY;
            case DATE:
                return SyBaseType.DATE;
            case TIME:
                return SyBaseType.TIME;
            case TIMESTAMP:
                return SyBaseType.DATETIME;
            default:
                throw CommonError.convertToConnectorTypeError(
                        DatabaseIdentifier.SYBASE,
                        seaTunnelDataType.getSqlType().toString(),
                        field);
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.SYBASE;
    }
}
