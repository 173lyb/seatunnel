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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.xugu.XuguTypeMapper;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_BFILE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_BLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_CLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_LONG;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_LONG_RAW;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_NCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_NCLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_NVARCHAR2;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_RAW;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_ROWID;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu.XuguDataTypeConvertor.XUGU_VARCHAR2;

@Slf4j
public class XuguCatalog extends AbstractJdbcCatalog {

    private static final XuguDataTypeConvertor DATA_TYPE_CONVERTOR =
            new XuguDataTypeConvertor();

    protected static List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            "APPQOSSYS",
                            "AUDSYS",
                            "CTXSYS",
                            "DVSYS",
                            "DBSFWUSER",
                            "DBSNMP",
                            "GSMADMIN_INTERNAL",
                            "LBACSYS",
                            "MDSYS",
                            "OJVMSYS",
                            "OLAPSYS",
                            "ORDDATA",
                            "ORDSYS",
                            "OUTLN",
                            "SYS",
                            "SYSTEM",
                            "WMSYS",
                            "XDB",
                            "EXFSYS",
                            "SYSMAN"));

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n"
                    + "    cols.COLUMN_NAME,\n"
                    + "    CASE \n"
                    + "        WHEN cols.data_type LIKE 'INTERVAL%%' THEN 'INTERVAL'\n"
                    + "        ELSE REGEXP_SUBSTR(cols.data_type, '^[^(]+')\n"
                    + "    END as TYPE_NAME,\n"
                    + "    cols.data_type || \n"
                    + "        CASE \n"
                    + "            WHEN cols.data_type IN ('VARCHAR2', 'CHAR') THEN '(' || cols.data_length || ')'\n"
                    + "            WHEN cols.data_type IN ('NUMBER') AND cols.data_precision IS NOT NULL AND cols.data_scale IS NOT NULL THEN '(' || cols.data_precision || ', ' || cols.data_scale || ')'\n"
                    + "            WHEN cols.data_type IN ('NUMBER') AND cols.data_precision IS NOT NULL AND cols.data_scale IS NULL THEN '(' || cols.data_precision || ')'\n"
                    + "            WHEN cols.data_type IN ('RAW') THEN '(' || cols.data_length || ')'\n"
                    + "        END AS FULL_TYPE_NAME,\n"
                    + "    cols.data_length AS COLUMN_LENGTH,\n"
                    + "    cols.data_precision AS COLUMN_PRECISION,\n"
                    + "    cols.data_scale AS COLUMN_SCALE,\n"
                    + "    com.comments AS COLUMN_COMMENT,\n"
                    + "    cols.data_default AS DEFAULT_VALUE,\n"
                    + "    CASE cols.nullable WHEN 'N' THEN 'NO' ELSE 'YES' END AS IS_NULLABLE\n"
                    + "FROM\n"
                    + "    all_tab_columns cols\n"
                    + "LEFT JOIN \n"
                    + "    all_col_comments com ON cols.table_name = com.table_name AND cols.column_name = com.column_name AND cols.owner = com.owner\n"
                    + "WHERE \n"
                    + "    cols.owner = '%s'\n"
                    + "    AND cols.table_name = '%s'\n"
                    + "ORDER BY \n"
                    + "    cols.column_id \n";

    public XuguCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT name FROM v$database";
    }

    @Override
    protected String getCreateTableSql(TablePath tablePath, CatalogTable table) {
        return new XuguCreateTableSqlBuilder(table).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT OWNER, TABLE_NAME FROM ALL_TABLES"
                + "  WHERE TABLE_NAME NOT LIKE 'MDRT_%'"
                + "  AND TABLE_NAME NOT LIKE 'MDRS_%'"
                + "  AND TABLE_NAME NOT LIKE 'MDXT_%'"
                + "  AND (TABLE_NAME NOT LIKE 'SYS_IOT_OVER_%' AND IOT_NAME IS NULL)";
    }

    @Override
    protected String getTableName(ResultSet rs) throws SQLException {
        if (EXCLUDED_SCHEMAS.contains(rs.getString(1))) {
            return null;
        }
        return rs.getString(1) + "." + rs.getString(2);
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        String fullTypeName = resultSet.getString("FULL_TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_LENGTH");
        long columnPrecision = resultSet.getLong("COLUMN_PRECISION");
        long columnScale = resultSet.getLong("COLUMN_SCALE");
        String columnComment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
        boolean isNullable = resultSet.getString("IS_NULLABLE").equals("YES");

        SeaTunnelDataType<?> type =
                fromJdbcType(columnName, typeName, columnPrecision, columnScale);
        long bitLen = 0;
        switch (typeName) {
            case XUGU_LONG:
            case XUGU_ROWID:
            case XUGU_NCLOB:
            case XUGU_CLOB:
                columnLength = -1;
                break;
            case XUGU_RAW:
                bitLen = 2000 * 8;
                break;
            case XUGU_BLOB:
            case XUGU_LONG_RAW:
            case XUGU_BFILE:
                bitLen = -1;
                break;
            case XUGU_CHAR:
            case XUGU_NCHAR:
            case XUGU_NVARCHAR2:
            case XUGU_VARCHAR2:
            default:
                break;
        }

        return PhysicalColumn.of(
                columnName,
                type,
                0,
                isNullable,
                defaultValue,
                columnComment,
                fullTypeName,
                false,
                false,
                bitLen,
                null,
                columnLength);
    }

    private SeaTunnelDataType<?> fromJdbcType(
            String columnName, String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(XuguDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(XuguDataTypeConvertor.SCALE, scale);
        return DATA_TYPE_CONVERTOR.toSeaTunnelType(columnName, typeName, dataTypeProperties);
    }

    @Override
    protected String getUrlFromDatabaseName(String databaseName) {
        return defaultUrl;
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            if (StringUtils.isNotBlank(tablePath.getDatabaseName())) {
                return databaseExists(tablePath.getDatabaseName())
                        && listTables(tablePath.getDatabaseName())
                                .contains(tablePath.getSchemaAndTableName());
            }
            return listTables().contains(tablePath.getSchemaAndTableName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    private List<String> listTables() {
        List<String> databases = listDatabases();
        return listTables(databases.get(0));
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new XuguTypeMapper());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\"",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getExistDataSql(TablePath tablePath) {
        return String.format(
                "select * from \"%s\".\"%s\" WHERE rownum = 1",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected List<ConstraintKey> getConstraintKeys(DatabaseMetaData metaData, TablePath tablePath)
            throws SQLException {
        try {
            return getConstraintKeys(
                    metaData,
                    tablePath.getDatabaseName(),
                    tablePath.getSchemaName(),
                    tablePath.getTableName());
        } catch (SQLException e) {
            log.info("Obtain constraint failure", e);
            return new ArrayList<>();
        }
    }
}
