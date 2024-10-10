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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2TypeConverter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.db2.DB2TypeMapper;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@Slf4j
public class DB2Catalog extends AbstractJdbcCatalog {

    protected static List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            "SYSTOOLS",
                            "SYSSTAT",
                            "SYSIBMTS",
                            "SYSPUBLIC",
                            "SYSPROC",
                            "SYSIBMINTERNAL",
                            "SYSIBMADM",
                            "SYSIBM",
                            "SYSFUN",
                            "SYSCAT",
                            "SQLJ",
                            "NULLID"));

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            " SELECT COLNAME  AS COLUMN_NAME\n"
                    + "\t,TYPENAME AS TYPE_NAME\n"
                    + "\t,TYPENAME AS FULL_TYPE_NAME \n"
                    + "\t,LENGTH AS COLUMN_LENGTH\n"
                    + "\t,LENGTH AS COLUMN_PRECISION\n"
                    + "\t,SCALE AS COLUMN_SCALE\n"
                    + "\t,REMARKS AS  COLUMN_COMMENT\n"
                    + "\t,DEFAULT AS DEFAULT_VALUE\n"
                    + "\t,CASE \n"
                    + "\t\tWHEN \tNULLS = 'Y' THEN 'YES'\n"
                    + "\t\tELSE 'NO'\n"
                    + "\tEND AS IS_NULLABLE\t\n"
                    + "FROM SYSCAT.COLUMNS  WHERE TABSCHEMA = '%s' AND TABNAME = '%s';";

    public DB2Catalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT CURRENT_SERVER FROM SYSIBM.SYSDUMMY1";
    }

    @Override
    protected String getDatabaseWithConditionSql(String databaseName) {
        return String.format(getListDatabaseSql() + " where CURRENT_SERVER = '%s';", databaseName);
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return new DB2CreateTableSqlBuilder(table, createIndex).build(tablePath);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getSchemaAndTableName("\""));
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT CREATOR ,NAME FROM SYSIBM.SYSTABLES";
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + " WHERE CREATOR = '%s' and NAME = '%s';",
                tablePath.getSchemaName(),
                tablePath.getTableName());
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
        int columnScale = resultSet.getInt("COLUMN_SCALE");
        String columnComment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
        boolean isNullable = resultSet.getString("IS_NULLABLE").equals("YES");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .columnType(typeName)
                        .dataType(typeName)
                        .length(columnLength)
                        .precision(columnPrecision)
                        .scale(columnScale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(columnComment)
                        .build();
        return DB2TypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getOptionTableName(TablePath tablePath) {
        return tablePath.getSchemaAndTableName();
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new DB2TypeMapper());
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) {
        return String.format(
                "TRUNCATE TABLE \"%s\".\"%s\" IMMEDIATE",
                tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected String getExistDataSql(TablePath tablePath) {
        return String.format(
                "select * from \"%s\".\"%s\" limit 1",
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
