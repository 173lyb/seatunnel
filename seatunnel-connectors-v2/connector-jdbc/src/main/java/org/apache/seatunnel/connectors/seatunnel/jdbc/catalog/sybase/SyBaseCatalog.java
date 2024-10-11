/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.utils.CatalogUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sybase.SyBaseTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sybase.SybaseTypeConverter;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.apache.seatunnel.common.exception.CommonErrorCode.UNSUPPORTED_METHOD;

@Slf4j
public class SyBaseCatalog extends AbstractJdbcCatalog {;

    private static final String SELECT_COLUMNS_SQL_TEMPLATE =
            "SELECT\n"
                    + "A.name table_name,\n"
                    + "C.name column_name,\n"
                    + "NULl as comment,\n"
                    + "C.colid,\n"
                    + "D.name column_type,\n"
                    + "C.length max_length,\n"
                    + "C.prec precision,\n"
                    + "C.scale as scale,\n"
                    + "case C.status when 8 then 1 else 0\n"
                    + "\tend is_nullable,\n"
                    + "E.text as \tdefault_value\n"
                    + "FROM\n"
                    + "dbo.sysobjects A\n"
                    + "inner JOIN\n"
                    + "sysusers B\n"
                    + "ON\n"
                    + "A.uid=B.uid\n"
                    + "AND A.type='U'\n"
                    + "AND B.name='%s'\n"
                    + "inner JOIN\n"
                    + "syscolumns C\n"
                    + "ON\n"
                    + "A.id=C.id\n"
                    + "inner JOIN\n"
                    + "dbo.systypes D\n"
                    + "ON\n"
                    + "C.usertype=D.usertype\n"
                    + "LEFT  JOIN \n"
                    + "dbo.syscomments E\n"
                    + "ON\n"
                    + "C.cdefault = E.id\n"
                    + "WHERE 1=1\n"
                    + "and A.name = '%s'\n"
                    + "ORDER BY\n"
                    + "A.type,\n"
                    + "B.name,\n"
                    + "A.name";

    public SyBaseCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    protected String getListDatabaseSql() {
        return "SELECT db_name()";
    }

    /** 重写databaseExists方法，因为SELECT db_name()不支持where */
    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (StringUtils.isBlank(databaseName)) {
            return false;
        }
        try {
            return querySQLResultExists(getUrlFromDatabaseName(databaseName), getListDatabaseSql());
        } catch (SeaTunnelRuntimeException e) {
            if (e.getSeaTunnelErrorCode().getCode().equals(UNSUPPORTED_METHOD.getCode())) {
                log.warn(
                        "The catalog: {} is not supported the getListDatabaseSql for databaseExists",
                        this.catalogName);
                return listDatabases().contains(databaseName);
            }
            throw e;
        } catch (SQLException e) {
            throw new SeaTunnelException("查询kingbase数据库列表异常", e);
        }
    }

    @Override
    protected String getListTableSql(String databaseName) {
        return "SELECT \n"
                + "    B.name ,\n"
                + "    A.name \n"
                + "FROM \n"
                + "dbo.sysobjects A\n"
                + "inner JOIN\n"
                + "sysusers B\n"
                + "ON\n"
                + "A.uid=B.uid";
    }

    @Override
    protected String getTableWithConditionSql(TablePath tablePath) {
        return String.format(
                getListTableSql(tablePath.getDatabaseName())
                        + "  where B.name = '%s' and A.name = '%s'",
                tablePath.getSchemaName(),
                tablePath.getTableName());
    }

    @Override
    protected String getSelectColumnsSql(TablePath tablePath) {
        return String.format(
                SELECT_COLUMNS_SQL_TEMPLATE, tablePath.getSchemaName(), tablePath.getTableName());
    }

    @Override
    protected Column buildColumn(ResultSet resultSet) throws SQLException {
        String columnName = resultSet.getString("column_name");
        String dataType = resultSet.getString("type");
        int precision = resultSet.getInt("precision");
        int scale = resultSet.getInt("scale");
        long columnLength = resultSet.getLong("max_length");
        String comment = resultSet.getString("comment");
        Object defaultValue = resultSet.getObject("default_value");
        boolean isNullable = resultSet.getBoolean("is_nullable");

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(columnName)
                        .dataType(dataType)
                        .length(columnLength)
                        .precision((long) precision)
                        .scale(scale)
                        .nullable(isNullable)
                        .defaultValue(defaultValue)
                        .comment(comment)
                        .build();
        return SybaseTypeConverter.INSTANCE.convert(typeDefine);
    }

    @Override
    protected String getCreateTableSql(
            TablePath tablePath, CatalogTable table, boolean createIndex) {
        return SyBaseCreateTableSqlBuilder.builder(tablePath, table, createIndex)
                .build(tablePath, table);
    }

    @Override
    protected String getDropTableSql(TablePath tablePath) {
        return String.format("DROP TABLE %s", tablePath.getFullName());
    }

    @Override
    protected String getCreateDatabaseSql(String databaseName) {
        return String.format("CREATE DATABASE %s", databaseName);
    }

    @Override
    protected String getDropDatabaseSql(String databaseName) {
        return String.format("DROP DATABASE %s;", databaseName);
    }

    @Override
    protected void dropDatabaseInternal(String databaseName) throws CatalogException {
        closeDatabaseConnection(databaseName);
        super.dropDatabaseInternal(databaseName);
    }

    @Override
    public CatalogTable getTable(String sqlQuery) throws SQLException {
        Connection defaultConnection = getConnection(defaultUrl);
        return CatalogUtils.getCatalogTable(defaultConnection, sqlQuery, new SyBaseTypeMapper());
    }

    @Override
    public String getExistDataSql(TablePath tablePath) {
        return String.format("select TOP 1 * from %s ;", tablePath.getFullNameWithQuoted("[", "]"));
    }

    @Override
    protected String getTruncateTableSql(TablePath tablePath) throws CatalogException {
        return String.format("TRUNCATE TABLE  %s", tablePath.getFullNameWithQuoted("[", "]"));
    }
}
