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

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NonNull;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SybaseJdbcConnectionProvider extends SimpleJdbcConnectionProvider {

    private static final Logger log = LoggerFactory.getLogger(SybaseJdbcConnectionProvider.class);

    public SybaseJdbcConnectionProvider(@NonNull JdbcConnectionConfig jdbcConfig) {
        super(jdbcConfig);
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        boolean isValid = false;
        if (connection == null) {
            return false;
        }
        Statement statement = connection.createStatement();
        isValid = statement.execute("select 1");
        statement.close();
        return connection != null && isValid;
    }

    @Override
    public Connection getOrEstablishConnection() throws SQLException, ClassNotFoundException {
        if (this.isConnectionValid()) {
            return connection;
        }
        Driver driver = getLoadedDriver();
        Properties info = new Properties();
        if (jdbcConfig.getUsername().isPresent()) {
            info.setProperty("user", jdbcConfig.getUsername().get());
        }
        if (jdbcConfig.getPassword().isPresent()) {
            info.setProperty("password", jdbcConfig.getPassword().get());
        }
        info.putAll(jdbcConfig.getProperties());
        connection = driver.connect(jdbcConfig.getUrl(), info);
        if (connection == null) {
            // Throw same exception as DriverManager.getConnection when no driver found to match
            // caller expectation.
            throw new JdbcConnectorException(
                    JdbcConnectorErrorCode.NO_SUITABLE_DRIVER,
                    "No suitable driver found for " + jdbcConfig.getUrl());
        }

        connection.setAutoCommit(jdbcConfig.isAutoCommit());

        return connection;
    }

    @Override
    public Connection reestablishConnection() throws SQLException, ClassNotFoundException {
        closeConnection();
        return this.getOrEstablishConnection();
    }
}
