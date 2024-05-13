package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sybase;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.SimpleJdbcConnectionPoolProviderProxy;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.ConnectionPoolManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SybaseJdbcConnectionPoolProviderProxy extends SimpleJdbcConnectionPoolProviderProxy {
    public SybaseJdbcConnectionPoolProviderProxy(ConnectionPoolManager poolManager, JdbcConnectionConfig jdbcConfig, int queueIndex) {
        super(poolManager, jdbcConfig, queueIndex);
    }

    @Override
    public boolean isConnectionValid() throws SQLException {
        Connection connection = this.poolManager.getConnection(queueIndex);
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
    public Connection getOrEstablishConnection() {
        return this.poolManager.getSybaseConnection(queueIndex);
    }

}
