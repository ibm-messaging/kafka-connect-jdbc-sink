package com.ibm.eventstreams.connect.jdbcsink.sink.datasource;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * A data source backed by a connection pool.
 */
public class PooledDataSource implements IDataSource {

    private ComboPooledDataSource datasource;

    private PooledDataSource(
            ComboPooledDataSource dataSource
    )  {
        this.datasource = dataSource;
    }

    @Override public Connection getConnection() throws SQLException {
        return datasource.getConnection();
    }

    public static class Builder {

        private ComboPooledDataSource datasource = new ComboPooledDataSource();

        public Builder(final String username, final String password, final String jdbcUrl, final String driverClass) throws PropertyVetoException {
            this.datasource.setDriverClass(driverClass);
            this.datasource.setJdbcUrl(jdbcUrl);
            this.datasource.setUser(username);
            this.datasource.setPassword(password);
        }

        public IDataSource build() {
            return new PooledDataSource(this.datasource);
        }

        // Optional configurable methods...
        public Builder withInitialPoolSize(int poolSize) {
            this.datasource.setInitialPoolSize(poolSize);
            return this;
        }


    }
}
