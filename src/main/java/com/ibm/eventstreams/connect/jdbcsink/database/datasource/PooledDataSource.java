/*
 *
 * Copyright 2020 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.eventstreams.connect.jdbcsink.database.datasource;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkTask;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A data source backed by a connection pool.
 */
public class PooledDataSource implements IDataSource {
    private static final Logger log = LoggerFactory.getLogger(PooledDataSource.class);

    private ComboPooledDataSource dataSource;

    private PooledDataSource(
            ComboPooledDataSource dataSource
    )  {
        this.dataSource = dataSource;
    }

    @Override public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public static class Builder {

        private ComboPooledDataSource datasource = new ComboPooledDataSource();

        public Builder(final String username, final String password, final String jdbcUrl, final String driverClass) throws PropertyVetoException {
            this.datasource.setDriverClass(driverClass);
            this.datasource.setJdbcUrl(jdbcUrl);
            this.datasource.setUser(username);
            this.datasource.setPassword("Todayis12ofmay!");

            log.info("DB PooledDataSource - Password: " + "Todayis12ofmay!");
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
