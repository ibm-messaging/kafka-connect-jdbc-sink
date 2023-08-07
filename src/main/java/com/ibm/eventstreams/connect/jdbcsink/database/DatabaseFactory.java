/*
 *
 * Copyright 2020, 2023 IBM Corporation
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

package com.ibm.eventstreams.connect.jdbcsink.database;

import java.beans.PropertyVetoException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkConfig;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.PooledDataSource;
import com.ibm.eventstreams.connect.jdbcsink.database.exception.DatabaseNotSupportedException;
import com.ibm.eventstreams.connect.jdbcsink.database.exception.JdbcDriverClassNotFoundException;

public class DatabaseFactory {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseFactory.class);

    public IDatabase makeDatabase(JDBCSinkConfig config) {

        logger.warn("DatabaseFactory: makeDatabase");

        DatabaseType databaseType = getDatabaseType(config);

        String databaseDriver = getDatabaseDriver(databaseType);

        IDataSource dataSource = getDataSource(config, databaseDriver);

        return databaseType.create(dataSource);
    }

    private DatabaseType getDatabaseType(JDBCSinkConfig config) {
        String jdbcUrl = config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL);
        DatabaseType databaseType = DatabaseType.fromJdbcUrl(jdbcUrl);

        if (databaseType == null) {
            throw new DatabaseNotSupportedException("Check " + jdbcUrl);
        }
        return databaseType;
    }

    private IDataSource getDataSource(JDBCSinkConfig config, String databaseDriver) {
        final String username = config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER);
        final String password = config.getPassword(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD).value();
        final int poolSize = config.getInt(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE);
        String jdbcUrl = config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL);

        IDataSource dataSource = null;
        try {
            dataSource = new PooledDataSource.Builder(
                    username,
                    password,
                    jdbcUrl,
                    databaseDriver).withInitialPoolSize(poolSize).build();
        } catch (PropertyVetoException e) {
            logger.error(e.toString());
        }
        return dataSource;
    }

    private String getDatabaseDriver(DatabaseType databaseType) {
        String databaseDriver = databaseType.getDriver();
        try {
            Class.forName(databaseDriver);
        } catch (ClassNotFoundException cnf) {
            logger.error(databaseType.name() + " JDBC driver not found", cnf);
            throw new JdbcDriverClassNotFoundException(databaseType.name());
        }
        return databaseDriver;
    }

}
