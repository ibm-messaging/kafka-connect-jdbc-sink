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

package com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database;

import com.ibm.eventstreams.connect.jdbcsink.sink.JDBCSinkConfig;
import com.ibm.eventstreams.connect.jdbcsink.sink.JDBCSinkTask;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.PooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;

public class DatabaseFactory {
    private static final Logger log = LoggerFactory.getLogger(JDBCSinkTask.class);

    public IDatabase makeDatabase(JDBCSinkConfig config) throws PropertyVetoException {

        log.warn("DatabaseFactory: makeDatabase");

        String jdbcUrl = config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL);

        DatabaseType database = DatabaseType.fromJdbcUrl(jdbcUrl);

        if (database == null) {
            throw new DatabaseNotSupportedException("Check " + jdbcUrl);
        }

        String databaseDriver = database.getDriver();
        try {
            Class.forName(databaseDriver);
        } catch (ClassNotFoundException cnf) {
            log.error(database.name() + " JDBC driver not found", cnf);
        }

        final String username = config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER);
        final String password = config.getPassword(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD).toString();
        final int poolSize = config.getInt(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE);

        IDataSource datasource = new PooledDataSource.Builder(
                username,
                password,
                jdbcUrl,
                databaseDriver
        ).withInitialPoolSize(poolSize).build();

        return database.create(datasource);
    }

}
