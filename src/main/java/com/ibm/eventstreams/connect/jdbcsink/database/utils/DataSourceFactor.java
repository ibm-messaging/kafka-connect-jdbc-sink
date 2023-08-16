/*
 *
 * Copyright 2023 IBM Corporation
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

package com.ibm.eventstreams.connect.jdbcsink.database.utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;

public class DataSourceFactor {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataSourceFactor.class);

    public Connection connection = null;

    public DataSourceFactor(final IDataSource dataSource) {
        try {
            this.connection = dataSource.getConnection();
        } catch (final SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean isPostgreSQL() throws SQLException {
        return connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ENGLISH).contains("postgresql");
    }

    public boolean isDB2() throws SQLException {
        return connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ENGLISH).contains("db2");
    }

    public boolean isMySQL() throws SQLException {
        return connection.getMetaData().getDatabaseProductName().toLowerCase(Locale.ENGLISH).contains("mysql");
    }

    public int getPostgresMajorVersion() {
        try {
            final DatabaseMetaData metaData = connection.getMetaData();
            final String databaseProductName = metaData.getDatabaseProductName();
            if ("PostgreSQL".equals(databaseProductName)) {
                final int majorVersion = metaData.getDatabaseMajorVersion();
                return majorVersion;
            }
        } catch (final SQLException e) {
            e.printStackTrace();
        }
        return 0;
    }

    public boolean doesTableExist(final String tableName) throws SQLException {
        final String[] tableParts = tableName.split("\\.");
        final DatabaseMetaData dbm = connection.getMetaData();
        final ResultSet table = dbm.getTables(null, tableParts[0], tableParts[1], null);
        return table.next();
    }

    public PreparedStatement prepareStatement(final String statement) throws SQLException {
        return connection.prepareStatement(statement);
    }

    public void close() throws SQLException {
        LOGGER.trace("[{}] Entry {}.close, props={}", Thread.currentThread().getId(), this.getClass().getName());
        if (connection != null) {
            connection.close();
        }
        LOGGER.trace("[{}]  Exit {}.close", Thread.currentThread().getId(), this.getClass().getName());
    }
}
