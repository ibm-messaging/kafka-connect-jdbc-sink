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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.beans.PropertyVetoException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkConfig;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.database.exception.DatabaseNotSupportedException;
import com.ibm.eventstreams.connect.jdbcsink.database.exception.JdbcDriverClassNotFoundException;

public class DatabaseFactoryTest {

    @Test
    public void testMakeDatabaseType()
            throws DatabaseNotSupportedException, JdbcDriverClassNotFoundException, PropertyVetoException {
        // Set up the configuration
        // Configure the configuration properties
        final Map<String, String> props = new HashMap<>();

        props.put("table.name.format", "schema.mytable");
        props.put("connection.user", "db2inst1");
        props.put("connection.password", "password");

        for (final String db : new String[] {"db2", "postgresql", "mysql", "sqlserver"}) {
            props.put("connection.url", String.format("jdbc:%s://localhost:50000/sample", db));
            // Create the database factory
            final DatabaseFactory databaseFactory = new DatabaseFactory();

            final JDBCSinkConfig config;
            config = new JDBCSinkConfig(props);

            // Verify that the database is a expected one
            if (db.equals("db2")) {
                final IDatabase database = databaseFactory.makeDatabase(config);
                assertEquals(DatabaseType.db2, database.getType());
            } else if (db.equals("mysql")) {
                final IDatabase database = databaseFactory.makeDatabase(config);
                assertEquals(DatabaseType.mysql, database.getType());
            } else if (db.equals("postgresql")) {
                final IDatabase database = databaseFactory.makeDatabase(config);
                assertEquals(DatabaseType.postgresql, database.getType());
            } else if (db.equals("sqlserver")) {
                // catch DatabaseNotSupportedException
                assertThrows(DatabaseNotSupportedException.class, () -> {
                    databaseFactory.makeDatabase(config);
                });
            }
        }
    }

    @Test
    public void testMakeDatabase() {
        // Prepare test data
        final JDBCSinkConfig config = mock(JDBCSinkConfig.class);
        when(config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL)).thenReturn("jdbc:db2://localhost:3306/test");
        when(config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER)).thenReturn("username");
        when(config.getPassword(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD)).thenReturn(new Password("password"));
        when(config.getInt(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE)).thenReturn(10);

        // Create a mock for the IDataSource
        final IDataSource dataSource = mock(IDataSource.class);

        // Create a mock for the DatabaseType
        final DatabaseType databaseType = mock(DatabaseType.class);
        when(databaseType.getDriver()).thenReturn("com.ibm.db2.jcc.DB2Driver");
        when(databaseType.create(dataSource)).thenReturn(mock(IDatabase.class));

        // Create a mock for the DatabaseFactory
        final DatabaseFactory databaseFactory = mock(DatabaseFactory.class);
        doCallRealMethod().when(databaseFactory).makeDatabase(config);

        // Test the makeDatabase method
        final IDatabase database = databaseFactory.makeDatabase(config);

        // Verify the interactions and assertions
        verify(config, times(2)).getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL);
        verify(config).getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER);
        verify(config).getPassword(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD);
        verify(config).getInt(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE);
        assertNotNull(database);
    }
}
