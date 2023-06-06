package com.ibm.eventstreams.connect.jdbcsink.database;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkConfig;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.database.exception.DatabaseNotSupportedException;
import com.ibm.eventstreams.connect.jdbcsink.database.exception.JdbcDriverClassNotFoundException;

import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.beans.PropertyVetoException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatabaseFactoryTest {

    @Test
    public void testMakeDatabaseType()
            throws DatabaseNotSupportedException, JdbcDriverClassNotFoundException, PropertyVetoException {
        // Set up the configuration
        // Configure the configuration properties
        Map<String, String> props = new HashMap<>();

        props.put("table.name.format", "schema.mytable");
        props.put("connection.user", "db2inst1");
        props.put("connection.password", "password");

        for (String db : new String[] { "db2", "postgresql", "sqlserver" }) {
            props.put("connection.url", String.format("jdbc:%s://localhost:50000/sample", db));
            // Create the database factory
            DatabaseFactory databaseFactory = new DatabaseFactory();

            JDBCSinkConfig config;
            config = new JDBCSinkConfig(props);

            // Verify that the database is a expected one
            if (db.equals("db2")) {
                IDatabase database = databaseFactory.makeDatabase(config);
                assertEquals(DatabaseType.db2, database.getType());
            } else if (db.equals("mysql")) {
                // catch JdbcDriverClassNotFoundException
                assertThrows(JdbcDriverClassNotFoundException.class, () -> {
                    IDatabase database = databaseFactory.makeDatabase(config);
                    assertEquals(DatabaseType.mysql, database.getType());
                });
            } else if (db.equals("postgresql")) {
                IDatabase database = databaseFactory.makeDatabase(config);
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
        JDBCSinkConfig config = Mockito.mock(JDBCSinkConfig.class);
        when(config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL)).thenReturn("jdbc:db2://localhost:3306/test");
        when(config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER)).thenReturn("username");
        when(config.getPassword(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD)).thenReturn(new Password("password"));
        when(config.getInt(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE)).thenReturn(10);

        // Create a mock for the IDataSource
        IDataSource dataSource = Mockito.mock(IDataSource.class);

        // Create a mock for the DatabaseType
        DatabaseType databaseType = Mockito.mock(DatabaseType.class);
        when(databaseType.getDriver()).thenReturn("com.ibm.db2.jcc.DB2Driver");
        when(databaseType.create(dataSource)).thenReturn(Mockito.mock(IDatabase.class));

        // Create a mock for the DatabaseFactory
        DatabaseFactory databaseFactory = Mockito.mock(DatabaseFactory.class);
        doCallRealMethod().when(databaseFactory).makeDatabase(config);

        // Test the makeDatabase method
        IDatabase database = databaseFactory.makeDatabase(config);

        // Verify the interactions and assertions
        verify(config).getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL);
        verify(config).getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER);
        verify(config).getPassword(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD);
        verify(config).getInt(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE);
        assertNotNull(database);
    }
}
