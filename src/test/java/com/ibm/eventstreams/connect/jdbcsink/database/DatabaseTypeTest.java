package com.ibm.eventstreams.connect.jdbcsink.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;

public class DatabaseTypeTest {

    @Test
    public void testFromJdbcUrl_ValidUrl_ReturnsCorrectDatabaseType() {
        // Prepare test data
        String connectionUrl = "jdbc:postgresql://localhost:5432/mydatabase";

        // Test the fromJdbcUrl method
        DatabaseType result = DatabaseType.fromJdbcUrl(connectionUrl);

        // Verify the result
        assertEquals(DatabaseType.postgresql, result);
    }

    @Test
    public void testFromJdbcUrl_InvalidUrl_ReturnsNull() {
        // Prepare test data
        String connectionUrl = "jdbc:oracle://localhost:1521/mydatabase";

        // Test the fromJdbcUrl method
        DatabaseType result = DatabaseType.fromJdbcUrl(connectionUrl);

        // Verify the result
        assertNull(result);
    }

    @Test
    public void testCreate_ValidDataSource_ReturnsRelationalDatabase() {
        // Prepare test data
        IDataSource dataSource = mock(IDataSource.class);

        // Test the create method for each database type
        for (DatabaseType databaseType : DatabaseType.values()) {
            IDatabase result = databaseType.create(dataSource);

            // Verify the result
            assertNotNull(result);
            assertTrue(result instanceof RelationalDatabase);
            assertEquals(databaseType, result.getType());
        }
    }

    @Test
    public void testGetDriver_ReturnsCorrectDriver() {
        // Test the getDriver method for each database type
        assertEquals("com.ibm.db2.jcc.DB2Driver", DatabaseType.db2.getDriver());
        assertEquals("org.postgresql.Driver", DatabaseType.postgresql.getDriver());
        assertEquals("com.mysql.cj.jdbc.Driver", DatabaseType.mysql.getDriver());
    }
}
