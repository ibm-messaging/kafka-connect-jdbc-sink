package com.ibm.eventstreams.connect.jdbcsink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

class JDBCSinkConfigTest {

    @Test
    void testValidConfig() {
        // Create a map with valid configuration properties
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:mysql://localhost:3306/mydatabase");
        props.put("connection.user", "myuser");
        props.put("connection.password", "mypassword");
        props.put("table.name.format", "mytable");
        props.put("connection.ds.pool.size", "5");
        props.put("insert.mode.databaselevel", "true");

        // Create a new JDBCSinkConfig instance
        JDBCSinkConfig config = new JDBCSinkConfig(props);

        // Verify the values of the configuration properties
        assertEquals("jdbc:mysql://localhost:3306/mydatabase", config.getString("connection.url"));
        assertEquals("myuser", config.getString("connection.user"));
        assertEquals("mypassword", config.getPassword("connection.password").value());
        assertEquals("mytable", config.getString("table.name.format"));
        assertEquals(5, config.getInt("connection.ds.pool.size"));
        assertTrue(config.getBoolean("insert.mode.databaselevel"));
    }

    @Test
    void testMissingRequiredConfig() {
        // Create a map with missing required configuration properties
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:mysql://localhost:3306/mydatabase");
        props.put("connection.user", "myuser");
        // Missing "connection.password" and "table.name.format"

        // Create a new JDBCSinkConfig instance and expect a ConfigException to be
        // thrown
        assertThrows(ConfigException.class, () -> new JDBCSinkConfig(props));
    }

}
