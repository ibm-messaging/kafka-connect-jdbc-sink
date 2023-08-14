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
        final Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:mysql://localhost:3306/mydatabase");
        props.put("connection.user", "myuser");
        props.put("connection.password", "mypassword");
        props.put("table.name.format", "mytable");
        props.put("connection.ds.pool.size", "5");
        props.put("insert.mode.databaselevel", "true");

        // Create a new JDBCSinkConfig instance
        final JDBCSinkConfig config = new JDBCSinkConfig(props);

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
        final Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:mysql://localhost:3306/mydatabase");
        props.put("connection.user", "myuser");
        // Missing "connection.password" and "table.name.format"

        // Create a new JDBCSinkConfig instance and expect a ConfigException to be
        // thrown
        assertThrows(ConfigException.class, () -> new JDBCSinkConfig(props));
    }

}
