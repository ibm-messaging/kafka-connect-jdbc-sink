package com.ibm.eventstreams.connect.jdbcsink;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JDBCSinkConnectorTest {

    @Mock
    private Task mockTask;

    private JDBCSinkConnector connector;

    @BeforeEach
    void setUp() {
        connector = new JDBCSinkConnector();
    }

    @Test
    void testTaskClass() {
        assertEquals(JDBCSinkTask.class, connector.taskClass());
    }

    @Test
    void testTaskConfigs() {
        Map<String, String> props = new HashMap<>();
        props.put("connection.url", "jdbc:db2://localhost:50000/testdb");
        props.put("connection.user", "db2inst1");
        props.put("connection.password", "password");
        props.put("connection.ds.pool.size", "5");
        props.put("insert.mode.databaselevel", "true");
        props.put("table.name.format", "[schema].[table]");
        props.put("topics", "test");
        props.put("tasks.max", "1");
        props.put("connector.class", "com.ibm.eventstreams.connect.jdbcsink.JDBCSinkConnector");
        connector.start(props);
        int maxTasks = 2;
        List<Map<String, String>> taskConfigs = connector.taskConfigs(maxTasks);
        assertEquals(2, taskConfigs.size());
        assertEquals(props, taskConfigs.get(0));
        assertEquals(props, taskConfigs.get(1));
    }

    @Test
    void testConfig() {
        ConfigDef configDef = connector.config();
        assertEquals(6, configDef.configKeys().size());
    }

    @Test
    void testValidate() {
        JDBCSinkConnector connector = new JDBCSinkConnector();
        Map<String, String> connConfig = new HashMap<>();

        Config taskConfig = connector.validate(connConfig);
        assertEquals(6, taskConfig.configValues().size());
        assertEquals(null, taskConfig.configValues().get(0).value());
        assertEquals(null, taskConfig.configValues().get(1).value());
        assertEquals(null, taskConfig.configValues().get(2).value());
        assertEquals(null, taskConfig.configValues().get(3).value());
        assertEquals(false, taskConfig.configValues().get(4).value());
        assertEquals(1, taskConfig.configValues().get(5).value());
    }
}
