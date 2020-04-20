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

package com.ibm.eventstreams.connect.jdbcsink.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class JDBCSinkConfig extends AbstractConfig {

    private static final String CONFIG_CONNECTION_GROUP = "Connection";

    public static final String CONFIG_NAME_CONNECTION_URL = "connection.url";
    private static final String CONFIG_DOCUMENTATION_CONNECTION_URL = "The hostname:port to connect to JDBC.";
    private static final String CONFIG_DISPLAY_CONNECTION_URL = "JDBC URL";

    public static final String CONFIG_NAME_CONNECTION_USER = "connection.user";
    private static final String CONFIG_DOCUMENTATION_CONNECTION_USER = "The user name for authenticating with JDBC.";
    private static final String CONFIG_DISPLAY_CONNECTION_USER = "JDBC User";

    public static final String CONFIG_NAME_CONNECTION_PASSWORD = "connection.password";
    private static final String CONFIG_DOCUMENTATION_CONNECTION_PASSWORD = "The password for authenticating with JDBC.";
    private static final String CONFIG_DISPLAY_CONNECTION_PASSWORD = "JDBC Password";

    public static final String CONFIG_NAME_CONNECTION_DS_POOL_SIZE = "connection.ds.pool.size";
    private static final String CONFIG_DOCUMENTATION_CONNECTION_DS_POOL_SIZE = "The number of connections maintained by the JDBC pool.";
    private static final String CONFIG_DISPLAY_CONNECTION_DS_POOL_SIZE = "JDBC datasource connection pool size";

    static final String CONFIG_NAME_TABLE_NAME_FORMAT = "table.name.format";
    private static final String CONFIG_DOCUMENTATION_TABLE_NAME_FORMAT = "A format string for the destination table name.";
    private static final String CONFIG_DISPLAY_TABLE_NAME_FORMAT = "Table format name";

    public static final String CONFIG_NAME_INSERT_MODE_DATABASELEVEL = "insert.mode.databaselevel";
    private static final String CONFIG_DOCUMENTATION_INSERT_MODE_DATABASELEVEL = "The insertion mode to use (ex: insert, upsert, or update).";
    private static final String CONFIG_DISPLAY_INSERT_MODE_DATABASELEVEL  = "Insert mode database level";

    public static ConfigDef config() {
        ConfigDef config = new ConfigDef();

        config.define(CONFIG_NAME_CONNECTION_URL,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                CONFIG_DOCUMENTATION_CONNECTION_URL,
                CONFIG_CONNECTION_GROUP,
                1,
                ConfigDef.Width.LONG,
                CONFIG_DISPLAY_CONNECTION_URL);

        config.define(CONFIG_NAME_CONNECTION_USER,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.HIGH,
                CONFIG_DOCUMENTATION_CONNECTION_USER,
                CONFIG_CONNECTION_GROUP,
                2,
                ConfigDef.Width.MEDIUM,
                CONFIG_DISPLAY_CONNECTION_USER);

        config.define(CONFIG_NAME_CONNECTION_PASSWORD,
                ConfigDef.Type.PASSWORD,
                null,
                ConfigDef.Importance.HIGH,
                CONFIG_DOCUMENTATION_CONNECTION_PASSWORD,
                CONFIG_CONNECTION_GROUP,
                3,
                ConfigDef.Width.MEDIUM,
                CONFIG_DISPLAY_CONNECTION_PASSWORD);

        config.define(CONFIG_NAME_TABLE_NAME_FORMAT,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                ConfigDef.Importance.HIGH,
                CONFIG_DOCUMENTATION_TABLE_NAME_FORMAT,
                CONFIG_CONNECTION_GROUP,
                4,
                ConfigDef.Width.LONG,
                CONFIG_DISPLAY_TABLE_NAME_FORMAT);

        config.define(CONFIG_NAME_CONNECTION_DS_POOL_SIZE,
                ConfigDef.Type.INT,
                1,
                ConfigDef.Importance.LOW,
                CONFIG_DOCUMENTATION_CONNECTION_DS_POOL_SIZE,
                CONFIG_CONNECTION_GROUP,
                5,
                ConfigDef.Width.MEDIUM,
                CONFIG_DISPLAY_CONNECTION_DS_POOL_SIZE);

        config.define(CONFIG_NAME_INSERT_MODE_DATABASELEVEL,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.LOW,
                CONFIG_DOCUMENTATION_INSERT_MODE_DATABASELEVEL,
                CONFIG_CONNECTION_GROUP,
                6,
                ConfigDef.Width.MEDIUM,
                CONFIG_DISPLAY_INSERT_MODE_DATABASELEVEL);

        return config;
    }

    protected JDBCSinkConfig(final Map<?, ?> props) {
        super(config(), props);
    }

}
