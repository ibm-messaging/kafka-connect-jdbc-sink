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

package com.ibm.eventstreams.connect.jdbcsink;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class JDBCSinkConnector extends SinkConnector {
    // TODO: check with ibm-messaging about externalizing snapshot version
    public static String VERSION = "1.0.0-SNAPSHOT";

    private Map<String, String> props;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return VERSION;
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector,
     * i.e. it has either just been instantiated and initialized or {@link #stop()}
     * has been invoked.
     *
     * @param props configuration settings
     */
    @Override
    public void start(Map<String, String> props) {
        this.props = props;
    }

    /**
     * Returns the Task implementation for this Connector.
     */
    @Override
    public Class<? extends Task> taskClass() {
        return JDBCSinkTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most count configurations.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for Tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return Collections.nCopies(maxTasks, props);
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }

    /**
     * Define the configuration for the connector.
     *
     * @return The ConfigDef for this connector.
     */
    @Override
    public ConfigDef config() {
        return JDBCSinkConfig.config();
    }

    /**
     * Provides a default validation implementation which returns a list of allowed
     * configurations together with configuration errors and recommended values for
     * each configuration.
     *
     * @param connectorConfigs connector configuration values
     * @return list of allowed configurations
     */
    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
    }

}