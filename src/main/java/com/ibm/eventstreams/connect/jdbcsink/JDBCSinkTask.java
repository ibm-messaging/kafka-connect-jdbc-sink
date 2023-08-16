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

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.connector.Connector;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.jdbcsink.database.DatabaseFactory;
import com.ibm.eventstreams.connect.jdbcsink.database.IDatabase;

public class JDBCSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(JDBCSinkTask.class);
    private static final String CLASSNAME = JDBCSinkTask.class.getName();

    // TODO: needs to be generic and incorporate other database types
    // needs an interface
    private JDBCSinkConfig config;

    public IDatabase database;

    int remainingRetries; // init max retries via config.maxRetries ...

    /**
     * Start the Task. This should handle any configuration parsing and one-time
     * setup of the task.
     *
     * @param props initial configuration
     */
    @Override
    public void start(final Map<String, String> props) {
        LOGGER.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), CLASSNAME, props);
        this.config = new JDBCSinkConfig(props);

        final DatabaseFactory databaseFactory = getDatabaseFactory();
        try {
            this.database = databaseFactory.makeDatabase(this.config);
        } catch (final Exception e) {
            LOGGER.error("Failed to build the database {} ", e);
            throw new ConnectException(e);
        }

        LOGGER.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), CLASSNAME);
    }

    protected DatabaseFactory getDatabaseFactory() {
        final DatabaseFactory databaseFactory = new DatabaseFactory();
        return databaseFactory;
    }

    /**
     * Put the records in the sink.
     *
     * If this operation fails, the SinkTask may throw a
     * {@link org.apache.kafka.connect.errors.RetriableException} to indicate that
     * the framework should attempt to retry the same call again. Other exceptions
     * will cause the task to be stopped immediately.
     * {@link SinkTaskContext#timeout(long)} can be used to set the maximum time
     * before the batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override
    public void put(final Collection<SinkRecord> records) {
        LOGGER.trace("[{}] Entry {}.put", Thread.currentThread().getId(), CLASSNAME);
        if (records.isEmpty()) {
            return;
        }

        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        LOGGER.info("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

        final String tableName = config.getString(JDBCSinkConfig.CONFIG_NAME_TABLE_NAME_FORMAT);

        LOGGER.info("# of records: " + records.size());
        try {
            final Instant start = Instant.now();
            this.database.getWriter().insert(tableName, records);
            LOGGER.info(String.format("%d RECORDS PROCESSED", records.size()));
            final Instant finish = Instant.now();
            final long timeElapsed = Duration.between(start, finish).toMillis(); // in millis
            LOGGER.info(String.format("Processed '%d' records", records.size()));
            LOGGER.info(String.format("Total Execution time: %d", timeElapsed));
        } catch (final SQLException error) {
            LOGGER.error("Write of {} records failed, remainingRetries={}", recordsCount, remainingRetries, error);
            throw new ConnectException(error);
        } catch (final RuntimeException e) {
            LOGGER.error("Unexpected runtime exception: ", e);
            throw e;
        }

        LOGGER.trace("[{}] Exit {}.put", Thread.currentThread().getId(), CLASSNAME);
    }

    @Override
    public void stop() {
    }

    @Override
    public void flush(final Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    /**
     * Get the version of this task. Usually this should be the same as the
     * corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override
    public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
}
