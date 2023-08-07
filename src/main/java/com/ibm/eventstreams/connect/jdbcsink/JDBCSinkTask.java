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
    private static final Logger logger = LoggerFactory.getLogger(JDBCSinkTask.class);
    private static final String classname = JDBCSinkTask.class.getName();

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
    public void start(Map<String, String> props) {
        logger.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), classname, props);
        this.config = new JDBCSinkConfig(props);

        DatabaseFactory databaseFactory = getDatabaseFactory();
        try {
            this.database = databaseFactory.makeDatabase(this.config);
        } catch (Exception e) {
            logger.error("Failed to build the database {} ", e);
            throw new ConnectException(e);
        }

        logger.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), classname);
    }

    protected DatabaseFactory getDatabaseFactory() {
        DatabaseFactory databaseFactory = new DatabaseFactory();
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
    public void put(Collection<SinkRecord> records) {
        logger.trace("[{}] Entry {}.put", Thread.currentThread().getId(), classname);
        if (records.isEmpty()) {
            return;
        }

        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        logger.info("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());

        final String tableName = config.getString(JDBCSinkConfig.CONFIG_NAME_TABLE_NAME_FORMAT);

        logger.info("# of records: " + records.size());
        try {
            Instant start = Instant.now();
            this.database.getWriter().insert(tableName, records);
            logger.info(String.format("%d RECORDS PROCESSED", records.size()));
            Instant finish = Instant.now();
            long timeElapsed = Duration.between(start, finish).toMillis(); // in millis
            logger.info(String.format("Processed '%d' records", records.size()));
            logger.info(String.format("Total Execution time: %d", timeElapsed));
        } catch (SQLException error) {
            logger.error("Write of {} records failed, remainingRetries={}", recordsCount, remainingRetries, error);
            throw new ConnectException(error);
        } catch (final RuntimeException e) {
            logger.error("Unexpected runtime exception: ", e);
            throw e;
        }

        logger.trace("[{}] Exit {}.put", Thread.currentThread().getId(), classname);
    }

    @Override
    public void stop() {
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
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
