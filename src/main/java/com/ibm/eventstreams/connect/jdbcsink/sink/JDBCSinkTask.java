package com.ibm.eventstreams.connect.jdbcsink.sink;

import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database.DatabaseFactory;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database.IDatabase;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public class JDBCSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(JDBCSinkTask.class);
    private static final String classname = JDBCSinkTask.class.getName();

    // TODO: needs to be generic and incorporate other database types
    //  needs an interface
    private JDBCSinkConfig config;

    private IDatabase database;

    int remainingRetries; // init max retries via config.maxRetries ...

    /**
     * Start the Task. This should handle any configuration parsing and one-time setup of the task.
     * @param props initial configuration
     */
    @Override public void start(Map<String, String> props) {
        log.trace("[{}] Entry {}.start, props={}", Thread.currentThread().getId(), classname, props);
        this.config = new JDBCSinkConfig(props);

        DatabaseFactory databaseFactory = new DatabaseFactory();
        try {
            this.database = databaseFactory.makeDatabase(this.config);
        } catch (PropertyVetoException e) {
            log.error("Failed to build the database {} ", e);
            e.printStackTrace();
            // TODO: do something else here?
        }

        log.trace("[{}]  Exit {}.start", Thread.currentThread().getId(), classname);
    }

    /**
     * Put the records in the sink.
     *
     * If this operation fails, the SinkTask may throw a {@link org.apache.kafka.connect.errors.RetriableException} to
     * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
     * be stopped immediately. {@link SinkTaskContext#timeout(long)} can be used to set the maximum time before the
     * batch will be retried.
     *
     * @param records the set of records to send
     */
    @Override public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        final SinkRecord first = records.iterator().next();
        final int recordsCount = records.size();
        log.info("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the database...",
                recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()
        );
        try {
            final String tableName = config.getString(JDBCSinkConfig.CONFIG_NAME_TABLE_NAME_FORMAT);
            this.database.getWriter().insert(tableName, records);
        } catch (SQLException sqle) {
            log.warn("Write of {} records failed, remainingRetries={}", records.size(), remainingRetries, sqle);
        }
    }

    @Override public void stop() {
    }

    @Override public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    /**
     * Get the version of this task. Usually this should be the same as the corresponding {@link Connector} class's version.
     *
     * @return the version, formatted as a String
     */
    @Override public String version() {
        return getClass().getPackage().getImplementationVersion();
    }
}