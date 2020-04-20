package com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database.writer;

import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.SQLException;
import java.util.Collection;

public interface IDatabaseWriter {

    // TODO: handle upserting / idempotency to prevent insertion of duplicate records
    boolean insert(final String tableName, final Collection<SinkRecord> records) throws SQLException;


}
