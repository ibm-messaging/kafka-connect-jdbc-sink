package com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database;

import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database.writer.IDatabaseWriter;

public interface IDatabase extends IDataSource {
    IDatabaseWriter getWriter();
    DatabaseType getType();
}
