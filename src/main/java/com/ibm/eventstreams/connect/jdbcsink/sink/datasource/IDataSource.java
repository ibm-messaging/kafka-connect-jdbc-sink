package com.ibm.eventstreams.connect.jdbcsink.sink.datasource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Exposes needed methods from the JDBC ecosystem such
 * as C3P0 connection pooling.
 */
public interface IDataSource {
    Connection getConnection() throws SQLException;
}
