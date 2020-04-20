package com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database;

import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.PooledDataSource;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database.writer.IDatabaseWriter;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database.writer.JDBCWriter;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import javax.xml.crypto.Data;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

public class RelationalDatabase implements IDatabase {

    private final IDataSource datasource;
    private final IDatabaseWriter writer;
    private final DatabaseType type;

    public RelationalDatabase(DatabaseType type, IDataSource datasource) throws PropertyVetoException {
        // TODO: find SQL statement creator by type and configure writer
        this.datasource = datasource;
        this.type = type;
        this.writer = new JDBCWriter(true, this.datasource);;
    }

    @Override public Connection getConnection() throws SQLException {
        return this.datasource.getConnection();
    }

    @Override public IDatabaseWriter getWriter() {
        return this.writer;
    }

    @Override public DatabaseType getType() {
        return this.type;
    }
}
