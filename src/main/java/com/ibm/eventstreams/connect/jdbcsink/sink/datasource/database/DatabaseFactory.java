package com.ibm.eventstreams.connect.jdbcsink.sink.datasource.database;

import com.ibm.eventstreams.connect.jdbcsink.sink.JDBCSinkConfig;
import com.ibm.eventstreams.connect.jdbcsink.sink.JDBCSinkTask;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.sink.datasource.PooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.beans.PropertyVetoException;

public class DatabaseFactory {
    private static final Logger log = LoggerFactory.getLogger(JDBCSinkTask.class);
    private boolean databaseInsertMode; // TODO: handle this

    public IDatabase makeDatabase(JDBCSinkConfig config) throws PropertyVetoException {

        log.warn("DatabaseFactory: makeDatabase");

        String jdbcUrl = config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_URL);

        DatabaseType database = DatabaseType.fromJdbcUrl(jdbcUrl);

        if (database == null) {
            throw new DatabaseNotSupportedException("Check " + jdbcUrl);
        }

        String databaseDriver = database.getDriver();
        try {
            Class.forName(databaseDriver);
        } catch (ClassNotFoundException cnf) {
            log.error(database.name() + " JDBC driver not found", cnf);
        }

        final String username = config.getString(JDBCSinkConfig.CONFIG_NAME_CONNECTION_USER);
        final String password = config.getPassword(JDBCSinkConfig.CONFIG_NAME_CONNECTION_PASSWORD).toString();
        final int poolSize = config.getInt(JDBCSinkConfig.CONFIG_NAME_CONNECTION_DS_POOL_SIZE);

        IDataSource datasource = new PooledDataSource.Builder(
                username,
                password,
                jdbcUrl,
                databaseDriver
        ).withInitialPoolSize(poolSize).build();

        databaseInsertMode = config.getBoolean(JDBCSinkConfig.CONFIG_NAME_INSERT_MODE_DATABASELEVEL);

        return database.create(datasource);
    }

}
