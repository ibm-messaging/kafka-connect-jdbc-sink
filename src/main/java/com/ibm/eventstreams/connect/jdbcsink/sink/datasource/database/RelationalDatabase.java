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
        this.writer = new JDBCWriter(this.datasource);;
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
