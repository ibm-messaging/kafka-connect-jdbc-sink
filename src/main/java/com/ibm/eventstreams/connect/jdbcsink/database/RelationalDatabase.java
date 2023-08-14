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

package com.ibm.eventstreams.connect.jdbcsink.database;

import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.database.writer.IDatabaseWriter;
import com.ibm.eventstreams.connect.jdbcsink.database.writer.JDBCWriter;

public class RelationalDatabase implements IDatabase {

    private final IDatabaseWriter writer;
    private final DatabaseType type;

    public RelationalDatabase(final DatabaseType type, final IDataSource dataSource) {
        this.type = type;
        this.writer = new JDBCWriter(dataSource);
    }

    @Override
    public IDatabaseWriter getWriter() {
        return this.writer;
    }

    @Override
    public DatabaseType getType() {
        return this.type;
    }
}
