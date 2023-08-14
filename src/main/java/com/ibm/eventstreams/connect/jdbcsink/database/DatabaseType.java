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

import java.util.EnumSet;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkConnector;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;

/**
 * Database types supported for this sink connector
 * JDBC drivers and urls:
 * https://www.ibm.com/support/knowledgecenter/en/SSEP7J_10.1.1/com.ibm.swg.ba.cognos.vvm_ag_guide.10.1.1.doc/c_ag_samjdcurlform.html
 */
public enum DatabaseType {
    db2("com.ibm.db2.jcc.DB2Driver") {
        @Override
        public IDatabase create(final IDataSource dataSource) {
            return new RelationalDatabase(this, dataSource);
        }
    },
    postgresql("org.postgresql.Driver") {
        @Override
        public IDatabase create(final IDataSource dataSource) {
            return new RelationalDatabase(this, dataSource);
        }
    },
    mysql("com.mysql.cj.jdbc.Driver") {
        @Override
        public IDatabase create(final IDataSource dataSource) {
            return new RelationalDatabase(this, dataSource);
        }
    };

    private static final Logger log = LoggerFactory.getLogger(JDBCSinkConnector.class);
    private String driver;

    DatabaseType(final String value) {
        this.driver = value;
    }

    /**
     * Majority of jdbc urls have the database name as the second argument
     *
     * @param connectionUrl the jdbc connection url
     * @return Optional<DatabaseType>
     */
    public static DatabaseType fromJdbcUrl(final String connectionUrl) {
        final int stringSplitLimit = 3;
        final String jdbcUrlDelimiter = ":";

        final String[] urlSegments = connectionUrl.split(jdbcUrlDelimiter, stringSplitLimit);

        DatabaseType type = null;

        if (urlSegments.length == stringSplitLimit) {
            final String matchedDatabaseType = urlSegments[stringSplitLimit - 2];
            log.info("matchedType = " + matchedDatabaseType);

            type = EnumSet.allOf(DatabaseType.class).stream()
                    .filter(t -> t.name().toLowerCase(Locale.ENGLISH).equals(matchedDatabaseType))
                    .findFirst()
                    .orElse(null);
        }

        log.info("DATABASE TYPE = " + type);
        return type;
    }

    public abstract IDatabase create(IDataSource dataSource);

    public String getDriver() {
        return driver;
    }
}
