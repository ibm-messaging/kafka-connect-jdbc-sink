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

package com.ibm.eventstreams.connect.jdbcsink.database.datasource;

import com.ibm.eventstreams.connect.jdbcsink.database.DatabaseType;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DatabaseTypeTest {

    @Nested
    class FromJDBCUrl {

        @Test
        void shouldReturnDatabaseTypeForValidJdbcUrl() {
            final String databaseType = "postgresql";
            final String connectionUrl = String.format("jdbc:%s://127.0.0.1:5432/blah", databaseType);

            final DatabaseType type = DatabaseType.fromJdbcUrl(connectionUrl);

            assertEquals(type, DatabaseType.postgresql);
        }

        @Test
        void shouldReturnNullForInValidJdbcUrl() {
            final String databaseType = "postgresql";
            final String connectionUrl = String.format("jdbc://", databaseType);

            final DatabaseType type = DatabaseType.fromJdbcUrl(connectionUrl);

            assertEquals(type, null);
        }

        @Test
        void shouldReturnNullForANotSupportedDatabaseType() {
            final String databaseType = "not supported";
            final String connectionUrl = String.format("jdbc:%s://127.0.0.1:5432/blah", databaseType);

            final DatabaseType type = DatabaseType.fromJdbcUrl(connectionUrl);

            assertEquals(type, null);
        }
    }
}
