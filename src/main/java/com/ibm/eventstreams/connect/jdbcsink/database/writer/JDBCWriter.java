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

package com.ibm.eventstreams.connect.jdbcsink.database.writer;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkTask;
import com.ibm.eventstreams.connect.jdbcsink.database.builder.CommandBuilder;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class JDBCWriter implements IDatabaseWriter {

    private static final Logger logger = LoggerFactory.getLogger(JDBCSinkTask.class);

    private final IDataSource dataSource;
    private final CommandBuilder commandBuilder;

    public JDBCWriter(final IDataSource dataSource) {
        this.dataSource = dataSource;
        this.commandBuilder = new CommandBuilder(dataSource);
    }

    public void createTable(Connection connection, String tableName, Schema schema) throws SQLException {
        final String CREATE_STATEMENT = "CREATE TABLE %s (%s)";

        StringBuilder fieldDefinitions = new StringBuilder();
        fieldDefinitions.append(commandBuilder.getIdColumnDefinition());

        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            String nullable = field.schema().isOptional() ? "" : " NOT NULL";

            // Add field definitions based on database-specific data types
            if (commandBuilder.isPostgreSQL()) {
                fieldDefinitions
                        .append(String.format(", %s %s%s", fieldName, commandBuilder.getPostgreSQLFieldType(fieldType), nullable));
            } else if (commandBuilder.isDB2()) {
                fieldDefinitions.append(String.format(", %s %s%s", fieldName, commandBuilder.getDB2FieldType(fieldType), nullable));
            } else if (commandBuilder.isMySQL()) {
                fieldDefinitions.append(String.format(", %s %s%s", fieldName, commandBuilder.getMySQLFieldType(fieldType), nullable));
            } else {
                throw new SQLException("Unsupported database type");
            }
        }

        String createTableSql = String.format(CREATE_STATEMENT, tableName, fieldDefinitions.toString());

        logger.debug("Creating table: " + tableName);
        logger.debug("Field definitions: " + fieldDefinitions.toString());
        logger.debug("Final prepared statement: " + createTableSql);

        try (PreparedStatement pstmt = connection.prepareStatement(createTableSql)) {
            pstmt.execute();
        }

        logger.info("Table " + tableName + " has been created");
    }

    private String buildInsertStatement(String tableName, List<String> fieldNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(tableName);
        sb.append("(").append(String.join(", ", fieldNames)).append(")");
        sb.append(" VALUES ");
        sb.append("(").append(String.join(", ", Collections.nCopies(fieldNames.size(), "?"))).append(")");
        return sb.toString();
    }

    @Override
    public void insert(String tableName, Collection<SinkRecord> records) throws SQLException {
        Connection connection = null;
        try {
            connection = this.dataSource.getConnection();

            if (!commandBuilder.doesTableExist(connection, tableName)) {
                logger.info("Table not found. Creating table: " + tableName);
                createTable(connection, tableName, records.iterator().next().valueSchema());
            }

            List<String> fieldNames = records.iterator().next().valueSchema().fields().stream()
                    .map(Field::name)
                    .collect(Collectors.toList());

            String insertStatement = buildInsertStatement(tableName, fieldNames);
            logger.debug("Insert Statement: {}", insertStatement);
            PreparedStatement pstmt = connection.prepareStatement(insertStatement);

            for (SinkRecord record : records) {
                Struct recordValue = (Struct) record.value();

                List<Object> fieldValues = fieldNames.stream()
                        .map(fieldName -> recordValue.get(fieldName))
                        .collect(Collectors.toList());

                logger.debug("Field values: {}", fieldValues);
                for (int i = 0; i < fieldValues.size(); i++) {
                    pstmt.setObject(i + 1, fieldValues.get(i));
                }

                pstmt.addBatch();
                logger.debug("Record added to batch: {}", record.value());
            }

            int[] batchResults = pstmt.executeBatch();
            logger.debug("Batch execution results: {}", Arrays.toString(batchResults));

            pstmt.close();
        } catch (BatchUpdateException batchUpdateException) {
            logger.error("SOME OPERATIONS IN BATCH FAILED");
            logger.error(batchUpdateException.toString());
        } catch (SQLException sQLException) {
            logger.error(sQLException.toString());
            throw sQLException;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}
