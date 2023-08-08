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

package com.ibm.eventstreams.connect.jdbcsink.database.writer;

import java.sql.BatchUpdateException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.eventstreams.connect.jdbcsink.JDBCSinkTask;
import com.ibm.eventstreams.connect.jdbcsink.database.builder.CommandBuilder;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;
import com.ibm.eventstreams.connect.jdbcsink.database.utils.DataSourceFactor;

public class JDBCWriter implements IDatabaseWriter {

    private static final Logger logger = LoggerFactory.getLogger(JDBCSinkTask.class);

    private final DataSourceFactor dataSourceFactor;
    private final CommandBuilder commandBuilder;

    public JDBCWriter(final IDataSource dataSource) {
        this.dataSourceFactor = new DataSourceFactor(dataSource);
        this.commandBuilder = new CommandBuilder();
    }

    public void createTable(String tableName, Schema schema) throws SQLException {
        logger.trace("[{}] Entry {}.createTable, props={}", Thread.currentThread().getId(), this.getClass().getName());

        final String CREATE_STATEMENT = "CREATE TABLE %s (%s)";

        StringBuilder fieldDefinitions = new StringBuilder();
        fieldDefinitions.append(commandBuilder.getIdColumnDefinition(dataSourceFactor));

        for (Field field : schema.fields()) {
            String fieldName = field.name();
            Schema.Type fieldType = field.schema().type();
            String nullable = field.schema().isOptional() ? "" : " NOT NULL";

            // Add field definitions based on database-specific data types
            if (dataSourceFactor.isPostgreSQL()) {
                fieldDefinitions
                        .append(String.format(", %s %s%s", fieldName, commandBuilder.getPostgreSQLFieldType(fieldType),
                                nullable));
            } else if (dataSourceFactor.isDB2()) {
                fieldDefinitions.append(
                        String.format(", %s %s%s", fieldName, commandBuilder.getDB2FieldType(fieldType), nullable));
            } else if (dataSourceFactor.isMySQL()) {
                fieldDefinitions.append(
                        String.format(", %s %s%s", fieldName, commandBuilder.getMySQLFieldType(fieldType), nullable));
            } else {
                throw new SQLException("Unsupported database type");
            }
        }

        String createTableSql = String.format(CREATE_STATEMENT, tableName, fieldDefinitions.toString());

        logger.debug("Creating table: " + tableName);
        logger.debug("Field definitions: " + fieldDefinitions.toString());
        logger.debug("Final prepared statement: " + createTableSql);

        try (PreparedStatement pstmt = dataSourceFactor.prepareStatement(createTableSql)) {
            pstmt.execute();
        }

        logger.info("Table " + tableName + " has been created");
        logger.trace("[{}]  Exit {}.createTable", Thread.currentThread().getId(), this.getClass().getName());
    }

    @Override
    public void insert(String tableName, Collection<SinkRecord> records) throws SQLException {
        logger.trace("[{}] Entry {}.insert, props={}", Thread.currentThread().getId(), this.getClass().getName());
        try {
            if (!dataSourceFactor.doesTableExist(tableName)) {
                logger.info("Table not found. Creating table: " + tableName);
                createTable(tableName, records.iterator().next().valueSchema());
            }

            List<String> fieldNames = records.iterator().next().valueSchema().fields().stream()
                    .map(Field::name)
                    .collect(Collectors.toList());

            String insertStatement = commandBuilder.buildInsertStatement(tableName, fieldNames);
            logger.debug("Insert Statement: {}", insertStatement);
            PreparedStatement pstmt = dataSourceFactor.prepareStatement(insertStatement);

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
            throw batchUpdateException;
        } catch (SQLException sqlException) {
            logger.error(sqlException.toString());
            throw sqlException;
        } finally {
            dataSourceFactor.close();
        }
        logger.trace("[{}]  Exit {}.insert", Thread.currentThread().getId(), this.getClass().getName());
    }
}
