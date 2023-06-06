package com.ibm.eventstreams.connect.jdbcsink.database.writer;

import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import com.ibm.db2.jcc.am.DatabaseMetaData;
import com.ibm.eventstreams.connect.jdbcsink.database.datasource.IDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;

import static org.mockito.Mockito.*;

public class JDBCWriterTest {

        @Test
        public void testCreateTable_TableDoesNotExist_CreatesTable() throws SQLException {
                // Prepare test data
                Connection connection = mock(Connection.class);
                IDataSource dataSource = mock(IDataSource.class);
                PreparedStatement preparedStatement = mock(PreparedStatement.class);
                when(dataSource.getConnection()).thenReturn(connection);
                when(connection.prepareStatement("CREATE TABLE ? (?)")).thenReturn(preparedStatement);
                // when(connection.getMetaData()).thenReturn(mock(DatabaseMetaData.class));

                String tableName = "test_table";

                Schema schema = SchemaBuilder.struct()
                                .field("id", Schema.INT32_SCHEMA)
                                .field("name", Schema.STRING_SCHEMA)
                                .build();

                JDBCWriter jdbcWriter = new JDBCWriter(dataSource);

                // Execute the method under test
                jdbcWriter.createTable(connection, tableName, schema);

                // Verify the behavior
                verify(connection).prepareStatement("CREATE TABLE ? (?)");
                verify(preparedStatement).execute();
                verify(preparedStatement).close();
        }

        @Test
        public void testInsert_RecordsExist_InsertsRecords() throws SQLException {
                // Prepare test data
                Connection connection = mock(Connection.class);
                IDataSource dataSource = mock(IDataSource.class);
                DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
                ResultSet resultSet = mock(ResultSet.class);
                when(dataSource.getConnection()).thenReturn(connection);
                when(connection.getMetaData()).thenReturn(databaseMetaData);
                when(databaseMetaData.getTables(any(), any(), any(), any())).thenReturn(resultSet);
                when(resultSet.next()).thenReturn(true);
                when(connection.prepareStatement("INSERT INTO ?(?) VALUES (?)"))
                                .thenReturn(mock(PreparedStatement.class));

                String tableName = "schema.test_table";

                Schema schema = SchemaBuilder.struct()
                                .field("id", Schema.INT32_SCHEMA)
                                .field("name", Schema.STRING_SCHEMA)
                                .build();

                Struct recordValue = new Struct(schema)
                                .put("id", 1)
                                .put("name", "John");

                SinkRecord record = new SinkRecord("topic", 0, null, null, schema, recordValue, 0);

                Collection<SinkRecord> records = Collections.singletonList(record);

                JDBCWriter jdbcWriter = new JDBCWriter(dataSource);

                // Execute the method under test
                jdbcWriter.insert(tableName, records);

                // Verify the behavior
                verify(connection).prepareStatement("INSERT INTO ?(?) VALUES (?)");
                verify(connection).close();
        }

        @Test
        public void testInsert_RecordsExist_CreateAndInsertsRecords() throws SQLException {
                // Prepare test data
                Connection connection = mock(Connection.class);
                IDataSource dataSource = mock(IDataSource.class);
                DatabaseMetaData databaseMetaData = mock(DatabaseMetaData.class);
                ResultSet resultSet = mock(ResultSet.class);
                PreparedStatement preparedStatement = mock(PreparedStatement.class);
                when(dataSource.getConnection()).thenReturn(connection);
                when(connection.getMetaData()).thenReturn(databaseMetaData);
                when(databaseMetaData.getTables(any(), any(), any(), any())).thenReturn(resultSet);
                when(resultSet.next()).thenReturn(false);
                when(connection.prepareStatement("INSERT INTO ?(?) VALUES (?)"))
                                .thenReturn(mock(PreparedStatement.class));
                when(connection.prepareStatement("CREATE TABLE ? (?)")).thenReturn(preparedStatement);

                String tableName = "schema.test_table";

                Schema schema = SchemaBuilder.struct()
                                .field("id", Schema.INT32_SCHEMA)
                                .field("name", Schema.STRING_SCHEMA)
                                .build();

                Struct recordValue = new Struct(schema)
                                .put("id", 1)
                                .put("name", "John");

                SinkRecord record = new SinkRecord("topic", 0, null, null, schema, recordValue, 0);

                Collection<SinkRecord> records = Collections.singletonList(record);

                JDBCWriter jdbcWriter = new JDBCWriter(dataSource);

                // Execute the method under test
                jdbcWriter.insert(tableName, records);

                // Verify the behavior
                verify(connection).prepareStatement("INSERT INTO ?(?) VALUES (?)");
                verify(connection).prepareStatement("CREATE TABLE ? (?)");
                verify(connection).close();
        }
}
