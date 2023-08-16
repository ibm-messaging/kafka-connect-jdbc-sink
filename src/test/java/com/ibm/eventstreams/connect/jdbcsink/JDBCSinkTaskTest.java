/*
 *
 * Copyright 2023 IBM Corporation
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

package com.ibm.eventstreams.connect.jdbcsink;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ibm.eventstreams.connect.jdbcsink.database.DatabaseFactory;
import com.ibm.eventstreams.connect.jdbcsink.database.IDatabase;
import com.ibm.eventstreams.connect.jdbcsink.database.writer.IDatabaseWriter;

class JDBCSinkTaskTest {

    private JDBCSinkTask task;
    private SinkTaskContext context;
    public Map<String, String> generalConnectorProps = new HashMap<>();

    @BeforeEach
    void setUp() {
        task = spy(new JDBCSinkTask());
        context = mock(SinkTaskContext.class);
        task.initialize(context);

        // Configure the configuration properties
        generalConnectorProps.put("table.name.format", "schema.mytable");
        generalConnectorProps.put("connection.url", "jdbc:db2://localhost:50000/sample");
        generalConnectorProps.put("connection.user", "db2inst1");
        generalConnectorProps.put("connection.password", "password");

        task.database = spy(IDatabase.class);
        when(task.database.getWriter()).thenReturn(spy(IDatabaseWriter.class));

    }

    @Test
    void testPut() throws SQLException {
        final DatabaseFactory databaseFactoryMock = mock(DatabaseFactory.class);
        when(task.getDatabaseFactory()).thenReturn(databaseFactoryMock);
        when(databaseFactoryMock.makeDatabase(any(JDBCSinkConfig.class))).thenReturn(mock(IDatabase.class));
        // Call the put() method
        task.start(generalConnectorProps);

        // Create a collection of SinkRecords
        final Collection<SinkRecord> records = new ArrayList<>();
        final SinkRecord record1 = new SinkRecord("topic1", 0, null, null, null, null, 0);
        final SinkRecord record2 = new SinkRecord("topic1", 1, null, null, null, null, 1);
        records.add(record1);
        records.add(record2);

        task.database = mock(IDatabase.class);
        when(task.database.getWriter()).thenReturn(mock(IDatabaseWriter.class));

        task.put(records);

        // Verify the method invocations
        // verify(database.getWriter(), times(1)).insert("mytable", records);
        verify(task.database.getWriter(), times(1)).insert("schema.mytable", records);
    }

    @Test
    void testPutEmptyRecords() {
        // Create an empty collection of SinkRecords
        final Collection<SinkRecord> records = Collections.emptyList();

        // Call the put() method
        task.put(records);

        // Verify that no method invocations were made on the database writer
        verifyZeroInteractions(task.database.getWriter());
    }

    @Test
    void testStop() {
        // Call the stop() method
        task.stop();

        // Verify that no method invocations were made on the database writer
        verifyZeroInteractions(task.database.getWriter());
    }

    @Test
    void testFlush() {
        // Create a map of topic partitions and offsets
        final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        final TopicPartition partition = new TopicPartition("topic1", 0);
        map.put(partition, new OffsetAndMetadata(0));

        // Call the flush() method
        task.flush(map);

        // Verify that no method invocations were made on the database writer
        verifyZeroInteractions(task.database.getWriter());
    }

    private void verifyZeroInteractions(final IDatabaseWriter writer) {
        verifyNoMoreInteractions(writer);
    }
}
