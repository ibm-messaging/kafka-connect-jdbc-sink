package com.ibm.eventstreams.connect.jdbcsink;

import com.ibm.eventstreams.connect.jdbcsink.database.IDatabase;
import com.ibm.eventstreams.connect.jdbcsink.database.writer.IDatabaseWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import java.sql.SQLException;
import java.util.*;

import static org.mockito.Mockito.*;

class JDBCSinkTaskTest {

    private JDBCSinkTask task;
    private SinkTaskContext context;
    public Map<String, String> generalConnectorProps = new HashMap<>();

    @BeforeEach
    void setUp() {
        task = new JDBCSinkTask();
        context = mock(SinkTaskContext.class);
        task.initialize(context);

        // Configure the configuration properties
        generalConnectorProps.put("table.name.format", "schema.mytable");
        generalConnectorProps.put("connection.url", "jdbc:db2://localhost:50000/sample");
        generalConnectorProps.put("connection.user", "db2inst1");
        generalConnectorProps.put("connection.password", "password");

        task.database = mock(IDatabase.class);
        Mockito.when(task.database.getWriter()).thenReturn(mock(IDatabaseWriter.class));
    }

    @Test
    void testPut() throws SQLException {
        // Call the put() method
        task.start(generalConnectorProps);

        // Create a collection of SinkRecords
        Collection<SinkRecord> records = new ArrayList<>();
        SinkRecord record1 = new SinkRecord("topic1", 0, null, null, null, null, 0);
        SinkRecord record2 = new SinkRecord("topic1", 1, null, null, null, null, 1);
        records.add(record1);
        records.add(record2);

        task.database = mock(IDatabase.class);
        Mockito.when(task.database.getWriter()).thenReturn(mock(IDatabaseWriter.class));

        task.put(records);

        // Verify the method invocations
        // verify(database.getWriter(), times(1)).insert("mytable", records);
        verify(task.database.getWriter(), Mockito.times(1)).insert("schema.mytable", records);
    }

    @Test
    void testPutEmptyRecords() {
        // Create an empty collection of SinkRecords
        Collection<SinkRecord> records = Collections.emptyList();

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
        Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
        TopicPartition partition = new TopicPartition("topic1", 0);
        map.put(partition, new OffsetAndMetadata(0));

        // Call the flush() method
        task.flush(map);

        // Verify that no method invocations were made on the database writer
        verifyZeroInteractions(task.database.getWriter());
    }

    private void verifyZeroInteractions(IDatabaseWriter writer) {
        verifyNoMoreInteractions(writer);
    }
}
