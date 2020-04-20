package com.ibm.eventstreams.connect.jdbcsink.sink.datasource;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.List;
import java.util.stream.Collectors;

public class DataUtils {

    public static String arrayFieldNamesSplitAsString(List<Field> array) {
        return array.stream().map(Field::name).collect(Collectors.joining(","));
    }

    public static String arrayFieldValuesSplitAsString(Struct valueStruct, Schema valueSchema) {
        return valueSchema.fields().stream().map(field -> wrapInQuotesIfNeeded(field.schema().type(), valueStruct.get(field).toString())).collect(Collectors.joining(","));
    }

    private static String wrapInQuotesIfNeeded(final Schema.Type schemaType, final String str) {
        return schemaType == Schema.Type.STRING ? String.format("'%s'", str) : str;
    }
}
