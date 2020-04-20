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
