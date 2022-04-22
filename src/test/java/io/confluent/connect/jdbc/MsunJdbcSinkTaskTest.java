/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc;

import io.confluent.connect.jdbc.sink.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.easymock.EasyMockSupport;
import org.junit.Test;

import java.util.*;

public class MsunJdbcSinkTaskTest extends EasyMockSupport {

  private static final Schema SCHEMA = SchemaBuilder.struct().name("com.example.Person")
      .field("firstName", Schema.STRING_SCHEMA)
      .field("lastName", Schema.STRING_SCHEMA)
      .field("age", Schema.OPTIONAL_INT32_SCHEMA)
      .field("bool", Schema.OPTIONAL_BOOLEAN_SCHEMA)
      .field("short", Schema.OPTIONAL_INT16_SCHEMA)
      .field("byte", Schema.OPTIONAL_INT8_SCHEMA)
      .field("long", Schema.OPTIONAL_INT64_SCHEMA)
      .field("float", Schema.OPTIONAL_FLOAT32_SCHEMA)
      .field("double", Schema.OPTIONAL_FLOAT64_SCHEMA)
      .field("modified", Timestamp.SCHEMA)
      .build();
  private static final SinkRecord RECORD = new SinkRecord(
      "stub",
      0,
      null,
      null,
      null,
      null,
      0
  );


  @Test
  public void testStartTaskAndPutData() throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put("connection.url", "jdbc:postgresql://localhost:5432/target");
    props.put("topics.regex", "cdc422.*");

    props.put("connection.user", "postgres");
    props.put("connection.password", "postgres");
    props.put("insert.mode", "upsert");
    props.put("pk.mode", "record_key");
    props.put("auto.create", "true");
    props.put("auto.evolve", "true");
    props.put("delete.enabled", "true");

    JdbcSinkTask task = new JdbcSinkTask();
    task.initialize(mock(SinkTaskContext.class));

    task.start(props);

    Schema keySchema = SchemaBuilder.struct().name("com.example.Key")
            .field("id", Schema.INT64_SCHEMA)
            .build();
    Struct key = new Struct(keySchema)
            .put("id", 11L);

    Schema valueSchema = SchemaBuilder.struct().name("com.example.Person")
            .field("id", Schema.INT64_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("bit_field", Schema.BOOLEAN_SCHEMA)
            .field("bits_field", Schema.BYTES_SCHEMA)
            .field("varbit_field", Schema.BYTES_SCHEMA)
            .build();

    Struct value = new Struct(valueSchema)
        .put("id", 11L)
        .put("name", "Smith")
        .put("bit_field", true)
        .put("bits_field", new byte[]{7})
        .put("varbit_field", new byte[]{7});

    final String topic = "target.chis.test_data_type";

    task.put(Collections.singleton(
        new SinkRecord(topic, 1, keySchema, key, valueSchema, value, 42)
    ));


  }


  private List<SinkRecord> createRecordsList(int batchSize) {
    List<SinkRecord> records = new ArrayList<>();
    for (int i = 0; i < batchSize; i++) {
      records.add(RECORD);
    }
    return records;
  }

  private Map<String, String> setupBasicProps(int maxRetries, long retryBackoffMs) {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSinkConfig.CONNECTION_URL, "stub");
    props.put(JdbcSinkConfig.MAX_RETRIES, String.valueOf(maxRetries));
    props.put(JdbcSinkConfig.RETRY_BACKOFF_MS, String.valueOf(retryBackoffMs));
    return props;
  }
}
