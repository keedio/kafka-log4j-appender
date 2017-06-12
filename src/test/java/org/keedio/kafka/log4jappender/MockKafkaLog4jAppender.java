/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.keedio.kafka.log4jappender;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Properties;

public class MockKafkaLog4jAppender extends KafkaLog4jAppender {
  private MockProducer mockProducer =
    new MockProducer(false, new StringSerializer(), new StringSerializer());

  @Override
  protected Producer<byte[], byte[]> getKafkaProducer(Properties props) {
    return mockProducer;
  }

  @Override
  protected void append(LoggingEvent event) {
    if (super.getProducer() == null) {
      activateOptions();
    }
    super.append(event);
  }

  protected java.util.List<ProducerRecord<byte[], byte[]>> getHistory() {
    return mockProducer.history();
  }
}
