package org.keedio.kafka.log4jappender;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Properties;

/**
 * Created by luca on 12/6/17.
 */
public class MockSparkContextAwareKafkaLog4jAppender extends SparkContextAwareKafkaLog4jAppender {
  private MockProducer mockProducer =
    new MockProducer(false, new ByteArraySerializer(), new ByteArraySerializer());
  
  

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

  public MockProducer getMockProducer() {
    return mockProducer;
  }
}
