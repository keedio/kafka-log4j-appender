package org.keedio.kafka.log4jappender;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkConf$;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by luca on 12/6/17.
 */
public class SparkContextAwareKafkaLog4jAppenderTest {
  Logger logger = Logger.getLogger(SparkContextAwareKafkaLog4jAppenderTest.class);

  private static JavaSparkContext sparkContext = null;

  @BeforeClass
  public static void setupSparkContext() {
    SparkConf conf = new SparkConf();
    conf.setMaster("local[2]");
    conf.setAppName("SparkContextAwareKafkaLog4jAppenderTest");

    sparkContext = new JavaSparkContext(SparkContext$.MODULE$.getOrCreate(conf));
  }

  @AfterClass
  public static void shutdownSparkContext() {
    if (sparkContext != null) {
      sparkContext.stop();
    }
  }

  //@Test
  public void testLog4jAppends() throws IOException {
    PropertyConfigurator.configure(getLog4jConfig());

    for (int i = 1; i <= 5; ++i) {
      logger.error(getMessage(i));
    }

    MockSparkContextAwareKafkaLog4jAppender appender = (MockSparkContextAwareKafkaLog4jAppender) (Logger.getRootLogger()
      .getAppender("KAFKA"));

    List<ProducerRecord<byte[], byte[]>> records = appender.getHistory();

    ObjectMapper mapper = new ObjectMapper();
    
    for (ProducerRecord<byte[], byte[]> record : records) {
      Map<String, Object> map =  mapper.readValue(new String(record.value(), "UTF-8"), new TypeReference<Map<String, Object>>(){});
      Map<String, Object> event = (Map<String, Object>) map.get("event");
      Assert.assertNotNull(event);
      
      Assert.assertTrue(event.get("sparkAppId").toString().startsWith("local-"));
      Assert.assertEquals("SparkContextAwareKafkaLog4jAppenderTest", event.get("sparkAppName").toString());
    }
    
    Assert.assertEquals(
      5, appender.getHistory().size());
  }

  private byte[] getMessage(int i) throws UnsupportedEncodingException {
    return ("test_" + i).getBytes("UTF-8");
  }

  private Properties getLog4jConfig() {
    Properties props = new Properties();
    props.put("log4j.rootLogger", "INFO, KAFKA");
    props.put("log4j.appender.KAFKA", "org.keedio.kafka.log4jappender.MockSparkContextAwareKafkaLog4jAppender");
    props.put("log4j.appender.KAFKA.BrokerList", "127.0.0.1:9093");
    props.put("log4j.appender.KAFKA.Topic", "test-topic");
    props.put("log4j.appender.KAFKA.RequiredNumAcks", "1");
    props.put("log4j.appender.KAFKA.SyncSend", "false");
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA");
    return props;
  }
}
