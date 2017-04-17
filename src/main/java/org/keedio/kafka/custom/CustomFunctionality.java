package org.keedio.kafka.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.spi.LoggingEvent;
import org.keedio.kafka.log4jappender.KafkaLog4jAppender;

import java.util.Map;
import java.util.TreeMap;

/**
 * Custom functionality for Kafka Log4jAppender: instead of retrieving only a message, it will provide the event as JSON
 */
public class CustomFunctionality extends KafkaLog4jAppender {

  private static Map<String, Object> eventToMap(final LoggingEvent event, final String topic, final String hostname) {
    final Map<String, Object> em = new TreeMap<String, Object>() {{
      put("event.fqnOfCategoryClass", event.getFQNOfLoggerClass());
      put("event.categoryName", event.getLogger().getName());
      put("event.level", event.getLevel().toString());
      put("event.ndc", event.getNDC());
      put("event.message", event.getMessage());
      put("event.renderedMessage", event.getRenderedMessage());
      put("event.threadName", event.getThreadName());
      put("event.timeStamp", event.getTimeStamp());
      put("event.locationInfo", event.getLocationInformation());
      put("event.throwableInfo", event.getThrowableInformation());
      put("topic", topic);
      put("hostname", hostname);
    }};
    return em;
  }

  public String subAppend(LoggingEvent event, String topic, String hostname) {
    Map eventAsMap = eventToMap(event, topic, hostname);
    String json = null;
    try {
      json = new ObjectMapper().writeValueAsString(eventAsMap);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return json;
  }
}
