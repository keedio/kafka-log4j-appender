package org.keedio.kafka.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.spi.LoggingEvent;
import org.keedio.kafka.log4jappender.KafkaLog4jAppender;

import java.util.Hashtable;
import java.util.Map;
import java.util.TreeMap;

/**
 * Custom functionality for Kafka Log4jAppender: instead of retrieving only a message, it will provide the event as JSON
 */
public class CustomFunctionality extends KafkaLog4jAppender {

  /**
   * Creates a map with the information of the event, topic and hostname
   * @param event the event
   * @param topic the topic
   * @param hostName the hostname
   * @return the map
   */
  private static Map<String, Object> eventToMap(final LoggingEvent event, final String topic, final String hostName) {

    final Map<String, Object> eventInfo = new TreeMap<String, Object>() {{
      put("fqnOfCategoryClass", event.getFQNOfLoggerClass());
      put("categoryName", event.getLogger().getName());
      put("level", event.getLevel().toString());
      put("ndc", event.getNDC());
      put("message", event.getMessage());
      put("renderedMessage", event.getRenderedMessage());
      put("threadName", event.getThreadName());
      put("timeStamp", event.getTimeStamp());
      put("locationInfo", event.getLocationInformation());
      put("throwableInfo", event.getThrowableInformation());
    }};

    final Map<String, Object> em = new TreeMap<String, Object>() {{
      put("event", eventInfo);
      put("topic", topic);
      put("hostName", hostName);
    }};

    return em;
  }

  /**
   * custom subAppend method originally declared in KafkaLog4hjAppender
   * @param event the logging event
   * @param topic the topic
   * @param hostName the hostName
   * @return the json that contains the information to be retrieved
   */
  public String subAppend(LoggingEvent event, String topic, String hostName) {
    Map eventAsMap = eventToMap(event, topic, hostName);
    String json = null;
    try {
      json = new ObjectMapper().writeValueAsString(eventAsMap);
    } catch (JsonProcessingException e) {
      e.printStackTrace();
    }
    return json;
  }
}
