package org.keedio.kafka.custom;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.spi.LoggingEvent;
import org.keedio.kafka.log4jappender.KafkaLog4jAppender;

import java.util.Map;
import java.util.TreeMap;

public class CustomFunctionality extends KafkaLog4jAppender {

    private static Map<String,Object> eventToMap(final LoggingEvent event) {
        final Map<String, Object> em = new TreeMap<String, Object>() {{
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
        return em;
    }

    public String subAppend(LoggingEvent event) {
        Map eventAsMap = eventToMap(event);
        String json = null;
        try {
            json = new ObjectMapper().writeValueAsString(eventAsMap);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return json;
    }
}
