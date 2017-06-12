package org.keedio.kafka.log4jappender;

import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaSparkContext$;
import scala.Option;

import java.util.Map;

/**
 * Created by luca on 12/6/17.
 */
public class SparkContextAwareKafkaLog4jAppender extends KafkaLog4jAppender{
  
  protected JavaSparkContext sparkContext = null;

  @Override
  public void activateOptions() {
    super.activateOptions();
    
    Option<SparkContext> optSparkContext = SparkContext$.MODULE$.getActive();
    
    if (optSparkContext.isDefined()) {
      sparkContext = JavaSparkContext.fromSparkContext(optSparkContext.get());
    } else {
      LogLog.debug("No active Spark context found!");
    }
  }

  @Override
  protected Map<String, Object> generateEventMetadata(LoggingEvent event) {
    Map<String, Object> metadata = super.generateEventMetadata(event);
    
    if (sparkContext != null){
      String appId = sparkContext.getConf().getAppId();
      String appName = sparkContext.appName();
      
      metadata.put("sparkAppId", appId);
      metadata.put("sparkAppName", appName);
    }
    
    return metadata;
  }
}
