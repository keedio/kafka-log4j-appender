package org.keedio.kafka.log4jappender;

import org.apache.log4j.Logger;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.spark.SparkContext;
import org.apache.spark.SparkContext$;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Option;

import java.util.Map;

/**
 * Created by luca on 12/6/17.
 */
public class SparkContextAwareKafkaLog4jAppender extends KafkaLog4jAppender {

  public static final String SPARK_CUSTOM_APPLICATION_ID = "spark.custom.application.id";
  public static final String SPARK_CUSTOM_APPLICATION_NAME = "spark.custom.application.name";
  public static final String SPARK_APP_ID = "sparkAppId";
  public static final String SPARK_APP_NAME = "sparkAppName";
  public static final String SPARK_STAGE_ID = "sparkStageId";
  public static final String SPARK_PARTITION_ID = "sparkPartitionId";

  @Override
  protected Map<String, Object> generateEventMetadata(LoggingEvent event) {
    Map<String, Object> metadata = super.generateEventMetadata(event);

    TaskContext context = TaskContext.get();
    
    if (context != null) {
      String appId = context.getLocalProperty(SPARK_CUSTOM_APPLICATION_ID);
      String appName = context.getLocalProperty(SPARK_CUSTOM_APPLICATION_NAME);

      if (appId != null)
        metadata.put(SPARK_APP_ID, appId);
      if (appName != null)
        metadata.put(SPARK_APP_NAME, appName);

      metadata.put(SPARK_STAGE_ID, context.stageId());
      metadata.put(SPARK_PARTITION_ID, context.partitionId());
    }
    return metadata;
  }
}
