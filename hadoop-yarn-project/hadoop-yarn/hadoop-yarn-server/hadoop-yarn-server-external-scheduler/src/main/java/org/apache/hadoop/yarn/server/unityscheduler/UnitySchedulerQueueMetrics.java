package org.apache.hadoop.yarn.server.unityscheduler;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;


public class UnitySchedulerQueueMetrics extends QueueMetrics {

  protected UnitySchedulerQueueMetrics(MetricsSystem ms, String queueName,
      Queue parent, boolean enableUserMetrics, Configuration conf) {
    super(ms, queueName, parent, enableUserMetrics, conf);
  }
}
