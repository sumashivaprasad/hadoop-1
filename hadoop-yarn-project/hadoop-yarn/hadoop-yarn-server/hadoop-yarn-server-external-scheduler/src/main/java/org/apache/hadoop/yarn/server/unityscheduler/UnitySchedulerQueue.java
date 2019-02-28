package org.apache.hadoop.yarn.server.unityscheduler;

import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .AbstractUsersManager;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerApplicationAttempt;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;

public class UnitySchedulerQueue implements Queue {

  private String queueName;

  private QueueMetrics queueMetrics;

  private Priority applicationPriority;

  private AbstractUsersManager usersManager;

  public UnitySchedulerQueue(String queueName, QueueMetrics queueMetrics,
      AbstractUsersManager usersManager, Priority applicationPriority) {
    this.queueName = queueName;
    this.queueMetrics = queueMetrics;
    this.usersManager = usersManager;
    this.applicationPriority = applicationPriority;
  }

  @Override public String getQueueName() {
    return queueName;
  }

  @Override public QueueMetrics getMetrics() {
    return queueMetrics;
  }

  @Override public QueueInfo getQueueInfo(boolean includeChildQueues,
      boolean recursive) {
    return null;
  }

  @Override public List<QueueUserACLInfo> getQueueUserAclInfo(
      UserGroupInformation user) {
    return null;
  }

  @Override public boolean hasAccess(QueueACL acl, UserGroupInformation user) {
    return true;
  }

  @Override public AbstractUsersManager getAbstractUsersManager() {
    return usersManager;
  }

  @Override public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    //TODO
  }

  @Override public Set<String> getAccessibleNodeLabels() {
    Set<String> nodeLabels = new HashSet<>();
    nodeLabels.add(NO_LABEL);
    return nodeLabels;
  }

  @Override public String getDefaultNodeLabelExpression() {
    return "";
  }

  @Override public void incPendingResource(String nodeLabel,
      Resource resourceToInc) {
    //TODO
  }

  @Override public void decPendingResource(String nodeLabel,
      Resource resourceToDec) {
    //TODO
  }

  @Override public Priority getDefaultApplicationPriority() {
    return applicationPriority;
  }

  @Override public void incReservedResource(String partition,
      Resource reservedRes) {
    //TODO
  }

  @Override public void decReservedResource(String partition,
      Resource reservedRes) {
    //TODO
  }
}
