package org.apache.hadoop.yarn.server.unityscheduler;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NMToken;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer
    .RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .AbstractUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import si.v1.Si;

import java.util.List;

public class UnitySchedulerAppAttempt extends SchedulerApplicationAttempt {

  private ResourceScheduler scheduler;

  private static final Logger LOG = LoggerFactory.getLogger(
      UnitySchedulerAppAttempt.class);

  public UnitySchedulerAppAttempt(ApplicationAttemptId applicationAttemptId,
      String user, Queue queue, AbstractUsersManager abstractUsersManager,
      RMContext rmContext) {
    super(applicationAttemptId, user, queue, abstractUsersManager, rmContext);

    scheduler = rmContext.getScheduler();
  }

  /**
   * This method produces an Allocation that includes the current view
   * of the resources that will be allocated to and preempted from this
   * application.
   *
   * @return an allocation
   */
  public void addNewlyAllocatedContainers(List<Si.Allocation> allocations) {
    try {
      writeLock.lock();
      //TODO - Pre-emption whenever it is supported

      //TODO - DO some validation - Partition should be same as requested

      //      List<Container> newlyAllocatedContainers = new ArrayList<>();
      for (Si.Allocation allocation : allocations) {
        RMContainer rmContainer = getContainerFromAllocationResponse(
            allocation);
        //        RMNode node = rmContext.getRMNodes().get(rmContainer
        // .getNodeId());
        //        addToNewlyAllocatedContainers(nodeId, rmContainer);
        //TODO - Use addToNewlyAllocatedContainers() instead ?
        newlyAllocatedContainers.add(rmContainer);
      }

    } finally {
      writeLock.unlock();
    }
  }

  public Allocation getNewAllocations() {
    Resource headroom = getHeadroom();
    setApplicationHeadroomForMetrics(headroom);
    return new Allocation(pullNewlyAllocatedContainers(), headroom, null, null,
        null, pullUpdatedNMTokens());
  }

  private RMContainer getContainerFromAllocationResponse(
      Si.Allocation allocation) {

    Priority priority = Priority.newInstance(
        allocation.getPriority().getPriorityValue());

    AllocationRequestKey allocationRequestKey = new AllocationRequestKey(
        allocation.getJobId(), allocation.getAllocationKey(), priority);

    NodeId nodeId = NodeId.fromString(allocation.getNodeId());
    RMNode rmNode = rmContext.getRMNodes().get(nodeId);

    long allocationRequestId = Long.valueOf(
        allocationRequestKey.getAllocationRequestId());

    Container container = BuilderUtils.newContainer(ContainerId
            .newContainerId(getApplicationAttemptId(), getNewContainerId()),
        nodeId,
        rmNode.getHttpAddress(), Resources.none(),
        //TODO - Remove hardcoding to GUARANTEED
        priority, null, ExecutionType.GUARANTEED, allocationRequestId);

    SchedulerRequestKey schedulerRequestKey = new SchedulerRequestKey(priority,
        allocationRequestId, null);

    RMContainer rmContainer = new RMContainerImpl(container,
        schedulerRequestKey, getApplicationAttemptId(), rmNode.getNodeID(),
        getUser(), rmContext, System.currentTimeMillis(),
        allocation.getPartition(), true);

    return rmContainer;
  }

  //TODO - Headroom - ?
  @Override
  public Resource getHeadroom() {
    //Super.headRoom returns 0. Using Cluster Resource for now
    return scheduler.getClusterResource();
  }

}
