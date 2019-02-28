package org.apache.hadoop.yarn.server.unityscheduler;

import io.grpc.stub.StreamObserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels
    .RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .RMAppAttemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .RMAppAttemptEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ClusterNodeTracker;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica
    .FiCaSchedulerNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import si.v1.Si;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

public class UnitySchedulerCallBack {

  private static final Logger LOG = LoggerFactory.getLogger(
      UnitySchedulerCallBack.class);

  private UnitySchedulerShim shim;

  private UnitySchedulerContext context;

  CountDownLatch countDownLatch;

  public UnitySchedulerCallBack(UnitySchedulerShim shim,
      UnitySchedulerContext context, RMContext rmContext) {
    this.shim = shim;
    this.rmContext = rmContext;
    this.context = context;
    countDownLatch = new CountDownLatch(1);
  }

  private RMContext rmContext;

  private StreamObserver<Si.UpdateResponse> responseObserver =
      new StreamObserver<Si.UpdateResponse>() {

        @Override
        public void onNext(Si.UpdateResponse updateResponse) {

          if (updateResponse.getAcceptedJobsCount() > 0) {
            onJobsAccepted(updateResponse.getAcceptedJobsList());
          }

          if (updateResponse.getRejectedJobsCount() > 0) {
            onJobsRejected(updateResponse.getRejectedJobsList());
          }

          if (updateResponse.getAcceptedNodesCount() > 0) {
            onNodesAccepted(updateResponse.getAcceptedNodesList());
          }

          if (updateResponse.getRejectedNodesCount() > 0) {
            onNodesRejected(updateResponse.getRejectedNodesList());
          }

          if (updateResponse.getNewAllocationsCount() > 0) {
            onNewAllocations(updateResponse.getNewAllocationsList());
          }

          if (updateResponse.getRejectedAllocationsCount() > 0) {
            onRejectedAllocations(updateResponse.getRejectedAllocationsList());
          }

          //TODO - Node recommendations

          //TODO - Handle Resync
        }

        @Override
        public void onError(Throwable t) {
          countDownLatch.countDown();
        }

        @Override
        public void onCompleted() {
          countDownLatch.countDown();
        }
        //TODO - Add timeout
      };


  private void onRejectedAllocations(
      List<Si.RejectedAllocationAsk> rejectedAllocations) {
    LOG.error("Allocation ask is rejected");
    //TODO - release allocation asks or fail the attempt ?
  }

  public StreamObserver<Si.UpdateResponse> getUpdateResponseHandler() {
    return responseObserver;
  }

  private void onNewAllocations(List<Si.Allocation> newAllocations) {

    Map<String, List<Si.Allocation>> allocationsByJobId = new HashMap<>();

    try {
      shim.getWriteLock().lock();

      //Group allocation responses by jobId
      for (Si.Allocation allocation : newAllocations) {
        if (allocationsByJobId.get(allocation.getJobId()) == null) {
          allocationsByJobId.put(allocation.getJobId(), new ArrayList<>());
        } else{
          List<Si.Allocation> allocations = allocationsByJobId.get(
              allocation.getJobId());
          allocations.add(allocation);
        }
      }

      for (Iterator<Map.Entry<String, List<Si.Allocation>>> iterator =
           allocationsByJobId.entrySet().iterator(); iterator.hasNext(); ) {

        final Map.Entry<String, List<Si.Allocation>> allocations =
            iterator.next();

        String jobId = allocations.getKey();

        UnitySchedulerAppAttempt appAttempt = shim.getApplicationAttempt
            (context.getApplicationAttemptId(jobId));

        appAttempt.addNewlyAllocatedContainers(allocations.getValue());
      }

    } finally {
      shim.getWriteLock().unlock();
    }
  }

  void onJobsAccepted(final List<Si.AcceptedJob> acceptedJobs) {
    for (Si.AcceptedJob job : acceptedJobs) {
      String appAttemptId = job.getJobId();

      ApplicationAttemptId applicationAttemptId =
          ApplicationAttemptId.fromString(appAttemptId);

      context.addAcceptedJob(appAttemptId, applicationAttemptId);

      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptEvent(applicationAttemptId,
              RMAppAttemptEventType.ATTEMPT_ADDED));
    }
  }

  void onNodesAccepted(final List<Si.AcceptedNode> acceptedNodes) {
    for (Si.AcceptedNode node : acceptedNodes) {
      LOG.info("Node {} accepted ", node.getNodeId().toString());

      String nodeId = node.getNodeId();
      RMNode rmNode = rmContext.getRMNodes().get(NodeId.fromString(nodeId));

      addNode(rmNode);
    }
  }

  private void onNodesRejected(List<Si.RejectedNode> rejectedNodes) {
    //TODO - Add retries to register node again and decommission node if it
    // does not work after retries
    for (Si.RejectedNode node : rejectedNodes) {
      LOG.info("Node {} rejected ", node.getNodeId().toString());

      String nodeId = node.getNodeId();
      RMNode rmNode = rmContext.getRMNodes().get(NodeId.fromString(nodeId));

      removeNode(rmNode);
    }
  }

  private void onJobsRejected(final List<Si.RejectedJob> rejectedJobs) {

    for (Si.RejectedJob job : rejectedJobs) {
      String jobId = job.getJobId();
      ApplicationAttemptId applicationAttemptId =
          ApplicationAttemptId.fromString(jobId);

      rmContext.getDispatcher().getEventHandler().handle(
          new RMAppAttemptEvent(applicationAttemptId,
              RMAppAttemptEventType.FAIL, job.getReason()));

      //TODO - Below event should be sent only after max attempts have been
      // reached . Validate thats happening
//      this.rmContext.getDispatcher().getEventHandler().handle(
//          new RMAppEvent(applicationAttemptId.getApplicationId(),
//              RMAppEventType.APP_REJECTED, job.getReason()));

    }
  }

  private void addNode(RMNode nodeManager) {
    try {
      shim.getWriteLock().lock();

      Configuration conf = rmContext.getYarnConfiguration();
      boolean usePortForNodeName = conf.getBoolean(
          YarnConfiguration.RM_SCHEDULER_INCLUDE_PORT_IN_NODE_NAME,
          YarnConfiguration.DEFAULT_RM_SCHEDULER_USE_PORT_FOR_NODE_NAME);

      FiCaSchedulerNode schedulerNode = new FiCaSchedulerNode(nodeManager,
          usePortForNodeName, nodeManager.getNodeLabels());

      ClusterNodeTracker nodeTracker = shim.getNodeTracker();
      nodeTracker.addNode(schedulerNode);

      final RMNodeLabelsManager nodeLabelManager =
          rmContext.getNodeLabelManager();
      // update this node to node label manager
      if (nodeLabelManager != null) {
        nodeLabelManager.activateNode(nodeManager.getNodeID(),
            schedulerNode.getTotalResource());
      }

      // recover attributes from store if any.
      if (rmContext.getNodeAttributesManager() != null) {
        rmContext.getNodeAttributesManager().refreshNodeAttributesToScheduler(
            schedulerNode.getNodeID());
      }
    } finally {
      shim.getWriteLock().unlock();
    }
  }

  public boolean hasResponseCompletedOrErrored() {
    //TODO - Reconnect if theres an error or stream prematurely completes
      if ( countDownLatch.getCount() == 0) {
        return true;
      }
      return false;
  }

  private void removeNode(RMNode rmNode) {
    //TODO
  }
}
