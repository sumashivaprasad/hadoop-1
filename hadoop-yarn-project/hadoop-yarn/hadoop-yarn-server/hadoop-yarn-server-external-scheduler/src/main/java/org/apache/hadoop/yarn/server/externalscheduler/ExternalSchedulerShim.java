package org.apache.hadoop.yarn.server.externalscheduler;

import io.grpc.stub.StreamObserver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.protocolrecords.ResourceTypes;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.ResourceSizing;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.api.resource.PlacementConstraint;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerDynamicEditException;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.activities
    .ActivitiesLogger;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CSAssignment;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .CapacitySchedulerMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .ManagedParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.ParentQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.QueueManagementChange;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .ContainerPreemptEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .NodeAttributesUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .NodeLabelsUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .SchedulerEvent;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import si.v1.SchedulerGrpc;
import si.v1.Si;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class ExternalSchedulerShim extends AbstractYarnScheduler {

  /**
   * Construct the service.
   *
   */

  private String rmId;

  private ExternalSchedulerGrpcClient grpcClient;

  private SchedulerGrpc.SchedulerStub schedulerStub;

  public ExternalSchedulerShim() {
    super(ExternalSchedulerShim.class.getName());
  }


  @Override public void recover(RMStateStore.RMState state) throws Exception {
  }


  @Override public void handle(SchedulerEvent event) {
    switch(event.getType()) {
    case NODE_ADDED:
    {
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
      Si.UpdateRequest.Builder updateRequestBuilder = createUpdateRequestBuilder();

      Si.NewNodeInfo.Builder newNodeInfo = Si.NewNodeInfo.newBuilder();
      RMNode rmNode = nodeAddedEvent.getAddedRMNode();
      newNodeInfo.setNodeId(rmNode.getNodeID().toString());

      Resource nodeCapability = rmNode.getTotalCapability();
      final Si.Resource resourceCapability = getResourceCapability(
          nodeCapability);
      newNodeInfo.setSchedulableResource(resourceCapability);

      updateRequestBuilder.addNewSchedulableNodes(newNodeInfo);

      //TODO - Deal with container reports
      //      recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
      //          nodeAddedEvent.getAddedRMNode());
      //TODO: Will have to add existing allocations if containers are up and
      // recovered on node restart
      //newNodeInfo.addExistingAllocations();


    }
    break;
    case NODE_REMOVED:
    {
      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;

      Si.UpdateRequest.Builder updateRequestBuilder = createUpdateRequestBuilder();

      Si.UpdateNodeInfo.Builder updatedNodeInfo = Si.UpdateNodeInfo
          .newBuilder();
      RMNode rmNode = nodeRemovedEvent.getRemovedRMNode();
      updatedNodeInfo.setNodeId(rmNode.getNodeID().toString());
      updatedNodeInfo.setAction(Si.UpdateNodeInfo.ActionFromRM.DECOMISSION);

      //TODO: Would need to handle graceful decommissioning cases - DRAIN TO
      // DECOMMISSION
      updateRequestBuilder.addUpdatedNodes(updatedNodeInfo);

//      removeNode(nodeRemovedEvent.getRemovedRMNode());
    }
    break;
    case NODE_RESOURCE_UPDATE:
    {
      NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
          (NodeResourceUpdateSchedulerEvent)event;
//      updateNodeAndQueueResource(nodeResourceUpdatedEvent.getRMNode(),
//          nodeResourceUpdatedEvent.getResourceOption());
    }
    break;
    case NODE_LABELS_UPDATE:
    {
      NodeLabelsUpdateSchedulerEvent labelUpdateEvent =
          (NodeLabelsUpdateSchedulerEvent) event;

//      updateNodeLabelsAndQueueResource(labelUpdateEvent);
    }
    break;
    case NODE_ATTRIBUTES_UPDATE:
    {
      NodeAttributesUpdateSchedulerEvent attributeUpdateEvent =
          (NodeAttributesUpdateSchedulerEvent) event;

//      updateNodeAttributes(attributeUpdateEvent);
    }
    break;
    case NODE_UPDATE:
      //TODO - Dont think we need to do anything here - just a heart beat?
//    {
//      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
//      nodeUpdate(nodeUpdatedEvent.getRMNode());
//    }
    break;
    case APP_ADDED:
    {
      //TODO : Check if we need to do anythiung here since appAttemptId is
      // what we are interested in
      //        updateRequestBuilder.addRemoveJobs()
//      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;

//      String queueName = resolveReservationQueueName(appAddedEvent.getQueue(),
//          appAddedEvent.getApplicationId(), appAddedEvent.getReservationID(),
//          appAddedEvent.getIsAppRecovering());
//      if (queueName != null) {3
//        if (!appAddedEvent.getIsAppRecovering()) {
//          addApplication(appAddedEvent.getApplicationId(), queueName,
//              appAddedEvent.getUser(), appAddedEvent.getApplicatonPriority(),
//              appAddedEvent.getPlacementContext());
//        } else {
//          addApplicationOnRecovery(appAddedEvent.getApplicationId(), queueName,
//              appAddedEvent.getUser(), appAddedEvent.getApplicatonPriority(),
//              appAddedEvent.getPlacementContext());
//        }
//      }
    }
    break;
    case APP_REMOVED:
    {
      //TODO - Remove from cache
      //TODO - Send removeJobRequest
//      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
//      doneApplication(appRemovedEvent.getApplicationID(),
//          appRemovedEvent.getFinalState());
    }
    break;
    case APP_ATTEMPT_ADDED:
    {

      //TODO - Add to cache
//      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
//          (AppAttemptAddedSchedulerEvent) event;
//
//      ApplicationAttemptId appAttemptId = appAttemptAddedEvent
//          .getApplicationAttemptId();
//      Si.UpdateRequest.Builder updateRequestBuilder =
//          createUpdateRequestBuilder();
//      Si.AddJobRequest.Builder addJobRequestBuilder = Si.AddJobRequest
//        .newBuilder();
//      addJobRequestBuilder.setJobId(appAttemptId.toString());
//      addJobRequestBuilder.set
//      updateRequestBuilder.addNewJobs(addJobRequestBuilder.build());
      //TODO - Add to cache

    }
    break;
    case APP_ATTEMPT_REMOVED:
    {
      //TODO - Remove from cache
      //TODO - Send removeJobRequest
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
//      doneApplicationAttempt(appAttemptRemovedEvent.getApplicationAttemptID(),
//          appAttemptRemovedEvent.getFinalAttemptState(),
//          appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
    }
    break;
    case CONTAINER_EXPIRED:
    {
//      ContainerExpiredSchedulerEvent containerExpiredEvent =
//          (ContainerExpiredSchedulerEvent) event;
//      ContainerId containerId = containerExpiredEvent.getContainerId();
//      if (containerExpiredEvent.isIncrease()) {
//        rollbackContainerUpdate(containerId);
//      } else {
//        completedContainer(getRMContainer(containerId),
//            SchedulerUtils.createAbnormalContainerStatus(
//                containerId,
//                SchedulerUtils.EXPIRED_CONTAINER),
//            RMContainerEventType.EXPIRE);
//      }
    }
    break;
    case RELEASE_CONTAINER:
    {
//      RMContainer container = ((ReleaseContainerEvent) event).getContainer();
//      completedContainer(container,
//          SchedulerUtils.createAbnormalContainerStatus(
//              container.getContainerId(),
//              SchedulerUtils.RELEASED_CONTAINER),
//          RMContainerEventType.RELEASED);
    }
    break;
    case KILL_RESERVED_CONTAINER:
    {
//      ContainerPreemptEvent killReservedContainerEvent =
//          (ContainerPreemptEvent) event;
//      RMContainer container = killReservedContainerEvent.getContainer();
//      killReservedContainer(container);
    }
    break;
    case MARK_CONTAINER_FOR_PREEMPTION:
    {
//      ContainerPreemptEvent preemptContainerEvent =
//          (ContainerPreemptEvent)event;
//      ApplicationAttemptId aid = preemptContainerEvent.getAppId();
//      RMContainer containerToBePreempted = preemptContainerEvent.getContainer();
//      markContainerForPreemption(aid, containerToBePreempted);
    }
    break;
    case MARK_CONTAINER_FOR_KILLABLE:
    {
//      ContainerPreemptEvent containerKillableEvent = (ContainerPreemptEvent)event;
//      RMContainer killableContainer = containerKillableEvent.getContainer();
//      markContainerForKillable(killableContainer);
    }
    break;
    case MARK_CONTAINER_FOR_NONKILLABLE:
    {
//      if (isLazyPreemptionEnabled) {
//        ContainerPreemptEvent cancelKillContainerEvent =
//            (ContainerPreemptEvent) event;
//        markContainerForNonKillable(cancelKillContainerEvent.getContainer());
//      }
    }
    break;
    case MANAGE_QUEUE:
    {
      //TODO - API based queue management
      //TODO - Add to cache
//      QueueManagementChangeEvent queueManagementChangeEvent =
//          (QueueManagementChangeEvent) event;
//      ParentQueue parentQueue = queueManagementChangeEvent.getParentQueue();
//      try {
//        final List<QueueManagementChange> queueManagementChanges =
//            queueManagementChangeEvent.getQueueManagementChanges();
//        ((ManagedParentQueue) parentQueue)
//            .validateAndApplyQueueManagementChanges(queueManagementChanges);
//      } catch (SchedulerDynamicEditException sde) {
//        LOG.error("Queue Management Change event cannot be applied for "
//            + "parent queue : " + parentQueue.getQueueName(), sde);
//      } catch (IOException ioe) {
//        LOG.error("Queue Management Change event cannot be applied for "
//            + "parent queue : " + parentQueue.getQueueName(), ioe);
//      }
    }
    break;
    default:
//      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  @Override public void setRMContext(RMContext rmContext) {
    rmId = WebAppUtils.getWebAppBindURL(rmContext.getYarnConfiguration(),
        YarnConfiguration.RM_BIND_HOST,
        WebAppUtils.getRMWebAppURLWithoutScheme(rmContext
            .getYarnConfiguration()));

    Configuration conf = rmContext.getYarnConfiguration();
    String unitySchedulerHost = conf.get(YarnConfiguration
        .UNITY_SCHEDULER_HOST);
    int unitySchedulerPort = conf.getInt(YarnConfiguration
        .UNITY_SCHEDULER_PORT, YarnConfiguration.DEFAULT_UNITY_SCHEDULER_PORT);

    grpcClient = new ExternalSchedulerGrpcClient(unitySchedulerHost,
        unitySchedulerPort);

    schedulerStub = grpcClient
        .createSchedulerStub();
  }

  @Override public Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> resourceRequests, List<SchedulingRequest> schedulingRequests,
      List<ContainerId> release, List<String> blacklistAdditions,
      List<String> blacklistRemovals, ContainerUpdates updateRequests) {

    Si.UpdateRequest.Builder updateRequestBuilder =
        createUpdateRequestBuilder();

    //TODO - Check if resource request present, then dont use scheduling
    // request and vice versa
    List<Si.AllocationAsk> allocationRRAsks = transformResourceRequests
        (appAttemptId, resourceRequests);

    Si.AddJobRequest.Builder addJobRequestBuilder = Si.AddJobRequest
            .newBuilder();
    addJobRequestBuilder.setJobId(appAttemptId.toString());

    //TODO - Check whether this is just a new Job Request independent
    // Allocation REequest
//    addJobRequestBuilder.setPartitionName(allocationRRAsks.get(0)
//        .getPartitionName());
//    addJobRequestBuilder.setPartitionName(allocationRRAsks.get(0)
//        .getPartitionName());

    updateRequestBuilder.addNewJobs(addJobRequestBuilder);

    updateRequestBuilder.addAllAsks(allocationRRAsks);

    //TODO - Uncomment below after scheduling request translation is done.
//    List<Si.AllocationAsk> allocationSRAsks = transformSchedulingRequests
//        (appAttemptId,
//        schedulingRequests);
//    updateRequestBuilder.addAllAsks(allocationSRAsks);

    Si.UpdateRequest updateRequest = updateRequestBuilder.build();

    final StreamObserver<Si.UpdateRequest> scheduleUpdateRequestObserver =
        schedulerStub.update(responseObserver);
    scheduleUpdateRequestObserver.onNext(updateRequest);


    //TODO - release requests

    //TODO - Blacklist nodes - send node updates

    //TODO - Container updates

  }

  private StreamObserver<Si.UpdateResponse> responseObserver = new
      StreamObserver<Si.UpdateResponse>() {

        final CountDownLatch finishLatch = new CountDownLatch(1);
        @Override
        public void onNext(Si.UpdateResponse updateResponse) {
          final List<Si.AcceptedJob> acceptedJobsList =
              updateResponse.getAcceptedJobsList();


          if ( acceptedJobsList) {

          }

        }

        @Override
        public void onError(Throwable t) {
          finishLatch.countDown();
        }

        @Override
        public void onCompleted() {
          finishLatch.countDown();
        }

        //TODO - Add timeout
      };



  private Si.UpdateRequest.Builder createUpdateRequestBuilder() {
    Si.UpdateRequest.Builder updateRequestBuilder = Si.UpdateRequest
        .newBuilder();

    updateRequestBuilder.setRmId(rmId);
    return updateRequestBuilder;
  }

  private List<Si.AllocationAsk> transformResourceRequests(ApplicationAttemptId
      appAttemptId,
      List<ResourceRequest> ask) {

    List<Si.AllocationAsk> allocationAsks = new ArrayList<>();

    for ( ResourceRequest req : ask ) {

      Si.AllocationAsk.Builder askBuilder = Si.AllocationAsk.newBuilder();
      askBuilder.setQueueName(getQueue(appAttemptId));

      //Allocation key
      askBuilder.setAllocationKey(String.valueOf(req.getAllocationRequestId()));

      askBuilder.setJobId(appAttemptId.toString());

      askBuilder.setMaxAllocations(req.getNumContainers());

      //Node Label
      askBuilder.setPartitionName(req.getNodeLabelExpression());

      //Priority
      Si.Priority priority = getResourcePriority(req.getPriority());
      askBuilder.setPriority(priority);

      //Set resource asks
      //TODO - Set from vm,mem also - ?
      //TODO - How do we track Job instances  - unique id in job id?
      Resource resource = req.getCapability();
      askBuilder.setResourceAsk(getResourceCapability(resource));

      //TODO - Dedup locality constraints by allocationId
      allocationAsks.add(askBuilder.build());

    }
    return allocationAsks;
  }

  private List<Si.AllocationAsk> transformSchedulingRequests
      (ApplicationAttemptId appAttemptId, List<SchedulingRequest> schedulingRequests) {

    List<Si.AllocationAsk> allocationAsks = new ArrayList<>();

    for (SchedulingRequest schedulingRequest : schedulingRequests) {
      schedulingRequest.getAllocationRequestId();

      Si.AllocationAsk.Builder askBuilder = Si.AllocationAsk.newBuilder();

      ResourceSizing resourceSizing = schedulingRequest.getResourceSizing();
      Resource resource = resourceSizing.getResources();

      Si.Resource resourceAsk = getResourceCapability(resource);
      askBuilder.setResourceAsk(resourceAsk);

      askBuilder.setMaxAllocations(resourceSizing.getNumAllocations());
      askBuilder.setJobId(appAttemptId.toString());

      //Allocation Tags
      Set<String> allocationTags = schedulingRequest.getAllocationTags();

      for (String tag : allocationTags) {
        //TODO - What should allocation tag key/value be - set vs map?
        askBuilder.putTags(tag, tag);
      }

      askBuilder.setQueueName(getQueue(appAttemptId));

      //Priority
      Si.Priority priority = getResourcePriority(schedulingRequest.getPriority());
      askBuilder.setPriority(priority);

      //Placement constraints
      askBuilder.setPlacementConstraint(transformPlacementConstraint
          (schedulingRequest.getPlacementConstraint()));

      allocationAsks.add(askBuilder.build());

      //      askBuilder.setPlacementConstraint(req.)
    }
    return allocationAsks;
  }

  private Si.PlacementConstraint transformPlacementConstraint
      (PlacementConstraint placementConstraintReq) {
    Si.PlacementConstraint.Builder result = Si
        .PlacementConstraint.newBuilder();

    Si.SimplePlacementConstraint.Builder simplePlacementConstraint =
        Si.SimplePlacementConstraint.newBuilder();
//    final PlacementConstraint.AbstractConstraint constraintExpr =
//        placementConstraintReq.getConstraintExpr();


    //Transform single constraint

//    PlacementConstraint.SingleConstraint singleConstraint =
//        (PlacementConstraint.SingleConstraint) constraintExpr;
//    String scope = singleConstraint.getScope();
//
//    simplePlacementConstraint.setAllocationAffinityAttribute
//        (AllocationAffinityConstraints.)
//     singleConstraint.getMinCardinality();
//    singleConstraint.getMaxCardinality();

    //TODO: Transform composite constraint
    //TODO: Transform cardinality constraint
    //TODO: Transform timeplaced constraint

    result.setSimpleConstraint(simplePlacementConstraint);
    return result.build();
  }



  private String getQueue(ApplicationAttemptId appAttemptId) {
    //TODO - Check if any other way to get queue
    RMApp app = rmContext.getRMApps().get(appAttemptId.getApplicationId());
    return app.getQueue();
  }

  private Si.Priority getResourcePriority(Priority priority) {
    Si.Priority.Builder reqPriority = Si.Priority.newBuilder();
    reqPriority.setPriorityValue(priority.getPriority());
    // reqPriority.setPriorityClassName(Si.Priority.)
    return reqPriority.build();
  }

  private Si.Resource getResourceCapability(Resource resource) {

    Si.Resource.Builder resourceBuilder = Si.Resource.newBuilder();
    for (ResourceInformation resourceInformation : resource.getResources()) {

      Si.Quantity.Builder quantityBuilder = Si.Quantity.newBuilder();
      quantityBuilder.setValue(resourceInformation.getValue());

      // TODO:        resourceInformation.getMinimumAllocation(); - ?
      // TODO:        resourceInformation.getMaximumAllocation(); - ?
      // TODO - Check if we need to transform ResourceInformation names to
      // TODO: AllocationAsk constants
      resourceBuilder.putResources(resourceInformation.getName(),
          quantityBuilder.build());

    }
    return resourceBuilder.build();
  }

  @Override
  protected void nodeUpdate(RMNode rmNode) {
//    long begin = System.nanoTime();
//    try {
//      readLock.lock();
//      setLastNodeUpdateTime(Time.now());
//      super.nodeUpdate(rmNode);
//    } finally {
//      readLock.unlock();
//    }


    //long latency = System.nanoTime() - begin;


    //CapacitySchedulerMetrics.getMetrics().addNodeUpdate(latency);

    //TODO - Update metrics



//    rmNode.
//    Si.UpdateRequest.Builder updateRequestBuilder = Si.UpdateRequest
//        .newBuilder();
//    updateRequestBuilder.addNewSchedulableNodes()


  }



  @Override public QueueMetrics getRootQueueMetrics() {
    return null;
  }

  @Override public boolean checkAccess(UserGroupInformation callerUGI,
      QueueACL acl, String queueName) {
    return false;
  }

  @Override public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
    return null;
  }

  @Override protected void completedContainerInternal(RMContainer rmContainer,
      ContainerStatus containerStatus, RMContainerEventType event) {

  }

  @Override public void killContainer(RMContainer container) {
    
  }

  @Override public QueueInfo getQueueInfo(String queueName,
      boolean includeChildQueues, boolean recursive) throws IOException {
    return null;
  }

  @Override public List<QueueUserACLInfo> getQueueUserAclInfo() {
    return null;
  }

  @Override public ResourceCalculator getResourceCalculator() {
    return null;
  }

  @Override public int getNumClusterNodes() {
    return 0;
  }


}
