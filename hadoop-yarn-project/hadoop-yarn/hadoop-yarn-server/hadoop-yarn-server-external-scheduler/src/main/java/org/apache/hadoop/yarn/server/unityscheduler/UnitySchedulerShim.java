package org.apache.hadoop.yarn.server.unityscheduler;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels
    .RMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;

import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt
    .RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer
    .RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer
    .RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica
    .FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppAttemptRemovedSchedulerEvent;

import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event
    .AppRemovedSchedulerEvent;
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
    .SchedulerEvent;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceCalculator;
import org.apache.hadoop.yarn.webapp.util.WebAppUtils;
import si.v1.SchedulerGrpc;
import si.v1.Si;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.nodelabels.CommonNodeLabelsManager
    .NO_LABEL;
import static org.apache.hadoop.yarn.server.resourcemanager.webapp
    .RMWebServices.DEFAULT_QUEUE;

public class UnitySchedulerShim extends AbstractYarnScheduler<UnitySchedulerAppAttempt,
    FiCaSchedulerNode> {

  private static final Logger LOG = LoggerFactory.getLogger(
      UnitySchedulerShim.class);

  private String rmId;

  private UnitySchedulerGrpcClient grpcClient;

  private SchedulerGrpc.SchedulerStub schedulerStub;

  private UnitySchedulerCallBack callBackHandler;

  private Configuration yarnConf;

  private RMNodeLabelsManager labelManager;

  private ResourceCalculator resourceCalculator;

  private UnitySchedulerContext context = new UnitySchedulerContext();

  /**
   * Construct the service.
   */
  public UnitySchedulerShim() {
    super(UnitySchedulerShim.class.getName());
  }

  @Override
  public void serviceInit(Configuration conf) throws Exception {
    Configuration configuration = new Configuration(conf);
    super.serviceInit(conf);
    initScheduler(configuration);
  }

  @VisibleForTesting
  void initScheduler(Configuration configuration) throws
      IOException {
    try {
      writeLock.lock();

      this.minimumAllocation = super.getMinimumAllocation();
      initMaximumResourceCapability(super.getMaximumAllocation());

      this.yarnConf = configuration;

      //TODO - Validation - DefaultResourceCalculator is not supported
      resourceCalculator = new DominantResourceCalculator();

      this.minimumAllocation = super.getMinimumAllocation();
      initMaximumResourceCapability(super.getMaximumAllocation());

      this.applications = new ConcurrentHashMap<>();
      this.labelManager = rmContext.getNodeLabelManager();

      // number of threads for async scheduling

      LOG.info("Initialized UnityScheduler shim with "
          + "minimumAllocation=<"
          + getMinimumResourceCapability() + ">, " + "maximumAllocation=<"
          + getMaximumResourceCapability() + ">");
    } finally {
      writeLock.unlock();
    }
  }

  private void registerResourceManager() {

    Si.RegisterResourceManagerRequest.Builder registerRMRequest =
        Si.RegisterResourceManagerRequest.newBuilder();

    registerRMRequest.setRmId(rmId);
    registerRMRequest.setPolicyGroup("queues");
    registerRMRequest.setVersion("1.0");

    SchedulerGrpc.SchedulerBlockingStub schedulerBlockingStub =
        grpcClient.createSchedulerBlockingStub();

    schedulerBlockingStub.registerResourceManager(registerRMRequest.build());
  }

  @Override public void recover(RMStateStore.RMState state) throws Exception {
  }

  @Override public void handle(SchedulerEvent event) {
    switch (event.getType()) {
    case NODE_ADDED: {
      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent) event;
      addNode(nodeAddedEvent);
    }
    break;
    case NODE_REMOVED: {
      NodeRemovedSchedulerEvent nodeRemovedEvent =
          (NodeRemovedSchedulerEvent) event;
      removeNode(nodeRemovedEvent);
    }
    break;
    case NODE_RESOURCE_UPDATE: {
      NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent =
          (NodeResourceUpdateSchedulerEvent) event;
    }
    break;
    case NODE_LABELS_UPDATE: {
      NodeLabelsUpdateSchedulerEvent labelUpdateEvent =
          (NodeLabelsUpdateSchedulerEvent) event;
    }
    break;
    case NODE_ATTRIBUTES_UPDATE: {
      NodeAttributesUpdateSchedulerEvent attributeUpdateEvent =
          (NodeAttributesUpdateSchedulerEvent) event;
    }
    break;
    case NODE_UPDATE:
      //TODO - Utilization Report
      //    {
      //      NodeUpdateSchedulerEvent nodeUpdatedEvent =
      // (NodeUpdateSchedulerEvent)event;
      //      nodeUpdate(nodeUpdatedEvent.getRMNode());
      //    }
      break;
    case APP_ADDED: {
      //TODO : Check if we need to do anything else here
      AppAddedSchedulerEvent schedulerEvent = (AppAddedSchedulerEvent) event;
      addApplication(schedulerEvent);
      //TODO - App recovery
    }
    break;
    case APP_REMOVED: {
      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent) event;
      doneApplication(appRemovedEvent.getApplicationID(), appRemovedEvent.getFinalState());
      //TODO - Remove from cache
    }
    break;
    case APP_ATTEMPT_ADDED: {
      //TODO - Add to cache
      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
          (AppAttemptAddedSchedulerEvent) event;
      addJob(appAttemptAddedEvent);
    }
    break;
    case APP_ATTEMPT_REMOVED: {
      //TODO - Remove from cache
      //TODO - Send removeJobRequest
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
          (AppAttemptRemovedSchedulerEvent) event;
      removeJob(appAttemptRemovedEvent);
    }
    break;
    case CONTAINER_EXPIRED: {
    }
    break;
    case RELEASE_CONTAINER: {
    }
    break;
    case KILL_RESERVED_CONTAINER: {
    }
    break;
    case MARK_CONTAINER_FOR_PREEMPTION: {
    }
    break;
    case MARK_CONTAINER_FOR_KILLABLE: {
    }
    break;
    case MARK_CONTAINER_FOR_NONKILLABLE: {
    }
    break;
    case MANAGE_QUEUE: {
      //TODO - API based queue management
      //TODO - Add to cache
    }
    break;
    default:
      LOG.error("Invalid eventtype " + event.getType() + ". Ignoring!");
    }
  }

  private void addNode(NodeAddedSchedulerEvent nodeAddedEvent) {
    Si.UpdateRequest.Builder updateRequestBuilder =
        createUpdateRequestBuilder();

    Si.NewNodeInfo.Builder newNodeInfo = Si.NewNodeInfo.newBuilder();
    RMNode rmNode = nodeAddedEvent.getAddedRMNode();
    newNodeInfo.setNodeId(rmNode.getNodeID().toString());

    Resource nodeCapability = rmNode.getTotalCapability();
    final Si.Resource resourceCapability =
        AllocationRequestUtils.transformResource(nodeCapability);
    newNodeInfo.setSchedulableResource(resourceCapability);

    updateRequestBuilder.addNewSchedulableNodes(newNodeInfo);

    grpcClient.sendUpdateRequest(schedulerStub, updateRequestBuilder.build(),
        callBackHandler);

    //TODO - Deal with container recovery
    //TODO: Will have to add existing allocations if containers are up and
    // recovered on node restart
    //newNodeInfo.addExistingAllocations();
  }

  private void removeNode(NodeRemovedSchedulerEvent nodeRemovedEvent) {

    Si.UpdateRequest.Builder updateRequestBuilder =
        createUpdateRequestBuilder();

    Si.UpdateNodeInfo.Builder updatedNodeInfo = Si.UpdateNodeInfo.newBuilder();
    RMNode rmNode = nodeRemovedEvent.getRemovedRMNode();
    updatedNodeInfo.setNodeId(rmNode.getNodeID().toString());
    updatedNodeInfo.setAction(Si.UpdateNodeInfo.ActionFromRM.DECOMISSION);

    //TODO: Would need to handle graceful decommissioning cases - DRAIN TO
    // DECOMMISSION
    updateRequestBuilder.addUpdatedNodes(updatedNodeInfo);

    grpcClient.sendUpdateRequest(schedulerStub, updateRequestBuilder.build(),
        callBackHandler);
  }

  @Override
  public void setRMContext(RMContext rmContext) {

    this.rmContext = rmContext;
    rmId = WebAppUtils.getWebAppBindURL(rmContext.getYarnConfiguration(),
        YarnConfiguration.RM_BIND_HOST, WebAppUtils
            .getRMWebAppURLWithoutScheme(rmContext.getYarnConfiguration()));

    Configuration conf = rmContext.getYarnConfiguration();
    String unitySchedulerHost = conf.get(
        YarnConfiguration.UNITY_SCHEDULER_HOST, "localhost");
    int unitySchedulerPort = conf.getInt(YarnConfiguration.UNITY_SCHEDULER_PORT,
        YarnConfiguration.DEFAULT_UNITY_SCHEDULER_PORT);

    grpcClient = new UnitySchedulerGrpcClient(unitySchedulerHost,
        unitySchedulerPort);

    schedulerStub = grpcClient.createSchedulerStub();
    callBackHandler = new UnitySchedulerCallBack(this, context, rmContext);

    registerResourceManager();
  }

  @Override public Allocation allocate(ApplicationAttemptId appAttemptId,
      List<ResourceRequest> resourceRequests,
      List<SchedulingRequest> schedulingRequests, List<ContainerId> release,
      List<String> blacklistAdditions, List<String> blacklistRemovals,
      ContainerUpdates updateRequests) {

    Si.UpdateRequest.Builder updateRequestBuilder =
        createUpdateRequestBuilder();

    SchedulerApplicationAttempt appAttempt = getApplicationAttempt
        (appAttemptId);

    // Release containers
    releaseContainers(release, appAttempt);

    if (resourceRequests.size() > 0) {
      List<Si.AllocationAsk> allocationRRAsks =
          AllocationRequestUtils.transformResourceRequests(rmContext,
              appAttemptId, resourceRequests);
      updateRequestBuilder.addAllAsks(allocationRRAsks);
    }

    if (schedulingRequests != null && schedulingRequests.size() > 0) {
      List<Si.AllocationAsk> allocationSRAsks =
          AllocationRequestUtils.transformSchedulingRequests(rmContext,
              appAttemptId, schedulingRequests);
      updateRequestBuilder.addAllAsks(allocationSRAsks);
    }

    String jobId = appAttemptId.toString();
    if (context.isJobAccepted(jobId)) {

      //TODO - Replace ActiveUsersManager -?
      //TODO - Uncomment below after fixing below code
//      UnitySchedulerAppAttempt currentAppAttempt = app.getCurrentAppAttempt();
//      if ( jobId.equals(currentAppAttempt.getApplicationAttemptId().toString
//          ())) {
//         LOG.error("Ignoring previous attempt allocations");
//         //TODO - Release asks
//      }

      grpcClient.sendUpdateRequest(schedulerStub, updateRequestBuilder.build(),
          callBackHandler);
    } else {
       LOG.error("JobId {} is not accepted by scheduler ", jobId);
    }

    //TODO - release requests

    //TODO - Blacklist nodes - send node updates

    //TODO - Container updates

    UnitySchedulerAppAttempt schedulerAppAttempt =
        getApplicationAttempt(appAttemptId);
    return schedulerAppAttempt.getNewAllocations();
  }

  private void addApplication(AppAddedSchedulerEvent appAddedSchedulerEvent) {
    QueueMetrics queueMetrics = new UnitySchedulerQueueMetrics(
        DefaultMetricsSystem.instance(), appAddedSchedulerEvent.getQueue(), null, false,
        rmContext.getYarnConfiguration());

    String user = appAddedSchedulerEvent.getUser();
    String queueName = appAddedSchedulerEvent.getQueue();

    SchedulerApplication<UnitySchedulerAppAttempt> application =
        new SchedulerApplication<UnitySchedulerAppAttempt>(new UnitySchedulerQueue
            (queueName, queueMetrics, new
                ActiveUsersManager(queueMetrics), appAddedSchedulerEvent
                .getApplicatonPriority()), user);

    ApplicationId applicationId = appAddedSchedulerEvent.getApplicationId();
    applications.put(applicationId, application);
    LOG.info("Accepted application " + applicationId + " from user: " + user
        + ", in queue: " + queueName);
    rmContext.getDispatcher().getEventHandler().handle(
        new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
  }

  private void doneApplication(ApplicationId applicationId,
      RMAppState finalState) {
    try {
      writeLock.lock();
      SchedulerApplication<UnitySchedulerAppAttempt> application = applications.get(
          applicationId);
      if (application == null) {
        // The AppRemovedSchedulerEvent maybe sent on recovery for completed
        // apps, ignore it.
        LOG.warn("Couldn't find application " + applicationId);
        return;
      }

      application.stop(finalState);
      applications.remove(applicationId);
    } finally {
      writeLock.unlock();
    }
  }

  private Si.UpdateRequest.Builder createUpdateRequestBuilder() {
    Si.UpdateRequest.Builder updateRequestBuilder =
        Si.UpdateRequest.newBuilder();

    updateRequestBuilder.setRmId(rmId);
    return updateRequestBuilder;
  }

  private void addJob(
      AppAttemptAddedSchedulerEvent appAttemptAddedSchedulerEvent) {

    ApplicationAttemptId appAttemptId =
        appAttemptAddedSchedulerEvent.getApplicationAttemptId();
    Si.UpdateRequest.Builder updateRequestBuilder =
        createUpdateRequestBuilder();
    Si.AddJobRequest.Builder addJobRequestBuilder =
        Si.AddJobRequest.newBuilder();

    RMApp app = rmContext.getRMApps().get(appAttemptId.getApplicationId());

    addJobRequestBuilder.setJobId(appAttemptId.toString());

    final String partition = AllocationRequestUtils.getPartition(app);
    addJobRequestBuilder.setPartitionName(partition);

    final String queue = AllocationRequestUtils.getQueue(app);
    addJobRequestBuilder.setQueueName(queue);
    updateRequestBuilder.addNewJobs(addJobRequestBuilder.build());

    SchedulerApplication<UnitySchedulerAppAttempt> application = applications
        .get(app.getApplicationId());

    QueueMetrics queueMetrics = new UnitySchedulerQueueMetrics(
        DefaultMetricsSystem.instance(), app.getQueue(), null, false,
        rmContext.getYarnConfiguration());

    UnitySchedulerAppAttempt appAttempt = new UnitySchedulerAppAttempt(
        appAttemptId, app.getUser(), null,
        new ActiveUsersManager(queueMetrics), rmContext);

    application.setCurrentAppAttempt(appAttempt);

    // Update attempt priority to the latest to avoid race condition i.e
    // SchedulerApplicationAttempt is created with old priority but it is not
    // set to SchedulerApplication#setCurrentAppAttempt.
    // Scenario would occur is
    // 1. SchdulerApplicationAttempt is created with old priority.
    // 2. updateApplicationPriority() updates SchedulerApplication. Since
    // currentAttempt is null, it just return.
    // 3. ScheduelerApplcationAttempt is set in
    // SchedulerApplication#setCurrentAppAttempt.
    appAttempt.setPriority(application.getPriority());

    grpcClient.sendUpdateRequest(schedulerStub, updateRequestBuilder.build(),
        callBackHandler);
  }

  private void removeJob(
      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent) {

    ApplicationAttemptId appAttemptId =
        appAttemptRemovedEvent.getApplicationAttemptID();
    Si.UpdateRequest.Builder updateRequestBuilder =
        createUpdateRequestBuilder();
    Si.RemoveJobRequest.Builder removeJobRequestBuilder =
        Si.RemoveJobRequest.newBuilder();

    RMApp app = rmContext.getRMApps().get(appAttemptId.getApplicationId());

    String jobId = appAttemptId.toString();

    removeJobRequestBuilder.setJobId(jobId);
    removeJobRequestBuilder.setPartitionName(app.getAppNodeLabelExpression());

    updateRequestBuilder.addRemoveJobs(removeJobRequestBuilder.build());


    try {
      writeLock.lock();

      RMAppAttemptState rmAppAttemptFinalState = appAttemptRemovedEvent
          .getFinalAttemptState();

      boolean keepContainers = appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts();
      LOG.info("Application Attempt " + appAttemptId + " is done."
          + " finalState=" + rmAppAttemptFinalState);

      SchedulerApplicationAttempt attempt = getApplicationAttempt(appAttemptId);
      SchedulerApplication<UnitySchedulerAppAttempt> application =
          (SchedulerApplication<UnitySchedulerAppAttempt>) applications.get(
              appAttemptId.getApplicationId());

      if (application == null || attempt == null) {
        LOG.info(
            "Unknown application " + appAttemptId + " has completed!");
        return;
      }

      // Release all the allocated, acquired, running containers
      for (RMContainer rmContainer : attempt.getLiveContainers()) {
        if (keepContainers && rmContainer.getState().equals(
            RMContainerState.RUNNING)) {
          // do not kill the running container in the case of work-preserving AM
          // restart.
          LOG.info("Skip killing " + rmContainer.getContainerId());
          continue;
        }
        super.completedContainer(rmContainer, SchedulerUtils
                .createAbnormalContainerStatus(rmContainer.getContainerId(),
                    SchedulerUtils.COMPLETED_APPLICATION),
            RMContainerEventType.KILL);
      }

      // Release all reserved containers
      for (RMContainer rmContainer : attempt.getReservedContainers()) {
        super.completedContainer(rmContainer, SchedulerUtils
            .createAbnormalContainerStatus(rmContainer.getContainerId(),
                "Application Complete"), RMContainerEventType.KILL);
      }

      // Clean up pending requests, metrics etc.
      attempt.stop(rmAppAttemptFinalState);

      context.removeAcceptedJob(jobId);

    } finally {
      writeLock.unlock();
    }

    //Inform scheduler
    grpcClient.sendUpdateRequest(schedulerStub, updateRequestBuilder.build(),
        callBackHandler);
  }

  @Override protected void nodeUpdate(RMNode rmNode) {
    //TODO - Update metrics - See CS code
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
    return resourceCalculator;
  }

  @Override public int getNumClusterNodes() {
    return 0;
  }

  ReentrantReadWriteLock.WriteLock getWriteLock() {
    return writeLock;
  }

  ReentrantReadWriteLock.ReadLock getReadLock() {
    return readLock;
  }
}
