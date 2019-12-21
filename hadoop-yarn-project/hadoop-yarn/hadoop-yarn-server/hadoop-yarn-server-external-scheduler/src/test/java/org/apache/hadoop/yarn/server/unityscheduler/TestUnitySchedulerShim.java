/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.unityscheduler;

import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .TestQueueMetricsForCustomResources.GB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_MB;
import static org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration.MAXIMUM_ALLOCATION_VCORES;


import static org.apache.hadoop.yarn.server.resourcemanager.scheduler
    .capacity.TestCapacitySchedulerAutoCreatedQueueBase.NULL_UPDATE_REQUESTS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.TestGroupsCaching;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.yarn.LocalConfigurationProvider;
import org.apache.hadoop.yarn.api.ApplicationMasterProtocol;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateRequest;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerUpdateType;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.ExecutionTypeRequest;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueState;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceOption;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.UpdateContainerRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;
import org.apache.hadoop.yarn.server.api.protocolrecords.UpdateNodeResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.AdminService;
import org.apache.hadoop.yarn.server.resourcemanager.Application;
import org.apache.hadoop.yarn.server.resourcemanager.MockAM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNM;
import org.apache.hadoop.yarn.server.resourcemanager.MockNodes;
import org.apache.hadoop.yarn.server.resourcemanager.MockRM;
import org.apache.hadoop.yarn.server.resourcemanager.NodeManager;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.RMContextImpl;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.Task;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MockRMWithAMS;
import org.apache.hadoop.yarn.server.resourcemanager.TestAMAuthorization.MyContainerManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.NullRMNodeLabelsManager;
import org.apache.hadoop.yarn.server.resourcemanager.nodelabels.RMNodeLabelsManager;

import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerImpl;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeResourceUpdateEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueResourceQuotas;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ContainerUpdates;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.TestSchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.YarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity
    .CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.AllocationState;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.allocator.ContainerAllocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ResourceCommitRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerApp;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.fica.FiCaSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.
    ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.placement.SimpleCandidateNodeSet;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.policy.FairOrderingPolicy;
import org.apache.hadoop.yarn.server.resourcemanager.security.ClientToAMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.NMTokenSecretManagerInRM;
import org.apache.hadoop.yarn.server.resourcemanager.security.RMContainerTokenSecretManager;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerLeafQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfo;
import org.apache.hadoop.yarn.server.resourcemanager.webapp.dao.CapacitySchedulerQueueInfoList;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.resource.DefaultResourceCalculator;
import org.apache.hadoop.yarn.util.resource.DominantResourceCalculator;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestUnitySchedulerShim {
  private static final Log LOG = LogFactory.getLog(TestUnitySchedulerShim.class);

  private ResourceManager resourceManager = null;
  private RMContext mockContext;

  @Before
  public void setUp() throws Exception {
    resourceManager = new ResourceManager() {
      @Override
      protected RMNodeLabelsManager createNodeLabelManager() {
        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
        mgr.init(getConfig());
        return mgr;
      }
    };

    YarnConfiguration conf = new YarnConfiguration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER,
        UnitySchedulerShim.class, ResourceScheduler.class);
    resourceManager.init(conf);
    resourceManager.getRMContext().getContainerTokenSecretManager().rollMasterKey();
    resourceManager.getRMContext().getNMTokenSecretManager().rollMasterKey();
    resourceManager.getResourceScheduler().setRMContext(resourceManager
        .getRMContext());
    ((AsyncDispatcher)resourceManager.getRMContext().getDispatcher()).start();
    mockContext = mock(RMContext.class);
    when(mockContext.getConfigurationProvider()).thenReturn(
        new LocalConfigurationProvider());
  }

  @After
  public void tearDown() throws Exception {
    if (resourceManager != null) {
      resourceManager.stop();
    }
  }

  private NodeManager registerNode(ResourceManager rm, String hostName,
      int containerManagerPort, int httpPort, String rackName,
          Resource capability) throws IOException, YarnException {
    NodeManager nm = new NodeManager(hostName,
        containerManagerPort, httpPort, rackName, capability, rm);
    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(rm.getRMContext().getRMNodes()
            .get(nm.getNodeId()));
    rm.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }


  private NodeManager registerNode(String hostName, int containerManagerPort,
                                   int httpPort, String rackName,
                                   Resource capability)
          throws IOException, YarnException {
    NodeManager nm = new NodeManager(hostName, containerManagerPort, httpPort,
        rackName, capability, resourceManager);
    NodeAddedSchedulerEvent nodeAddEvent1 =
        new NodeAddedSchedulerEvent(resourceManager.getRMContext()
            .getRMNodes().get(nm.getNodeId()));
    resourceManager.getResourceScheduler().handle(nodeAddEvent1);
    return nm;
  }


  @Test
  public void testUnityScheduler() throws Exception {

    LOG.info("--- START: testUnityScheduler ---");

    // Register node1
    String host_0 = "host_0";
    NodeManager nm_0 =
        registerNode(host_0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(4 * GB, 1));

    // Register node2
    String host_1 = "host_1";
    NodeManager nm_1 =
        registerNode(host_1, 1234, 2345, NetworkTopology.DEFAULT_RACK,
            Resources.createResource(2 * GB, 1));

    // ResourceRequest priorities
    Priority priority_0 = Priority.newInstance(0);
    Priority priority_1 = Priority.newInstance(1);

    // Submit an application
    Application application_0 = new Application("user_0", "a1", resourceManager);
    application_0.submit();

    application_0.addNodeManager(host_0, 1234, nm_0);
    application_0.addNodeManager(host_1, 1234, nm_1);

    Resource capability_0_0 = Resources.createResource(1 * GB, 1);
    application_0.addResourceRequestSpec(priority_1, capability_0_0);
//
    Resource capability_0_1 = Resources.createResource(2 * GB, 1);
    application_0.addResourceRequestSpec(priority_0, capability_0_1);

    Task task_0_0 = new Task(application_0, priority_1,
        new String[] {host_0, host_1});
    application_0.addTask(task_0_0);

    application_0.schedule();

    // Submit another application
//    Application application_1 = new Application("user_1", "b2", resourceManager);
//    application_1.submit();
//
//    application_1.addNodeManager(host_0, 1234, nm_0);
//    application_1.addNodeManager(host_1, 1234, nm_1);
//
//    Resource capability_1_0 = Resources.createResource(3 * GB, 1);
//    application_1.addResourceRequestSpec(priority_1, capability_1_0);
//
//    Resource capability_1_1 = Resources.createResource(2 * GB, 1);
//    application_1.addResourceRequestSpec(priority_0, capability_1_1);
//
//    Task task_1_0 = new Task(application_1, priority_1,
//        new String[] {host_0, host_1});
//    application_1.addTask(task_1_0);
//
//    // Send resource requests to the scheduler
//    application_0.schedule();
//    application_1.schedule();
//
//    // Send a heartbeat to kick the tires on the Scheduler
//    LOG.info("Kick!");
//
//    // task_0_0 and task_1_0 allocated, used=4G
//    nodeUpdate(nm_0);
//
//    // nothing allocated
//    nodeUpdate(nm_1);
//
//    // Get allocations from the scheduler
//    application_0.schedule();     // task_0_0
//    checkApplicationResourceUsage(1 * GB, application_0);
//
//    application_1.schedule();     // task_1_0
//    checkApplicationResourceUsage(3 * GB, application_1);
//
//    checkNodeResourceUsage(4*GB, nm_0);  // task_0_0 (1G) and task_1_0 (3G)
//    checkNodeResourceUsage(0*GB, nm_1);  // no tasks, 2G available
//
//    LOG.info("Adding new tasks...");
//
//    Task task_1_1 = new Task(application_1, priority_0,
//        new String[] {ResourceRequest.ANY});
//    application_1.addTask(task_1_1);
//
//    application_1.schedule();
//
//    Task task_0_1 = new Task(application_0, priority_0,
//        new String[] {host_0, host_1});
//    application_0.addTask(task_0_1);
//
//    application_0.schedule();
//
//    // Send a heartbeat to kick the tires on the Scheduler
//    LOG.info("Sending hb from " + nm_0.getHostName());
//    // nothing new, used=4G
//    nodeUpdate(nm_0);
//
//    LOG.info("Sending hb from " + nm_1.getHostName());
//    // task_0_1 is prefer as locality, used=2G
//    nodeUpdate(nm_1);
//
//    // Get allocations from the scheduler
//    LOG.info("Trying to allocate...");
//    application_0.schedule();
//    checkApplicationResourceUsage(1 * GB, application_0);
//
//    application_1.schedule();
//    checkApplicationResourceUsage(5 * GB, application_1);
//
//    nodeUpdate(nm_0);
//    nodeUpdate(nm_1);
//
//    checkNodeResourceUsage(4*GB, nm_0);
//    checkNodeResourceUsage(2*GB, nm_1);
//
    LOG.info("--- END: testUnityScheduler ---");
  }


  @Test
  public void testUnityScheduler2() throws Exception {
    Configuration conf = new Configuration();
    conf.setClass(YarnConfiguration.RM_SCHEDULER, UnitySchedulerShim.class,
        ResourceScheduler.class);
    MockRM rm = new MockRM(conf);
    rm.start();
    UnitySchedulerShim cs = (UnitySchedulerShim) rm.getResourceScheduler();

    ApplicationId appId = BuilderUtils.newApplicationId(100, 1);
    ApplicationAttemptId appAttemptId =
        BuilderUtils.newApplicationAttemptId(appId, 1);

    RMAppAttemptMetrics attemptMetric =
        new RMAppAttemptMetrics(appAttemptId, rm.getRMContext());
    RMAppImpl app = mock(RMAppImpl.class);
    when(app.getApplicationId()).thenReturn(appId);
    RMAppAttemptImpl attempt = mock(RMAppAttemptImpl.class);
    Container container = mock(Container.class);
    when(attempt.getMasterContainer()).thenReturn(container);
    ApplicationSubmissionContext submissionContext = mock(
        ApplicationSubmissionContext.class);
    when(attempt.getSubmissionContext()).thenReturn(submissionContext);
    when(attempt.getAppAttemptId()).thenReturn(appAttemptId);
    when(attempt.getRMAppAttemptMetrics()).thenReturn(attemptMetric);
    when(app.getCurrentAppAttempt()).thenReturn(attempt);

    rm.getRMContext().getRMApps().put(appId, app);

    SchedulerEvent addAppEvent =
        new AppAddedSchedulerEvent(appId, "default", "user");
    cs.handle(addAppEvent);
    SchedulerEvent addAttemptEvent =
        new AppAttemptAddedSchedulerEvent(appAttemptId, false);
    cs.handle(addAttemptEvent);

    Allocation allocate =
        cs.allocate(appAttemptId, Arrays.asList(ResourceRequest.newInstance(Priority.newInstance(1),
            "*", Resources.createResource(2 * GB), 2)),
            null, Collections.<ContainerId> emptyList(), null, null,
            NULL_UPDATE_REQUESTS);

    Assert.assertNotNull(attempt);

    Assert
        .assertEquals(Resource.newInstance(0, 0), allocate.getResourceLimit());
    Assert.assertEquals(Resource.newInstance(0, 0),
        attemptMetric.getApplicationAttemptHeadroom());

    // Add a node to cluster
    Resource newResource = Resource.newInstance(40 * GB, 10);
    RMNode node = MockNodes.newNodeInfo(0, newResource, 1, "127.0.0.1");
    cs.handle(new NodeAddedSchedulerEvent(node));

    Thread.sleep(5000);

    allocate =
        cs.allocate(appAttemptId, Collections.<ResourceRequest> emptyList(),
            null, Collections.<ContainerId> emptyList(), null, null,
            NULL_UPDATE_REQUESTS);



    Assert.assertEquals(newResource, allocate.getContainers());
//    Assert.assertEquals(newResource,
//        attemptMetric.getApplicationAttemptHeadroom());

    rm.stop();
  }
//
//  @Test
//  public void testNotAssignMultiple() throws Exception {
//    LOG.info("--- START: testNotAssignMultiple ---");
//    ResourceManager rm = new ResourceManager() {
//      @Override
//      protected RMNodeLabelsManager createNodeLabelManager() {
//        RMNodeLabelsManager mgr = new NullRMNodeLabelsManager();
//        mgr.init(getConfig());
//        return mgr;
//      }
//    };
//    CapacitySchedulerConfiguration csConf =
//        new CapacitySchedulerConfiguration();
//    csConf.setBoolean(
//        CapacitySchedulerConfiguration.ASSIGN_MULTIPLE_ENABLED, false);
//    setupQueueConfiguration(csConf);
//    YarnConfiguration conf = new YarnConfiguration(csConf);
//    conf.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class,
//        ResourceScheduler.class);
//    rm.init(conf);
//    rm.getRMContext().getContainerTokenSecretManager().rollMasterKey();
//    rm.getRMContext().getNMTokenSecretManager().rollMasterKey();
//    ((AsyncDispatcher) rm.getRMContext().getDispatcher()).start();
//    RMContext mC = mock(RMContext.class);
//    when(mC.getConfigurationProvider()).thenReturn(
//        new LocalConfigurationProvider());
//
//    // Register node1
//    String host0 = "host_0";
//    NodeManager nm0 =
//        registerNode(rm, host0, 1234, 2345, NetworkTopology.DEFAULT_RACK,
//            Resources.createResource(10 * GB, 10));
//
//    // ResourceRequest priorities
//    Priority priority0 = Priority.newInstance(0);
//    Priority priority1 = Priority.newInstance(1);
//
//    // Submit an application
//    Application application0 = new Application("user_0", "a1", rm);
//    application0.submit();
//    application0.addNodeManager(host0, 1234, nm0);
//
//    Resource capability00 = Resources.createResource(1 * GB, 1);
//    application0.addResourceRequestSpec(priority0, capability00);
//
//    Resource capability01 = Resources.createResource(2 * GB, 1);
//    application0.addResourceRequestSpec(priority1, capability01);
//
//    Task task00 =
//        new Task(application0, priority0, new String[] {host0});
//    Task task01 =
//        new Task(application0, priority1, new String[] {host0});
//    application0.addTask(task00);
//    application0.addTask(task01);
//
//    // Submit another application
//    Application application1 = new Application("user_1", "b2", rm);
//    application1.submit();
//    application1.addNodeManager(host0, 1234, nm0);
//
//    Resource capability10 = Resources.createResource(3 * GB, 1);
//    application1.addResourceRequestSpec(priority0, capability10);
//
//    Resource capability11 = Resources.createResource(4 * GB, 1);
//    application1.addResourceRequestSpec(priority1, capability11);
//
//    Task task10 = new Task(application1, priority0, new String[] {host0});
//    Task task11 = new Task(application1, priority1, new String[] {host0});
//    application1.addTask(task10);
//    application1.addTask(task11);
//
//    // Send resource requests to the scheduler
//    application0.schedule();
//
//    application1.schedule();
//
//    // Send a heartbeat to kick the tires on the Scheduler
//    LOG.info("Kick!");
//
//    // task00, used=1G
//    nodeUpdate(rm, nm0);
//
//    // Get allocations from the scheduler
//    application0.schedule();
//    application1.schedule();
//    // 1 Task per heart beat should be scheduled
//    checkNodeResourceUsage(3 * GB, nm0); // task00 (1G)
//    checkApplicationResourceUsage(0 * GB, application0);
//    checkApplicationResourceUsage(3 * GB, application1);
//
//    // Another heartbeat
//    nodeUpdate(rm, nm0);
//    application0.schedule();
//    checkApplicationResourceUsage(1 * GB, application0);
//    application1.schedule();
//    checkApplicationResourceUsage(3 * GB, application1);
//    checkNodeResourceUsage(4 * GB, nm0);
//    LOG.info("--- END: testNotAssignMultiple ---");
//  }




}
