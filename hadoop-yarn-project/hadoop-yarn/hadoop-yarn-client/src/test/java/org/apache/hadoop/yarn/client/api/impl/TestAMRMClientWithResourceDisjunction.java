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

package org.apache.hadoop.yarn.client.api.impl;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.service.Service.STATE;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMTokenCache;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttemptState;
import org.apache.hadoop.yarn.server.utils.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestAMRMClientWithResourceDisjunction {
  private static Configuration conf = null;
  private static MiniYARNCluster yarnCluster = null;
  private static YarnClient yarnClient = null;
  private static List<NodeReport> nodeReports = null;
  private static ApplicationAttemptId attemptId = null;
  private static int nodeCount = 2;
  private static Resource capability;
  private static Priority priority;
  private static String node1, node2, node3;
  private static String rack1, rack2, rack3;
  private static String[] racks;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new YarnConfiguration();
    conf.set(YarnConfiguration.RM_SCHEDULER,
        CapacitySchedulerForUniqueNM.class.getCanonicalName());
    conf.setInt(YarnConfiguration.RM_NM_HEARTBEAT_INTERVAL_MS, 100);
    yarnCluster = new MiniYARNCluster(
        TestAMRMClientWithResourceDisjunction.class.getName(), nodeCount, 1, 1);
    yarnCluster.init(conf);
    yarnCluster.start();

    // start rm client
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    priority = Priority.newInstance(1);
  }

  @Before
  public void startApp() throws Exception {
    // submit new app
    ApplicationSubmissionContext appContext = yarnClient.createApplication()
        .getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();
    // set the application name
    appContext.setApplicationName("Test");
    // Set the priority for the application master
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(0);
    appContext.setPriority(pri);
    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue("default");
    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer = BuilderUtils
        .newContainerLaunchContext(
            Collections.<String, LocalResource> emptyMap(),
            new HashMap<String, String>(), Arrays.asList("sleep", "100"),
            new HashMap<String, ByteBuffer>(), null,
            new HashMap<ApplicationAccessType, String>());
    appContext.setAMContainerSpec(amContainer);
    appContext.setResource(Resource.newInstance(1024, 1));
    // Create the request to send to the applications manager
    SubmitApplicationRequest appRequest = Records
        .newRecord(SubmitApplicationRequest.class);
    appRequest.setApplicationSubmissionContext(appContext);
    // Submit the application to the applications manager
    yarnClient.submitApplication(appContext);

    // wait for app to start
    RMAppAttempt appAttempt = null;
    while (true) {
      ApplicationReport appReport = yarnClient.getApplicationReport(appId);
      if (appReport.getYarnApplicationState() == YarnApplicationState.ACCEPTED) {
        attemptId = appReport.getCurrentApplicationAttemptId();
        appAttempt = yarnCluster.getResourceManager().getRMContext()
            .getRMApps().get(attemptId.getApplicationId())
            .getCurrentAppAttempt();
        while (true) {
          if (appAttempt.getAppAttemptState() == RMAppAttemptState.LAUNCHED) {
            break;
          }
        }
        break;
      }
    }
    // Just dig into the ResourceManager and get the AMRMToken just for the sake
    // of testing.
    UserGroupInformation.setLoginUser(UserGroupInformation
        .createRemoteUser(UserGroupInformation.getCurrentUser().getUserName()));
    UserGroupInformation.getCurrentUser().addToken(appAttempt.getAMRMToken());

    do {
      nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
      if (nodeReports.size() < nodeCount)
        Thread.sleep(1000);
    } while (nodeReports.size() < nodeCount);

    int maxMemPerNode = nodeReports.get(0).getCapability().getMemory();
    maxMemPerNode -= 1024;
    capability = Resource.newInstance(maxMemPerNode, 1);

    node1 = CapacitySchedulerForUniqueNM.getUniqueHostNameForTest(nodeReports
        .get(0).getNodeId());
    node2 = CapacitySchedulerForUniqueNM.getUniqueHostNameForTest(nodeReports
        .get(1).getNodeId());
    rack1 = nodeReports.get(0).getRackName();
    rack2 = nodeReports.get(1).getRackName();
    rack3 = rack2;
    racks = new String[] { rack1 };

    /**
     * Currently there is not enough testing control over invocation of events
     * on ResourceManager. This leads to many uncontrolled non-determinism that
     * makes it difficult to check for a particular erroneous scenario. The same
     * problem applies here if we use three NodeManagers. We instead use a
     * non-existent node manager, which is good enough to reveal the bug
     */
    node3 = "NonExistentNode";
  }

  @After
  public void cancelApp() throws YarnException, IOException {
    yarnClient.killApplication(attemptId.getApplicationId());
    attemptId = null;
  }

  @AfterClass
  public static void tearDown() {
    if (yarnClient != null && yarnClient.getServiceState() == STATE.STARTED) {
      yarnClient.stop();
    }
    if (yarnCluster != null && yarnCluster.getServiceState() == STATE.STARTED) {
      yarnCluster.stop();
    }
  }

  @Test(timeout = 60000)
  public void testAMRMClient() throws YarnException, IOException {
    AMRMClient<ContainerRequest> amClient = null;
    try {
      // start am rm client
      amClient = AMRMClient.<ContainerRequest> createAMRMClient();
      amClient.init(conf);
      amClient.start();

      amClient.registerApplicationMaster("Host", 10000, "");

      testAllocationDisjunction((AMRMClientImpl<ContainerRequest>) amClient);

      amClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED,
          null, null);

    } finally {
      if (amClient != null && amClient.getServiceState() == STATE.STARTED) {
        amClient.stop();
      }
    }
  }

  private void testAllocationDisjunction(
      final AMRMClientImpl<ContainerRequest> amClient) throws YarnException,
      IOException {
    // preconditions
    assertTrue(!node1.equals(node2) && !node1.equals(node3)
        && !node2.equals(node3));
    assertTrue(rack1.equals(rack2) && rack2.equals(rack3));
    assertTrue(racks.length == 1 && racks[0].equals(rack1));

    // (node1 || node2) && node3
    amClient.addContainerRequest(new ContainerRequest(capability, new String[] {
        node1, node2 }, racks, priority, true));
    amClient.addContainerRequest(new ContainerRequest(capability,
        new String[] { node3 }, new String[] {}, priority, true));

    int containersRequestedAny = amClient.remoteRequestsTable.get(priority)
        .get(ResourceRequest.ANY).get(capability).remoteRequest
        .getNumContainers();

    // RM should allocate container within 2 calls to allocate()
    int allocatedContainerCount = 0;
    // max tries to allow the node heartbeats to be received by RM
    int iterationsLeft = 20;

    NMTokenCache.clearCache();
    Assert.assertEquals(0, NMTokenCache.numberOfNMTokensInCache());

    Set<String> allocatedNodes = new HashSet<String>();

    while (allocatedContainerCount < containersRequestedAny
        && iterationsLeft-- > 0) {
      AllocateResponse allocResponse = amClient.allocate(0.1f);
      assertTrue(amClient.ask.size() == 0);

      Assert.assertEquals(nodeCount, amClient.getClusterNodeCount());
      allocatedContainerCount += allocResponse.getAllocatedContainers().size();
      for (Container c : allocResponse.getAllocatedContainers()) {
        allocatedNodes.add(c.getNodeId().getHost() + c.getNodeId().getPort());
      }

      if (allocatedContainerCount < containersRequestedAny) {
        // sleep to let NM's heartbeat to RM and trigger allocations
        sleep(1000);
      }
    }

    Assert.assertEquals(containersRequestedAny, allocatedContainerCount);
    assertTrue("We asked for 1 or 2 and we got 1 and 2!",
        !allocatedNodes.contains(node1) || !allocatedNodes.contains(node2));
    /**
     * The above assert already reveals the bug. For future, when we better
     * control over non-determinism of RM, we can use an existing node3 and
     * check for the following assertion as well.
     */
    // assertTrue("We asked for 3, and we got nothing",
    // allocatedNodes.contains(node3));
  }

  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
